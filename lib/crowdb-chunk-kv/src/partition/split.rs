// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Online split build and bounded final catch-up.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Buf, BytesMut};

use super::{decode_frame, FrameDecode, Partition, PartitionJournal, PartitionTree};
use crate::{
    Checkpoint, ChunkKvError, PartitionConfig, PartitionLifecycle, PreparedChildArtifact, Result,
    SplitArtifact, SplitChild, SplitPlan, WalRecord, MAX_FRAME_BYTES,
};

#[derive(Clone)]
pub struct SplitChildTarget {
    pub tree_id: u64,
    pub tree_config: crowdb_tree_ffi::Config,
    pub journal: Arc<dyn PartitionJournal>,
}

pub struct PreparedSplitChild {
    artifact: PreparedChildArtifact,
    checkpoint: Checkpoint,
    tree: Arc<dyn PartitionTree>,
    journal: Arc<dyn PartitionJournal>,
    parent_journal: Arc<dyn PartitionJournal>,
}

impl PreparedSplitChild {
    #[must_use]
    pub fn artifact(&self) -> &PreparedChildArtifact {
        &self.artifact
    }

    #[must_use]
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Open the completed child in `Prepared` state. Catalog proof is still
    /// required before it can serve.
    ///
    /// # Errors
    ///
    /// Returns a checkpoint, journal, tree, or child identity error.
    pub async fn open(self, config: PartitionConfig) -> Result<Partition> {
        Partition::recover_prepared_overlay(
            self.artifact,
            self.checkpoint,
            config,
            self.tree,
            self.journal,
            self.parent_journal,
        )
        .await
    }
}

pub struct PreparedSplit {
    pub artifact: SplitArtifact,
    pub left: PreparedSplitChild,
    pub right: PreparedSplitChild,
    pub left_rebuild: crowdb_tree_ffi::RangeRebuildStats,
    pub right_rebuild: crowdb_tree_ffi::RangeRebuildStats,
    pub delta_records: u64,
}

impl Partition {
    /// Rebuild two exact children while the parent serves, then fence only the
    /// bounded delta tail and checkpoint both children at one cutover.
    ///
    /// # Errors
    ///
    /// Returns a split retry, storage, journal, or apply error. A failure
    /// before the final fence resumes the parent; a failure after fencing
    /// leaves it fenced for authoritative resolution.
    pub async fn prepare_split(
        &self,
        plan: SplitPlan,
        left_target: SplitChildTarget,
        right_target: SplitChildTarget,
        max_fence_lag_records: u64,
    ) -> Result<PreparedSplit> {
        if max_fence_lag_records == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "split fence lag bound must be nonzero".into(),
            ));
        }
        validate_targets(self, &plan, &left_target, &right_target)?;
        self.begin_split(plan.clone()).await?;
        let preparation_started = Instant::now();
        let result = self
            .build_split(&plan, left_target, right_target, max_fence_lag_records)
            .await;
        if result.is_ok() {
            self.metrics
                .split_preparation_duration(elapsed_us(preparation_started));
        }
        if result.is_err() && self.lifecycle() == PartitionLifecycle::SplitPreparing {
            self.cancel_local_split(plan.transition_id).await;
        }
        result
    }

    async fn build_split(
        &self,
        plan: &SplitPlan,
        left_target: SplitChildTarget,
        right_target: SplitChildTarget,
        max_fence_lag_records: u64,
    ) -> Result<PreparedSplit> {
        let (base_checkpoint, source) = self.split_base_snapshot(plan).await?;
        let (left_tree, left_rebuild) = source
            .rebuild_range(
                left_target.tree_id,
                &plan.left.range,
                left_target.tree_config.clone(),
            )
            .await?;
        let (right_tree, right_rebuild) = source
            .rebuild_range(
                right_target.tree_id,
                &plan.right.range,
                right_target.tree_config.clone(),
            )
            .await?;
        if left_tree.last_applied_seq() != base_checkpoint.applied_seq
            || right_tree.last_applied_seq() != base_checkpoint.applied_seq
        {
            return Err(ChunkKvError::TreeCorruption(
                "split children do not share the base frontier".into(),
            ));
        }
        self.metrics.split_rebuild(left_rebuild, right_rebuild);

        let mut cursor = DeltaCursor {
            offset: base_checkpoint.replay_offset,
            applied_seq: base_checkpoint.applied_seq,
            delta_records: 0,
        };
        cursor
            .catch_up_to_lag(
                self,
                plan,
                left_tree.as_ref(),
                right_tree.as_ref(),
                max_fence_lag_records,
            )
            .await?;

        let checkpoint_started = Instant::now();
        let left_base = checkpoint_prepared_tree(left_tree.as_ref(), cursor.applied_seq).await?;
        let right_base = checkpoint_prepared_tree(right_tree.as_ref(), cursor.applied_seq).await?;
        self.metrics
            .split_base_checkpoint_duration(elapsed_us(checkpoint_started));

        let fence_lag_records = cursor
            .catch_up_to_lag(
                self,
                plan,
                left_tree.as_ref(),
                right_tree.as_ref(),
                max_fence_lag_records,
            )
            .await?;

        let fence_started = Instant::now();
        self.fence_split(plan.transition_id).await?;
        let cutover_seq = self.applied_seq.load(Ordering::Acquire);
        replay_children_until(
            self,
            &mut cursor,
            cutover_seq,
            left_tree.as_ref(),
            right_tree.as_ref(),
            &plan.left,
            &plan.right,
        )
        .await?;
        if cursor.applied_seq != cutover_seq {
            return Err(ChunkKvError::JournalCorruption(
                "split delta replay did not reach the fenced parent".into(),
            ));
        }
        self.metrics.split_catchup(
            cursor.delta_records,
            cursor.offset.saturating_sub(base_checkpoint.replay_offset),
            fence_lag_records,
        );

        self.finish_split(
            plan,
            CutoverChildren {
                base_checkpoint,
                left_target,
                right_target,
                left_tree,
                right_tree,
                left_rebuild,
                right_rebuild,
                left_base,
                right_base,
                cursor,
            },
            cutover_seq,
            fence_started,
        )
        .await
    }

    async fn finish_split(
        &self,
        plan: &SplitPlan,
        children: CutoverChildren,
        cutover_seq: u64,
        fence_started: Instant,
    ) -> Result<PreparedSplit> {
        let source = ChildOverlaySource {
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            checkpoint: children.base_checkpoint,
            cutover_offset: children.cursor.offset,
            cutover_seq,
            journal: Arc::clone(&self.journal),
        };
        let left = prepare_child(
            &plan.left,
            children.left_target,
            children.left_tree,
            children.left_base,
            &source,
        )?;
        let right = prepare_child(
            &plan.right,
            children.right_target,
            children.right_tree,
            children.right_base,
            &source,
        )?;
        let artifact = SplitArtifact {
            transition_id: plan.transition_id,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            cutover_seq,
            left: left.artifact.clone(),
            right: right.artifact.clone(),
        };
        self.record_split_artifact(artifact.clone()).await?;
        self.metrics.split_fence_duration(elapsed_us(fence_started));
        Ok(PreparedSplit {
            artifact,
            left,
            right,
            left_rebuild: children.left_rebuild,
            right_rebuild: children.right_rebuild,
            delta_records: children.cursor.delta_records,
        })
    }

    async fn split_base_snapshot(&self, plan: &SplitPlan) -> Result<(Checkpoint, Arc<dyn PartitionTree>)> {
        let transition = self.split_transition.lock().await;
        if transition.as_ref().map(|active| &active.plan) != Some(plan)
            || self.lifecycle() != PartitionLifecycle::SplitPreparing
        {
            return Err(ChunkKvError::SplitRetry(
                "split base no longer matches the active transition".into(),
            ));
        }
        let replay_offset = self.retry_replay_offset.load(Ordering::Acquire);
        let stream_manifest_generation = self.journal.manifest_generation();
        let (tree_manifest, applied_seq, source) = self.tree.checkpoint_snapshot(replay_offset).await?;
        if applied_seq > self.journal_durable_seq.load(Ordering::Acquire) {
            return Err(ChunkKvError::ApplyStateUnknown);
        }
        Ok((
            Checkpoint {
                tree_id: self.tree.tree_id(),
                tree_manifest,
                applied_seq,
                stream_name: self.journal.stream_name(),
                stream_manifest_generation,
                replay_offset,
            },
            source,
        ))
    }

    async fn cancel_local_split(&self, transition_id: crate::TransitionId) {
        let mut transition = self.split_transition.lock().await;
        if transition.as_ref().map(|active| active.plan.transition_id) == Some(transition_id)
            && self.lifecycle() == PartitionLifecycle::SplitPreparing
        {
            *transition = None;
            self.lifecycle.store(
                super::lifecycle_code(PartitionLifecycle::Serving),
                Ordering::Release,
            );
        }
    }
}

fn validate_targets(
    parent: &Partition,
    plan: &SplitPlan,
    left: &SplitChildTarget,
    right: &SplitChildTarget,
) -> Result<()> {
    if left.tree_id == 0
        || right.tree_id == 0
        || left.tree_id == right.tree_id
        || left.tree_id == parent.tree.tree_id()
        || right.tree_id == parent.tree.tree_id()
        || left.journal.tail() != 0
        || right.journal.tail() != 0
        || left.journal.stream_name() == right.journal.stream_name()
        || left.journal.stream_name() == parent.journal.stream_name()
        || right.journal.stream_name() == parent.journal.stream_name()
        || left.journal.manifest_generation() == 0
        || right.journal.manifest_generation() == 0
    {
        return Err(ChunkKvError::InvalidRequest(
            "split child storage identities must be distinct and empty".into(),
        ));
    }
    plan.validate()
}

struct DeltaCursor {
    offset: u64,
    applied_seq: u64,
    delta_records: u64,
}

impl DeltaCursor {
    async fn catch_up_to_lag(
        &mut self,
        parent: &Partition,
        plan: &SplitPlan,
        left_tree: &dyn PartitionTree,
        right_tree: &dyn PartitionTree,
        max_lag_records: u64,
    ) -> Result<u64> {
        loop {
            let target = parent.applied_seq.load(Ordering::Acquire);
            replay_children_until(
                parent,
                self,
                target,
                left_tree,
                right_tree,
                &plan.left,
                &plan.right,
            )
            .await?;
            let lag = parent
                .applied_seq
                .load(Ordering::Acquire)
                .saturating_sub(self.applied_seq);
            if lag <= max_lag_records {
                return Ok(lag);
            }
            tokio::task::yield_now().await;
        }
    }
}

struct CutoverChildren {
    base_checkpoint: Checkpoint,
    left_target: SplitChildTarget,
    right_target: SplitChildTarget,
    left_tree: Arc<dyn PartitionTree>,
    right_tree: Arc<dyn PartitionTree>,
    left_rebuild: crowdb_tree_ffi::RangeRebuildStats,
    right_rebuild: crowdb_tree_ffi::RangeRebuildStats,
    left_base: (u64, u64),
    right_base: (u64, u64),
    cursor: DeltaCursor,
}

struct ChildOverlaySource {
    parent_id: crate::PartitionId,
    parent_epoch: u64,
    checkpoint: Checkpoint,
    cutover_offset: u64,
    cutover_seq: u64,
    journal: Arc<dyn PartitionJournal>,
}

#[allow(clippy::too_many_arguments)]
async fn replay_children_until(
    parent: &Partition,
    cursor: &mut DeltaCursor,
    target_seq: u64,
    left_tree: &dyn PartitionTree,
    right_tree: &dyn PartitionTree,
    left: &SplitChild,
    right: &SplitChild,
) -> Result<()> {
    const WINDOW_BYTES: usize = 1024 * 1024;
    let tail = parent.journal.tail();
    let mut read_offset = cursor.offset;
    let mut frame_offset = cursor.offset;
    let mut buffered = BytesMut::new();
    while read_offset < tail && cursor.applied_seq < target_seq {
        let bytes = parent.journal.read_window(read_offset, WINDOW_BYTES).await?;
        if bytes.is_empty() {
            return Err(ChunkKvError::JournalCorruption(
                "split replay returned no bytes before the durable tail".into(),
            ));
        }
        read_offset = read_offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ChunkKvError::JournalCorruption("split replay offset overflows".into()))?;
        buffered.extend_from_slice(&bytes);
        while let FrameDecode::Complete(decoded) = decode_frame(&buffered)? {
            if decoded.record.mutation_seq > target_seq {
                return Ok(());
            }
            super::validate_replay_record(
                parent.id,
                parent.ownership_epoch.load(Ordering::Acquire),
                &decoded.record,
            )?;
            apply_child_record(cursor, &decoded.record, left_tree, right_tree, left, right).await?;
            let consumed = decoded.bytes_consumed;
            buffered.advance(consumed);
            frame_offset = frame_offset
                .checked_add(consumed as u64)
                .ok_or_else(|| ChunkKvError::JournalCorruption("split frame offset overflows".into()))?;
            cursor.offset = frame_offset;
            if cursor.applied_seq == target_seq {
                return Ok(());
            }
        }
        if buffered.len() > MAX_FRAME_BYTES {
            return Err(ChunkKvError::JournalCorruption(
                "split replay frame exceeds maximum size".into(),
            ));
        }
    }
    Ok(())
}

async fn apply_child_record(
    cursor: &mut DeltaCursor,
    record: &WalRecord,
    left_tree: &dyn PartitionTree,
    right_tree: &dyn PartitionTree,
    left: &SplitChild,
    right: &SplitChild,
) -> Result<()> {
    if record.mutation_seq <= cursor.applied_seq {
        return Ok(());
    }
    if record.mutation_seq != cursor.applied_seq.saturating_add(1) {
        return Err(ChunkKvError::JournalCorruption(
            "split delta contains a mutation sequence gap".into(),
        ));
    }
    let matches_left = left.range.contains(record.operation.key());
    let matches_right = right.range.contains(record.operation.key());
    if matches_left == matches_right {
        return Err(ChunkKvError::JournalCorruption(
            "split delta does not belong to exactly one child".into(),
        ));
    }
    for (tree, matches) in [(left_tree, matches_left), (right_tree, matches_right)] {
        if matches && record.result.applied() {
            tree.apply(record.mutation_seq, &record.operation).await?;
        } else {
            tree.advance_noop(record.mutation_seq).await?;
        }
    }
    cursor.applied_seq = record.mutation_seq;
    cursor.delta_records = cursor.delta_records.saturating_add(1);
    Ok(())
}

fn prepare_child(
    child: &SplitChild,
    target: SplitChildTarget,
    tree: Arc<dyn PartitionTree>,
    (tree_manifest, base_applied_seq): (u64, u64),
    source: &ChildOverlaySource,
) -> Result<PreparedSplitChild> {
    if tree.last_applied_seq() != source.cutover_seq || base_applied_seq > source.cutover_seq {
        return Err(ChunkKvError::ApplyStateUnknown);
    }
    let artifact = PreparedChildArtifact {
        partition_id: child.partition_id,
        range: child.range.clone(),
        ownership_epoch: child.ownership_epoch,
        tree_id: target.tree_id,
        tree_manifest,
        stream_name: target.journal.stream_name(),
        base_applied_seq,
        parent_id: source.parent_id,
        parent_epoch: source.parent_epoch,
        parent_stream_name: source.checkpoint.stream_name,
        parent_stream_manifest_generation: source.checkpoint.stream_manifest_generation,
        parent_replay_offset: source.checkpoint.replay_offset,
        parent_cutover_offset: source.cutover_offset,
        applied_seq: source.cutover_seq,
        child_stream_start_seq: source
            .cutover_seq
            .checked_add(1)
            .ok_or_else(|| ChunkKvError::InvalidRequest("split cutover sequence overflows".into()))?,
    };
    let checkpoint = Checkpoint {
        tree_id: target.tree_id,
        tree_manifest,
        applied_seq: base_applied_seq,
        stream_name: target.journal.stream_name(),
        stream_manifest_generation: target.journal.manifest_generation(),
        replay_offset: 0,
    };
    Ok(PreparedSplitChild {
        artifact,
        checkpoint,
        tree,
        journal: target.journal,
        parent_journal: Arc::clone(&source.journal),
    })
}

async fn checkpoint_prepared_tree(tree: &dyn PartitionTree, expected_seq: u64) -> Result<(u64, u64)> {
    tree.checkpoint(0).await?;
    let checkpoint = tree.checkpoint_state()?;
    if checkpoint.1 != expected_seq || checkpoint.1 > tree.last_applied_seq() {
        return Err(ChunkKvError::ApplyStateUnknown);
    }
    Ok(checkpoint)
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
