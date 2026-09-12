// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Online split build and bounded final catch-up.

use std::sync::atomic::Ordering;
use std::sync::Arc;

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
        Partition::recover_prepared(self.artifact, self.checkpoint, config, self.tree, self.journal).await
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
        let result = self
            .build_split(&plan, left_target, right_target, max_fence_lag_records)
            .await;
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
        let fence_lag_records = loop {
            let target = self.applied_seq.load(Ordering::Acquire);
            replay_children_until(
                self,
                &mut cursor,
                target,
                left_tree.as_ref(),
                right_tree.as_ref(),
                &plan.left,
                &plan.right,
            )
            .await?;
            let latest = self.applied_seq.load(Ordering::Acquire);
            if latest.saturating_sub(cursor.applied_seq) <= max_fence_lag_records {
                break latest.saturating_sub(cursor.applied_seq);
            }
            tokio::task::yield_now().await;
        };

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
        self.metrics
            .split_catchup(cursor.delta_records, fence_lag_records);

        let left = checkpoint_child(&plan.left, left_target, left_tree, cutover_seq).await?;
        let right = checkpoint_child(&plan.right, right_target, right_tree, cutover_seq).await?;
        let artifact = SplitArtifact {
            transition_id: plan.transition_id,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            cutover_seq,
            left: left.artifact.clone(),
            right: right.artifact.clone(),
        };
        self.record_split_artifact(artifact.clone()).await?;
        Ok(PreparedSplit {
            artifact,
            left,
            right,
            left_rebuild,
            right_rebuild,
            delta_records: cursor.delta_records,
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
        let (tree_manifest, applied_seq, source) = self.tree.checkpoint_snapshot().await?;
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
            parent
                .journal
                .validate_frame_source(frame_offset, decoded.bytes_consumed, decoded.chunk_id)
                .await?;
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

async fn checkpoint_child(
    child: &SplitChild,
    target: SplitChildTarget,
    tree: Arc<dyn PartitionTree>,
    cutover_seq: u64,
) -> Result<PreparedSplitChild> {
    let (tree_manifest, applied_seq) = tree.checkpoint().await?;
    if applied_seq != cutover_seq || tree.last_applied_seq() != cutover_seq {
        return Err(ChunkKvError::ApplyStateUnknown);
    }
    let artifact = PreparedChildArtifact {
        partition_id: child.partition_id,
        range: child.range.clone(),
        ownership_epoch: child.ownership_epoch,
        tree_id: target.tree_id,
        tree_manifest,
        stream_name: target.journal.stream_name(),
        applied_seq,
    };
    let checkpoint = Checkpoint {
        tree_id: target.tree_id,
        tree_manifest,
        applied_seq,
        stream_name: target.journal.stream_name(),
        stream_manifest_generation: target.journal.manifest_generation(),
        replay_offset: 0,
    };
    Ok(PreparedSplitChild {
        artifact,
        checkpoint,
        tree,
        journal: target.journal,
    })
}
