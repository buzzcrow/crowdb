// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Online split build and bounded final catch-up.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Buf, BytesMut};

use super::{decode_frame, FrameDecode, Partition, PartitionJournal, PartitionTree};
use crate::{
    Checkpoint, ChunkKvError, PartitionConfig, PartitionLifecycle, PreparedSplitWriterArtifact, Result,
    SplitArtifact, SplitChild, SplitPlan, WalRecord, MAX_FRAME_BYTES,
};

#[derive(Clone)]
/// Durable destination for either writer in a split session.
pub struct SplitWriterTarget {
    pub tree_id: u64,
    pub tree_config: crowdb_tree_ffi::Config,
    pub journal: Arc<dyn PartitionJournal>,
}

/// The two independently durable writers created by a split session.
#[derive(Clone)]
pub struct SplitSessionTargets {
    pub retained_parent: SplitWriterTarget,
    pub child: SplitWriterTarget,
}

pub struct PreparedSplitWriter {
    artifact: crate::PreparedSplitWriterArtifact,
    checkpoint: Checkpoint,
    tree: Arc<dyn PartitionTree>,
    journal: Arc<dyn PartitionJournal>,
    parent_journal: Arc<dyn PartitionJournal>,
}

impl PreparedSplitWriter {
    #[must_use]
    pub fn artifact(&self) -> &crate::PreparedSplitWriterArtifact {
        &self.artifact
    }

    #[must_use]
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Opens one completed split writer in `Prepared` state. Catalog proof is
    /// still required before it can serve.
    ///
    /// # Errors
    ///
    /// Returns a checkpoint, journal, tree, or writer identity error.
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
    pub retained_parent: Option<PreparedSplitWriter>,
    pub child: PreparedSplitWriter,
    pub child_rebuild: crowdb_tree_ffi::RangeRebuildStats,
    pub delta_records: u64,
}

impl Partition {
    /// Completes the durable retained-parent target after the common split
    /// frontier is established.  Callers use this session form so the catalog
    /// can publish two new writer identities rather than shrinking the old
    /// tree in place.
    ///
    /// # Errors
    ///
    /// Returns an error if either target is invalid, the shared view cannot
    /// be published durably to both writers, or split preparation fails.
    pub async fn prepare_split_session(
        &self,
        plan: SplitPlan,
        targets: SplitSessionTargets,
        max_catchup_lag_records: u64,
    ) -> Result<PreparedSplit> {
        validate_target(self, &plan, &targets.child)?;
        validate_target(self, &plan, &targets.retained_parent)?;
        if targets.child.tree_id == targets.retained_parent.tree_id
            || targets.child.journal.stream_name() == targets.retained_parent.journal.stream_name()
        {
            return Err(ChunkKvError::InvalidRequest(
                "split writer storage identities must be distinct".into(),
            ));
        }
        let retained_spec = SplitChild {
            partition_id: plan.parent_id,
            range: plan.parent_range.split(&plan.split_key)?.0,
            ownership_epoch: plan.parent_next_epoch,
        };
        self.begin_split(plan.clone()).await?;
        let shared_view_generation = self.tree.begin_split_memtable_view().await?;
        let preparation_started = Instant::now();
        let result = self
            .build_split_session(
                &plan,
                retained_spec,
                targets,
                shared_view_generation,
                max_catchup_lag_records,
            )
            .await;
        if result.is_ok() {
            self.metrics
                .split_preparation_duration(elapsed_us(preparation_started));
        }
        if result.is_err()
            && matches!(
                self.lifecycle(),
                PartitionLifecycle::SplitPreparing | PartitionLifecycle::SplitFinalizing
            )
        {
            self.cancel_local_split(plan.transition_id).await;
        }
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn build_split_session(
        &self,
        plan: &SplitPlan,
        retained_spec: SplitChild,
        targets: SplitSessionTargets,
        shared_view_generation: u64,
        max_catchup_lag_records: u64,
    ) -> Result<PreparedSplit> {
        if max_catchup_lag_records == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "split catch-up lag bound must be nonzero".into(),
            ));
        }
        let (base_checkpoint, source) = self.split_base_snapshot(plan).await?;
        let (retained_tree, retained_rebuild) = source
            .rebuild_range(
                targets.retained_parent.tree_id,
                &retained_spec.range,
                targets.retained_parent.tree_config.clone(),
            )
            .await?;
        let (child_tree, child_rebuild) = source
            .rebuild_range(
                targets.child.tree_id,
                &plan.child.range,
                targets.child.tree_config.clone(),
            )
            .await?;
        if retained_tree.last_applied_seq() != base_checkpoint.applied_seq
            || child_tree.last_applied_seq() != base_checkpoint.applied_seq
        {
            return Err(ChunkKvError::TreeCorruption(
                "split writers do not share the parent base frontier".into(),
            ));
        }
        self.metrics.split_rebuild(retained_rebuild);
        self.metrics.split_rebuild(child_rebuild);
        let mut retained_cursor = DeltaCursor::from_checkpoint(&base_checkpoint);
        let mut child_cursor = DeltaCursor::from_checkpoint(&base_checkpoint);
        retained_cursor
            .catch_up_to_lag(
                self,
                plan,
                retained_tree.as_ref(),
                &retained_spec,
                max_catchup_lag_records,
            )
            .await?;
        let catchup_lag_records = child_cursor
            .catch_up_to_lag(
                self,
                plan,
                child_tree.as_ref(),
                &plan.child,
                max_catchup_lag_records,
            )
            .await?;
        self.begin_split_ingress_buffer()?;
        let finalization_started = Instant::now();
        self.begin_split_finalization(plan.transition_id).await?;
        let cutover_seq = self.applied_seq.load(Ordering::Acquire);
        replay_writer_until(
            self,
            &mut retained_cursor,
            cutover_seq,
            retained_tree.as_ref(),
            &plan.parent_range,
            &retained_spec,
        )
        .await?;
        replay_writer_until(
            self,
            &mut child_cursor,
            cutover_seq,
            child_tree.as_ref(),
            &plan.parent_range,
            &plan.child,
        )
        .await?;
        if retained_cursor.applied_seq != cutover_seq
            || child_cursor.applied_seq != cutover_seq
            || retained_cursor.offset != child_cursor.offset
        {
            return Err(ChunkKvError::JournalCorruption(
                "split writers did not reach one common durable frontier".into(),
            ));
        }
        let source = SplitSourceFrontier {
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            checkpoint: base_checkpoint,
            cutover_offset: child_cursor.offset,
            cutover_seq,
            journal: Arc::clone(&self.journal),
        };
        let retained_base = checkpoint_prepared_tree(retained_tree.as_ref(), cutover_seq).await?;
        let child_base = checkpoint_prepared_tree(child_tree.as_ref(), cutover_seq).await?;
        retained_tree.unpin_generation(plan.transition_id)?;
        retained_tree.pin_generation(plan.transition_id, retained_base.1)?;
        child_tree.unpin_generation(plan.transition_id)?;
        child_tree.pin_generation(plan.transition_id, child_base.1)?;
        let mut retained = prepare_writer(
            &retained_spec,
            targets.retained_parent,
            retained_tree,
            retained_base,
            &source,
        )?;
        let mut child = prepare_writer(&plan.child, targets.child, child_tree, child_base, &source)?;
        self.tree
            .publish_split_memtable_view(
                shared_view_generation,
                retained.tree.as_ref(),
                &retained_spec.range,
            )
            .await?;
        self.tree
            .publish_split_memtable_view(shared_view_generation, child.tree.as_ref(), &plan.child.range)
            .await?;
        let retained_final = checkpoint_prepared_tree(retained.tree.as_ref(), cutover_seq).await?;
        let child_final = checkpoint_prepared_tree(child.tree.as_ref(), cutover_seq).await?;
        rebase_prepared_writer(&mut retained, retained_final)?;
        rebase_prepared_writer(&mut child, child_final)?;
        let artifact = SplitArtifact {
            transition_id: plan.transition_id,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            parent_next_epoch: plan.parent_next_epoch,
            shared_view_generation,
            cutover_seq,
            retained_parent: retained.artifact.clone(),
            child: child.artifact.clone(),
        };
        self.record_split_artifact(artifact.clone()).await?;
        self.tree
            .release_split_memtable_view(shared_view_generation)
            .await?;
        self.metrics.split_catchup(
            retained_cursor
                .delta_records
                .saturating_add(child_cursor.delta_records),
            child_cursor
                .offset
                .saturating_sub(source.checkpoint.replay_offset),
            catchup_lag_records,
        );
        self.metrics
            .split_finalization_duration(elapsed_us(finalization_started));
        Ok(PreparedSplit {
            artifact,
            retained_parent: Some(retained),
            child,
            child_rebuild,
            delta_records: child_cursor.delta_records,
        })
    }

    /// Legacy single-writer split preparation. Split sessions must use
    /// [`Self::prepare_split_session`] so both durable writers are created
    /// from the same shared view.
    ///
    /// # Errors
    ///
    /// Returns a split retry, storage, journal, or apply error.
    pub async fn prepare_split(
        &self,
        plan: SplitPlan,
        child_target: SplitWriterTarget,
        max_catchup_lag_records: u64,
    ) -> Result<PreparedSplit> {
        if max_catchup_lag_records == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "split catch-up lag bound must be nonzero".into(),
            ));
        }
        validate_target(self, &plan, &child_target)?;
        self.begin_split(plan.clone()).await?;
        let preparation_started = Instant::now();
        let result = self
            .build_split(&plan, child_target, max_catchup_lag_records)
            .await;
        if result.is_ok() {
            self.metrics
                .split_preparation_duration(elapsed_us(preparation_started));
        }
        if result.is_err()
            && matches!(
                self.lifecycle(),
                PartitionLifecycle::SplitPreparing | PartitionLifecycle::SplitFinalizing
            )
        {
            self.cancel_local_split(plan.transition_id).await;
        }
        result
    }

    async fn build_split(
        &self,
        plan: &SplitPlan,
        child_target: SplitWriterTarget,
        max_catchup_lag_records: u64,
    ) -> Result<PreparedSplit> {
        let (base_checkpoint, source) = self.split_base_snapshot(plan).await?;
        let (child_tree, child_rebuild) = source
            .rebuild_range(
                child_target.tree_id,
                &plan.child.range,
                child_target.tree_config.clone(),
            )
            .await?;
        if child_tree.last_applied_seq() != base_checkpoint.applied_seq {
            return Err(ChunkKvError::TreeCorruption(
                "split child does not share the parent base frontier".into(),
            ));
        }
        self.metrics.split_rebuild(child_rebuild);

        let mut cursor = DeltaCursor {
            offset: base_checkpoint.replay_offset,
            applied_seq: base_checkpoint.applied_seq,
            delta_records: 0,
        };
        cursor
            .catch_up_to_lag(
                self,
                plan,
                child_tree.as_ref(),
                &plan.child,
                max_catchup_lag_records,
            )
            .await?;

        let checkpoint_started = Instant::now();
        let child_base = checkpoint_prepared_tree(child_tree.as_ref(), cursor.applied_seq).await?;
        self.metrics
            .split_base_checkpoint_duration(elapsed_us(checkpoint_started));

        let catchup_lag_records = cursor
            .catch_up_to_lag(
                self,
                plan,
                child_tree.as_ref(),
                &plan.child,
                max_catchup_lag_records,
            )
            .await?;

        self.begin_split_ingress_buffer()?;
        let finalization_started = Instant::now();
        self.begin_split_finalization(plan.transition_id).await?;
        let cutover_seq = self.applied_seq.load(Ordering::Acquire);
        replay_writer_until(
            self,
            &mut cursor,
            cutover_seq,
            child_tree.as_ref(),
            &plan.parent_range,
            &plan.child,
        )
        .await?;
        if cursor.applied_seq != cutover_seq {
            return Err(ChunkKvError::JournalCorruption(
                "split delta replay did not reach the common frontier".into(),
            ));
        }
        self.metrics.split_catchup(
            cursor.delta_records,
            cursor.offset.saturating_sub(base_checkpoint.replay_offset),
            catchup_lag_records,
        );

        self.finish_split(
            plan,
            CutoverChild {
                base_checkpoint,
                child_target,
                child_tree,
                child_rebuild,
                child_base,
                cursor,
            },
            cutover_seq,
            finalization_started,
        )
        .await
    }

    async fn finish_split(
        &self,
        plan: &SplitPlan,
        child_state: CutoverChild,
        cutover_seq: u64,
        finalization_started: Instant,
    ) -> Result<PreparedSplit> {
        // A restarted ParentPreparing attempt may have left a pin after the
        // child checkpoint but before readiness became durable. No published
        // artifact can reference it in this phase, so replace that stale pin
        // with the exact generation produced by this retry.
        child_state.child_tree.unpin_generation(plan.transition_id)?;
        child_state
            .child_tree
            .pin_generation(plan.transition_id, child_state.child_base.1)?;
        let source = SplitSourceFrontier {
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            checkpoint: child_state.base_checkpoint,
            cutover_offset: child_state.cursor.offset,
            cutover_seq,
            journal: Arc::clone(&self.journal),
        };
        let child = match prepare_writer(
            &plan.child,
            child_state.child_target,
            Arc::clone(&child_state.child_tree),
            child_state.child_base,
            &source,
        ) {
            Ok(child) => child,
            Err(error) => {
                let _ = child_state.child_tree.unpin_generation(plan.transition_id);
                return Err(error);
            }
        };
        let retained_checkpoint = checkpoint_prepared_tree(self.tree.as_ref(), cutover_seq).await?;
        let retained_parent = prepare_retained_parent(plan, retained_checkpoint, &source)?;
        let artifact = SplitArtifact {
            transition_id: plan.transition_id,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            parent_next_epoch: plan.parent_next_epoch,
            shared_view_generation: 0,
            cutover_seq,
            retained_parent,
            child: child.artifact.clone(),
        };
        if let Err(error) = self.record_split_artifact(artifact.clone()).await {
            let _ = child_state.child_tree.unpin_generation(plan.transition_id);
            return Err(error);
        }
        self.metrics
            .split_finalization_duration(elapsed_us(finalization_started));
        Ok(PreparedSplit {
            artifact,
            retained_parent: None,
            child,
            child_rebuild: child_state.child_rebuild,
            delta_records: child_state.cursor.delta_records,
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
        let root_manifest_generation = self.tree.root_manifest_generation()?;
        if applied_seq > self.journal_durable_seq.load(Ordering::Acquire) {
            return Err(ChunkKvError::ApplyStateUnknown);
        }
        Ok((
            Checkpoint {
                tree_id: self.tree.tree_id(),
                tree_manifest,
                root_manifest_generation,
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
            && matches!(
                self.lifecycle(),
                PartitionLifecycle::SplitPreparing | PartitionLifecycle::SplitFinalizing
            )
        {
            *transition = None;
            self.split_ingress.store(None);
            self.lifecycle.store(
                super::lifecycle_code(PartitionLifecycle::Serving),
                Ordering::Release,
            );
        }
    }
}

fn rebase_prepared_writer(
    writer: &mut PreparedSplitWriter,
    (tree_manifest, root_manifest_generation, applied_seq): (u64, u64, u64),
) -> Result<()> {
    if applied_seq != writer.artifact.applied_seq {
        return Err(ChunkKvError::ApplyStateUnknown);
    }
    writer.artifact.tree_manifest = tree_manifest;
    writer.artifact.root_manifest_generation = root_manifest_generation;
    writer.artifact.base_applied_seq = applied_seq;
    writer.checkpoint = Checkpoint {
        tree_id: writer.artifact.tree_id,
        tree_manifest,
        root_manifest_generation,
        applied_seq,
        stream_name: writer.artifact.stream_name,
        stream_manifest_generation: writer.journal.manifest_generation(),
        replay_offset: 0,
    };
    Ok(())
}

fn prepare_retained_parent(
    plan: &SplitPlan,
    (tree_manifest, root_manifest_generation, applied_seq): (u64, u64, u64),
    source: &SplitSourceFrontier,
) -> Result<PreparedSplitWriterArtifact> {
    let (range, _) = plan.parent_range.split(&plan.split_key)?;
    Ok(PreparedSplitWriterArtifact {
        partition_id: plan.parent_id,
        range,
        ownership_epoch: plan.parent_next_epoch,
        tree_id: source.checkpoint.tree_id,
        tree_manifest,
        root_manifest_generation,
        stream_name: source.checkpoint.stream_name,
        base_applied_seq: applied_seq,
        parent_id: source.parent_id,
        parent_epoch: source.parent_epoch,
        parent_stream_name: source.checkpoint.stream_name,
        parent_stream_manifest_generation: source.checkpoint.stream_manifest_generation,
        parent_replay_offset: source.checkpoint.replay_offset,
        parent_cutover_offset: source.cutover_offset,
        applied_seq,
        child_stream_start_seq: applied_seq
            .checked_add(1)
            .ok_or_else(|| ChunkKvError::InvalidRequest("split cutover sequence overflows".into()))?,
    })
}

fn validate_target(parent: &Partition, plan: &SplitPlan, target: &SplitWriterTarget) -> Result<()> {
    if target.tree_id == 0
        || target.tree_id == parent.tree.tree_id()
        || target.journal.tail() != 0
        || target.journal.stream_name() == parent.journal.stream_name()
        || target.journal.manifest_generation() == 0
    {
        return Err(ChunkKvError::InvalidRequest(
            "split writer storage identities must be distinct and empty".into(),
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
    fn from_checkpoint(checkpoint: &Checkpoint) -> Self {
        Self {
            offset: checkpoint.replay_offset,
            applied_seq: checkpoint.applied_seq,
            delta_records: 0,
        }
    }

    async fn catch_up_to_lag(
        &mut self,
        parent: &Partition,
        plan: &SplitPlan,
        writer_tree: &dyn PartitionTree,
        writer: &SplitChild,
        max_lag_records: u64,
    ) -> Result<u64> {
        loop {
            let target = parent.applied_seq.load(Ordering::Acquire);
            replay_writer_until(parent, self, target, writer_tree, &plan.parent_range, writer).await?;
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

struct CutoverChild {
    base_checkpoint: Checkpoint,
    child_target: SplitWriterTarget,
    child_tree: Arc<dyn PartitionTree>,
    child_rebuild: crowdb_tree_ffi::RangeRebuildStats,
    child_base: (u64, u64, u64),
    cursor: DeltaCursor,
}

struct SplitSourceFrontier {
    parent_id: crate::PartitionId,
    parent_epoch: u64,
    checkpoint: Checkpoint,
    cutover_offset: u64,
    cutover_seq: u64,
    journal: Arc<dyn PartitionJournal>,
}

#[allow(clippy::too_many_arguments)]
async fn replay_writer_until(
    parent: &Partition,
    cursor: &mut DeltaCursor,
    target_seq: u64,
    writer_tree: &dyn PartitionTree,
    parent_range: &crate::PartitionRange,
    writer: &SplitChild,
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
            apply_writer_record(cursor, &decoded.record, writer_tree, parent_range, writer).await?;
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

async fn apply_writer_record(
    cursor: &mut DeltaCursor,
    record: &WalRecord,
    writer_tree: &dyn PartitionTree,
    parent_range: &crate::PartitionRange,
    writer: &SplitChild,
) -> Result<()> {
    if record.mutation_seq <= cursor.applied_seq {
        return Ok(());
    }
    if record.mutation_seq != cursor.applied_seq.saturating_add(1) {
        return Err(ChunkKvError::JournalCorruption(
            "split delta contains a mutation sequence gap".into(),
        ));
    }
    if !parent_range.contains(record.operation.key()) {
        return Err(ChunkKvError::JournalCorruption(
            "split delta is outside the retained-parent and child ranges".into(),
        ));
    }
    if writer.range.contains(record.operation.key()) && record.result.applied() {
        writer_tree.apply(record.mutation_seq, &record.operation).await?;
    } else {
        writer_tree.advance_noop(record.mutation_seq).await?;
    }
    cursor.applied_seq = record.mutation_seq;
    cursor.delta_records = cursor.delta_records.saturating_add(1);
    Ok(())
}

fn prepare_writer(
    writer: &SplitChild,
    target: SplitWriterTarget,
    tree: Arc<dyn PartitionTree>,
    (tree_manifest, root_manifest_generation, base_applied_seq): (u64, u64, u64),
    source: &SplitSourceFrontier,
) -> Result<PreparedSplitWriter> {
    if tree.last_applied_seq() != source.cutover_seq || base_applied_seq > source.cutover_seq {
        return Err(ChunkKvError::ApplyStateUnknown);
    }
    let artifact = PreparedSplitWriterArtifact {
        partition_id: writer.partition_id,
        range: writer.range.clone(),
        ownership_epoch: writer.ownership_epoch,
        tree_id: target.tree_id,
        tree_manifest,
        root_manifest_generation,
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
        root_manifest_generation,
        applied_seq: base_applied_seq,
        stream_name: target.journal.stream_name(),
        stream_manifest_generation: target.journal.manifest_generation(),
        replay_offset: 0,
    };
    Ok(PreparedSplitWriter {
        artifact,
        checkpoint,
        tree,
        journal: target.journal,
        parent_journal: Arc::clone(&source.journal),
    })
}

async fn checkpoint_prepared_tree(tree: &dyn PartitionTree, expected_seq: u64) -> Result<(u64, u64, u64)> {
    tree.checkpoint(0).await?;
    let checkpoint = tree.checkpoint_state()?;
    if checkpoint.1 != expected_seq || checkpoint.1 > tree.last_applied_seq() {
        return Err(ChunkKvError::ApplyStateUnknown);
    }
    Ok((checkpoint.0, tree.root_manifest_generation()?, checkpoint.1))
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
