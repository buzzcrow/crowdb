// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Checkpoint frontiers and transition retention.

use std::sync::atomic::Ordering;

use super::{
    read_state_error, Checkpoint, CheckpointReclaim, ChunkKvError, Partition, PartitionLifecycle, Result,
    TransitionId,
};

impl Partition {
    /// Creates an exact checkpoint while a transfer source is quiesced.
    ///
    /// # Errors
    ///
    /// Returns an error unless the partition is drained and fenced, or if the
    /// tree checkpoint fails.
    pub async fn checkpoint_quiesced(&self, ownership_epoch: u64) -> Result<Checkpoint> {
        let _maintenance = self.split_transition.lock().await;
        self.validate_epoch(ownership_epoch)?;
        if self.lifecycle() != PartitionLifecycle::TransferQuiesced
            || self.queued_requests.load(Ordering::Acquire) != 0
        {
            return Err(ChunkKvError::InvalidRequest(
                "checkpoint requires a quiesced mutation source".into(),
            ));
        }
        self.create_checkpoint().await
    }

    /// Publishes a recoverable checkpoint while the partition continues
    /// serving. The replay offset is captured before the tree flush, so a
    /// concurrent mutation can only make recovery replay an already included
    /// record; it cannot create a WAL gap.
    ///
    /// # Errors
    ///
    /// Returns a stale-epoch, lifecycle, tree, or frontier error.
    pub async fn checkpoint(&self, ownership_epoch: u64) -> Result<Checkpoint> {
        let _maintenance = self.split_transition.lock().await;
        self.validate_epoch(ownership_epoch)?;
        if self.checkpoint_pin_generation.load(Ordering::Acquire) != 0 {
            return Err(ChunkKvError::SplitRetry(
                "checkpoint publication is suppressed by an exact-root transition pin".into(),
            ));
        }
        match self.lifecycle() {
            PartitionLifecycle::Serving
            | PartitionLifecycle::TransferQuiesced
            | PartitionLifecycle::SplitFinalizing => self.create_checkpoint().await,
            state => Err(read_state_error(state)),
        }
    }

    pub(super) async fn create_checkpoint(&self) -> Result<Checkpoint> {
        let replay_offset = self.retry_replay_offset.load(Ordering::Acquire);
        self.journal.checkpoint().await?;
        let stream_generation = self.journal.record_generation();
        let (tree_manifest, applied_seq) = self.tree.checkpoint(replay_offset).await?;
        let root_manifest_generation = self.tree.root_manifest_generation()?;
        if applied_seq > self.journal_durable_seq.load(Ordering::Acquire)
            || self.tree.last_applied_seq() < applied_seq
        {
            return Err(ChunkKvError::ApplyStateUnknown);
        }
        self.metrics.checkpoint();
        Ok(Checkpoint {
            tree_id: self.tree.tree_id(),
            tree_manifest,
            root_manifest_generation,
            applied_seq,
            stream_name: self.journal.stream_name(),
            stream_generation,
            replay_offset,
        })
    }

    /// Trims WAL bytes only after the caller supplies the published checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for stale identity/frontiers, a nonzero replay offset
    /// without durable expired-request floors, or journal GC failure.
    pub async fn trim_published_checkpoint(
        &self,
        ownership_epoch: u64,
        checkpoint: &Checkpoint,
    ) -> Result<u64> {
        let _maintenance = self.split_transition.lock().await;
        self.ensure_reclaim_allowed(ownership_epoch)?;
        self.trim_checkpoint(checkpoint).await
    }

    async fn trim_checkpoint(&self, checkpoint: &Checkpoint) -> Result<u64> {
        if checkpoint.tree_id != self.tree.tree_id()
            || checkpoint.stream_name != self.journal.stream_name()
            || checkpoint.stream_generation == 0
            || checkpoint.stream_generation > self.journal.record_generation()
            || checkpoint.applied_seq > self.applied_seq.load(Ordering::Acquire)
            || checkpoint.replay_offset > self.journal.tail()
            || checkpoint.replay_offset > self.retry_replay_offset.load(Ordering::Acquire)
        {
            return Err(ChunkKvError::InvalidRequest(
                "checkpoint does not belong to this frontier".into(),
            ));
        }
        if checkpoint.replay_offset != 0 {
            return Err(ChunkKvError::MaintenanceDegraded(
                "WAL prefix trim requires durable expired-request floors".into(),
            ));
        }
        self.journal.trim_prefix(checkpoint.replay_offset).await
    }

    /// Advances both durable retention watermarks after the caller has
    /// published this exact checkpoint, then reclaims unreferenced chunk
    /// objects left by interrupted page-store work.
    ///
    /// # Errors
    ///
    /// Returns an error for stale authority, a mismatched checkpoint, or a
    /// journal/tree maintenance failure.
    pub async fn reclaim_published_checkpoint(
        &self,
        ownership_epoch: u64,
        checkpoint: &Checkpoint,
    ) -> Result<CheckpointReclaim> {
        let _maintenance = self.split_transition.lock().await;
        self.ensure_reclaim_allowed(ownership_epoch)?;
        self.reclaim_checkpoint(checkpoint).await
    }

    pub(super) async fn reclaim_checkpoint(&self, checkpoint: &Checkpoint) -> Result<CheckpointReclaim> {
        let journal_bytes = self.trim_checkpoint(checkpoint).await?;
        let metadata_records = self
            .journal
            .reclaim_metadata_before(
                checkpoint.stream_generation,
                self.config.metadata_reclaim_records_per_pass,
            )
            .await?;
        let tree_bytes = self.tree.reclaim_before(checkpoint.root_manifest_generation)?;
        let orphan_bytes = self.tree.reclaim_orphans()?;
        self.metrics
            .reclaim(journal_bytes, metadata_records, tree_bytes, orphan_bytes);
        Ok(CheckpointReclaim {
            journal_bytes,
            metadata_records,
            tree_bytes,
            orphan_bytes,
        })
    }

    /// Publishes and pins one exact source checkpoint in the same maintenance turn.
    ///
    /// # Errors
    /// Returns a stale authority, lifecycle, checkpoint or durable pin error.
    pub async fn checkpoint_with_pin(
        &self,
        ownership_epoch: u64,
        transition_id: TransitionId,
    ) -> Result<Checkpoint> {
        let _maintenance = self.split_transition.lock().await;
        self.ensure_reclaim_allowed(ownership_epoch)?;
        let checkpoint = self.create_checkpoint().await?;
        self.retain_generation_pin(transition_id, checkpoint.root_manifest_generation)?;
        self.metadata_reclaim.store(None);
        Ok(checkpoint)
    }

    pub(super) fn ensure_reclaim_allowed(&self, ownership_epoch: u64) -> Result<()> {
        self.validate_epoch(ownership_epoch)?;
        if self.checkpoint_pin_generation.load(Ordering::Acquire) != 0 {
            return Err(ChunkKvError::SplitRetry(
                "history is retained by an exact-root transition pin".into(),
            ));
        }
        match self.lifecycle() {
            PartitionLifecycle::Serving | PartitionLifecycle::TransferQuiesced => Ok(()),
            state => Err(read_state_error(state)),
        }
    }
    /// Persists an exact root-generation reference before publishing a
    /// topology artifact that depends on it.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the generation is absent or conflicts
    /// with the transition's existing pin.
    pub(super) fn retain_generation_pin(&self, transition_id: TransitionId, generation: u64) -> Result<()> {
        let current = self.checkpoint_pin_generation.load(Ordering::Acquire);
        if current != 0
            && (current != generation
                || self.checkpoint_pin_transition_high.load(Ordering::Acquire) != transition_id.high
                || self.checkpoint_pin_transition_low.load(Ordering::Acquire) != transition_id.low)
        {
            return Err(ChunkKvError::InvalidRequest(
                "another transition already pins the partition checkpoint lineage".into(),
            ));
        }
        self.tree.pin_generation(transition_id, generation)?;
        self.checkpoint_pin_transition_high
            .store(transition_id.high, Ordering::Relaxed);
        self.checkpoint_pin_transition_low
            .store(transition_id.low, Ordering::Relaxed);
        self.checkpoint_pin_generation
            .store(generation, Ordering::Release);
        Ok(())
    }

    /// Releases this partition tree's durable generation pin after the
    /// authoritative catalog has removed its overlay dependency.
    ///
    /// # Errors
    ///
    /// Returns a storage error when durable pin deletion cannot complete.
    pub fn release_generation_pin(&self, transition_id: TransitionId) -> Result<()> {
        self.tree.unpin_generation(transition_id)?;
        if self.checkpoint_pin_transition_high.load(Ordering::Acquire) == transition_id.high
            && self.checkpoint_pin_transition_low.load(Ordering::Acquire) == transition_id.low
        {
            self.checkpoint_pin_generation.store(0, Ordering::Release);
        }
        Ok(())
    }
}
