// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reclamation of superseded extent directories for independently serving WALs.

use std::sync::{atomic::Ordering, Arc};

use super::{Partition, PartitionLifecycle, Result};

pub(super) struct MetadataReclaim {
    generation: u64,
    pending: bool,
}

impl Partition {
    /// Reclaims one bounded batch of superseded WAL extent pages.
    ///
    /// The current manifest still maps the complete retained WAL, so existing
    /// tree checkpoints and retry history remain recoverable. This does not
    /// trim WAL bytes or advance tree retention. Transition pins retain their
    /// exact manifest generations and suppress maintenance.
    ///
    /// Returns true when another batch may remain. Callers revisit other
    /// hosted partitions before continuing backlog work.
    ///
    /// # Errors
    /// Returns a stale epoch or metadata-store error. Failed deletion retains
    /// its watermark for the next pass.
    pub async fn maintain_serving_metadata(&self, ownership_epoch: u64) -> Result<bool> {
        let _maintenance = self.split_transition.lock().await;
        self.validate_epoch(ownership_epoch)?;
        if self.lifecycle() != PartitionLifecycle::Serving
            || self.checkpoint_pin_generation.load(Ordering::Acquire) != 0
        {
            self.metadata_reclaim.store(None);
            return Ok(false);
        }
        let current = self.journal.record_generation();
        let generation = match self.metadata_reclaim.load_full() {
            Some(retained) if retained.pending => retained.generation,
            Some(retained) if retained.generation == current => return Ok(false),
            _ => current,
        };
        self.metadata_reclaim.store(Some(Arc::new(MetadataReclaim {
            generation,
            pending: true,
        })));
        let reclaimed = self
            .journal
            .reclaim_metadata_before(generation, self.config.metadata_reclaim_records_per_pass)
            .await?;
        self.metrics.reclaim(0, reclaimed, 0, 0);
        let pending = reclaimed >= self.config.metadata_reclaim_records_per_pass as u64;
        self.metadata_reclaim
            .store(Some(Arc::new(MetadataReclaim { generation, pending })));
        Ok(pending)
    }
}
