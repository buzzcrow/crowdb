// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Catalog-fenced extent metadata and split-overlay maintenance.

use super::{
    local_partition_for_entry, Arc, ChunkKvError, ChunkKvRangeCatalogPartitionState, ChunkKvService, Id128,
};

impl ChunkKvService {
    /// Runs one bounded ownership-materialization pass for every local split child.
    ///
    /// Returns child identities whose own checkpoint no longer depends on the
    /// split-parent suffix.
    ///
    /// # Errors
    ///
    /// Returns the first materialization or checkpoint error.
    pub async fn materialize_split_overlays(&self) -> Result<Vec<Id128>, ChunkKvError> {
        let catalog = self.catalog.load_full();
        let partitions = self.partitions.load_full();
        let mut completed = Vec::new();
        for entry in &catalog.entries {
            if entry.owner.instance_id != self.instance_id
                || entry.state != ChunkKvRangeCatalogPartitionState::Serving
                || entry.artifact.tail_overlay.is_none()
                || self
                    .independently_recoverable
                    .load()
                    .contains(&(entry.partition_id, entry.owner_epoch))
            {
                continue;
            }
            let Some(partition) = partitions.get(&entry.partition_id) else {
                continue;
            };
            if partition.snapshot().lifecycle != crowdb_chunk_kv::PartitionLifecycle::Serving {
                continue;
            }
            if !partition
                .materialize_split_ownership(entry.owner_epoch)
                .await?
                .complete
            {
                continue;
            }
            let checkpoint = partition.checkpoint(entry.owner_epoch).await?;
            let Some(overlay) = entry.artifact.tail_overlay.as_ref() else {
                continue;
            };
            if checkpoint.applied_seq < overlay.cutover_seq {
                return Err(ChunkKvError::ApplyStateUnknown);
            }
            self.independently_recoverable.rcu(|current| {
                let mut next = (**current).clone();
                next.insert((entry.partition_id, entry.owner_epoch));
                Arc::new(next)
            });
            completed.push(entry.partition_id);
        }
        Ok(completed)
    }

    /// Reclaims a bounded extent metadata batch for each independently recoverable owner.
    ///
    /// Returns true while any owner has a metadata backlog. Catalog transitions,
    /// overlays, expired grants and mismatched local handles cannot authorize GC.
    ///
    /// # Errors
    /// Returns a storage maintenance failure.
    pub async fn maintain_serving_metadata(&self) -> Result<bool, ChunkKvError> {
        let catalog = self.catalog.load_full();
        let partitions = self.partitions.load_full();
        let mut pending = false;
        let mut first_error = None;
        for entry in &catalog.entries {
            if entry.owner.instance_id != self.instance_id
                || entry.state != ChunkKvRangeCatalogPartitionState::Serving
                || entry.artifact.tail_overlay.is_some()
                || entry.transition_id.is_some()
                || self
                    .authority
                    .authorize(
                        catalog.generation,
                        entry.partition_id,
                        entry.owner_epoch,
                        self.monotonic_ms(),
                    )
                    .is_err()
            {
                continue;
            }
            let Some(partition) = partitions
                .get(&entry.partition_id)
                .and_then(|partition| local_partition_for_entry(partition, entry))
            else {
                continue;
            };
            match partition.maintain_serving_metadata(entry.owner_epoch).await {
                Ok(backlog) => pending |= backlog,
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(pending), Err)
    }
}
