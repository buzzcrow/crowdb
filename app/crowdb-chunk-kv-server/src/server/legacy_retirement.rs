// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resume explicitly started namespace retirement through the existing host pump.
use super::{local_partition_for_entry, ChunkKvRangeCatalogPartitionState, ChunkKvService};
use crate::storage::ChunkKvStorage;
use crowdb_chunk_kv::ChunkKvError;
use crowdb_chunk_stream::StreamRegistry;
use std::collections::HashSet;

impl ChunkKvService {
    /// Resume only durable, explicitly started legacy retirements for live local
    /// metadata namespaces. Missing proof waits for the next scheduled pass.
    /// # Errors
    /// Returns metadata, ownership, or physical cleanup failures without dropping claims.
    pub async fn resume_legacy_retirement(&self, storage: &ChunkKvStorage) -> Result<bool, ChunkKvError> {
        let catalog = self.catalog.load_full();
        let partitions = self.partitions.load_full();
        let mut groups = HashSet::new();
        let mut pending = false;
        for entry in &catalog.entries {
            if entry.owner.instance_id != self.instance_id
                || entry.state != ChunkKvRangeCatalogPartitionState::Serving
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
                || partitions
                    .get(&entry.partition_id)
                    .and_then(|partition| local_partition_for_entry(partition, entry))
                    .is_none()
            {
                continue;
            }
            let record = storage
                .streams()
                .registry()
                .load(entry.artifact.stream_name)
                .await
                .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
                .ok_or_else(|| {
                    ChunkKvError::MaintenanceDegraded("legacy namespace stream is absent".into())
                })?;
            if !groups.insert(record.metadata_group_id) {
                continue;
            }
            if let Some(progress) = storage
                .resume_legacy_storage(record.metadata_group_id, 64)
                .await
                .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
            {
                pending |= !progress.complete && progress.pending.is_none();
            }
        }
        Ok(pending)
    }
}
