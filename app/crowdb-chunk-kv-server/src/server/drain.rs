// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;

use super::{ChunkKvError, ChunkKvService};

impl ChunkKvService {
    /// Stops new admission and revokes successful serving replies. Admitted
    /// operations retain their partition handle and can finish durable work.
    pub fn begin_drain(&self) {
        self.admitting.store(false, Ordering::Release);
        self.load_sampling.close();
        self.authority.clear();
    }

    /// Drain admitted mutations and release each hosted WAL writer for a new boot.
    /// Background assignment changes must be stopped before calling this method.
    /// # Errors
    /// Leaves uncertain or fenced writers subject to their normal lease exclusion.
    pub async fn drain_writers(&self) -> Result<(), ChunkKvError> {
        self.begin_drain();
        self.load_sampling.drain().await;
        let partitions = self.partitions.load_full();
        let prepared = self.prepared_transfer_targets.load_full();
        let splits = self.local_split_sessions.load_full();
        let mut first_error = None;
        for partition in partitions
            .values()
            .chain(prepared.values())
            .chain(splits.values().map(|entry| &entry.dispatcher))
        {
            if let Err(error) = partition
                .suspend_for_shutdown(partition.snapshot().ownership_epoch)
                .await
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Release hosted Tree owners after background tasks and RPC handlers end.
    /// Independent snapshots keep their exact backend owners until their own drop.
    /// # Errors
    /// Reports a failure of the host blocking task that closes native Trees.
    pub async fn release_hosted_trees(&self) -> Result<(), ChunkKvError> {
        self.begin_drain();
        let partitions = self.partitions.swap(std::sync::Arc::default());
        let prepared = self.prepared_transfer_targets.swap(std::sync::Arc::default());
        let splits = self.local_split_sessions.swap(std::sync::Arc::default());
        tokio::task::spawn_blocking(move || drop((partitions, prepared, splits)))
            .await
            .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))
    }
}
