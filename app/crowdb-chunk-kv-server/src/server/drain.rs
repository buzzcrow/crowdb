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
        let mut first_error = None;
        for partition in partitions.values() {
            if let Err(error) = partition
                .suspend_for_shutdown(partition.snapshot().ownership_epoch)
                .await
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
