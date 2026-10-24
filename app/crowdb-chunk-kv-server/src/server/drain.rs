// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ChunkKvError, ChunkKvService};

impl ChunkKvService {
    /// Drain admitted mutations and release each hosted WAL writer for a new boot.
    /// Background assignment changes must be stopped before calling this method.
    /// # Errors
    /// Leaves uncertain or fenced writers subject to their normal lease exclusion.
    pub async fn drain_writers(&self) -> Result<(), ChunkKvError> {
        self.begin_drain();
        let partitions = self.partitions.load_full();
        for partition in partitions.values() {
            partition
                .suspend_for_transfer(partition.snapshot().ownership_epoch)
                .await?;
        }
        Ok(())
    }
}
