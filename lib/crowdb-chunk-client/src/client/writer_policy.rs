// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use super::{ChunkIoClient, FailedDiskList, SmallWriteMetrics, SmallWritePolicy, SmallWritePool};

impl ChunkIoClient {
    /// The immutable policy owned by this concrete writer pool.
    #[must_use]
    pub fn small_write_policy(&self) -> &Arc<SmallWritePolicy> {
        &self.small_pool.policy
    }

    /// Shares discovered transports, routes and read credits while constructing
    /// a separate small-writer pool for an explicitly selected chunk purpose.
    /// # Errors
    /// Rejects invalid writer policy before any background manager starts.
    pub fn with_writer_policy(&self, policy: SmallWritePolicy) -> crate::Result<Self> {
        let failed_disks = Arc::new(FailedDiskList::new(policy.failed_disk_ttl));
        let small_pool = SmallWritePool::new(
            Arc::clone(&self.allocator),
            Arc::clone(&self.disk_writer),
            policy,
            Arc::new(SmallWriteMetrics::default()),
            Arc::clone(&failed_disks),
        )?;
        Ok(Self {
            allocator: Arc::clone(&self.allocator),
            disk_writer: Arc::clone(&self.disk_writer),
            topology: self.topology.clone(),
            metrics: self.metrics.clone(),
            small_pool,
            reader: self.reader.clone(),
            failed_disks,
            large_write_repair: Arc::clone(&self.large_write_repair),
            large_write_buffer: Arc::clone(&self.large_write_buffer),
        })
    }
}
