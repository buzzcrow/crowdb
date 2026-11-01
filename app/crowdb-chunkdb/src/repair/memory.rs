// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Memory admission for one full-segment repair attempt.

use super::{RepairMetrics, RepairRunError, RepairStripTaskHandler};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

pub(super) struct RepairMemoryGuard {
    metrics: Arc<RepairMetrics>,
    bytes: usize,
}

impl Drop for RepairMemoryGuard {
    fn drop(&mut self) {
        self.metrics.release_memory(self.bytes);
    }
}

impl RepairStripTaskHandler {
    pub(super) async fn reserve_memory(
        &self,
        shard_bytes: u64,
        segment_count: usize,
    ) -> Result<(OwnedSemaphorePermit, RepairMemoryGuard), RepairRunError> {
        let required = shard_bytes
            .checked_mul(u64::try_from(segment_count.saturating_add(1)).unwrap_or(u64::MAX))
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| RepairRunError::Permanent("repair memory calculation overflows".into()))?;
        if required > self.memory_limit {
            return Err(RepairRunError::Retry(format!(
                "repair requires {required} bytes but the limit is {}",
                self.memory_limit,
            )));
        }
        let permits = u32::try_from(required)
            .map_err(|_| RepairRunError::Permanent("repair memory exceeds semaphore".into()))?;
        let reservation = self
            .memory
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| RepairRunError::Retry("repair memory budget closed".into()))?;
        self.metrics.reserve_memory(required);
        Ok((
            reservation,
            RepairMemoryGuard {
                metrics: Arc::clone(&self.metrics),
                bytes: required,
            },
        ))
    }
}
