// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Partition admission, batching, and retention bounds.

use crate::{ChunkKvError, Result};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PartitionConfig {
    pub queue_requests: usize,
    pub queue_bytes: u64,
    pub batch_requests: usize,
    pub batch_bytes: u64,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub retained_results: usize,
    pub metadata_reclaim_records_per_pass: usize,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            queue_requests: 1_024,
            queue_bytes: 64 * 1024 * 1024,
            batch_requests: 64,
            batch_bytes: 4 * 1024 * 1024,
            max_key_bytes: 64 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            retained_results: 65_536,
            metadata_reclaim_records_per_pass: 128,
        }
    }
}

impl PartitionConfig {
    /// Validate partition admission and maintenance bounds.
    ///
    /// # Errors
    /// Returns an invalid request error when any capacity is zero.
    pub fn validate(&self) -> Result<()> {
        if self.queue_requests == 0
            || self.queue_bytes == 0
            || self.batch_requests == 0
            || self.batch_bytes == 0
            || self.max_key_bytes == 0
            || self.max_value_bytes == 0
            || self.retained_results == 0
            || self.metadata_reclaim_records_per_pass == 0
        {
            return Err(ChunkKvError::InvalidRequest(
                "partition bounds must be nonzero".into(),
            ));
        }
        Ok(())
    }
}
