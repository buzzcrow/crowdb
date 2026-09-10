// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crate::{ClientError, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    pub operation_timeout: Duration,
    pub retry_backoff: Duration,
    pub max_attempts: u32,
    pub max_route_refreshes: u32,
    pub max_owner_connections: usize,
    pub max_inflight_partition_groups: usize,
    pub max_batch_items: usize,
    pub max_response_bytes: usize,
    pub max_buffered_scan_pages: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(5),
            retry_backoff: Duration::from_millis(10),
            max_attempts: 8,
            max_route_refreshes: 3,
            max_owner_connections: 64,
            max_inflight_partition_groups: 8,
            max_batch_items: 4_096,
            max_response_bytes: 16 * 1024 * 1024,
            max_buffered_scan_pages: 4,
        }
    }
}

impl ClientConfig {
    /// Validates every retry and memory bound.
    ///
    /// # Errors
    ///
    /// Returns an error when a duration or capacity is zero.
    pub fn validate(&self) -> Result<()> {
        if self.operation_timeout.is_zero()
            || self.retry_backoff.is_zero()
            || self.max_attempts == 0
            || self.max_route_refreshes == 0
            || self.max_owner_connections == 0
            || self.max_inflight_partition_groups == 0
            || self.max_batch_items == 0
            || self.max_response_bytes == 0
            || self.max_buffered_scan_pages == 0
        {
            return Err(ClientError::InvalidRequest(
                "all retry and resource bounds must be nonzero".into(),
            ));
        }
        Ok(())
    }
}
