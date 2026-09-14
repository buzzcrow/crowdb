// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free `DiskIO` client status.

/// A point-in-time semantic client status snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskioStatus {
    pub route_generation: u64,
    pub route_age_ms: u64,
    pub disks: usize,
    pub nodes: usize,
    pub endpoints: usize,
    pub normal_connections: usize,
    pub normal_healthy_connections: usize,
    pub priority_connections: usize,
    pub priority_healthy_connections: usize,
    pub inflight: u64,
    pub retries: u64,
    pub admission_rejections: u64,
    pub ambiguous_writes: u64,
    pub connect_attempts: u64,
    pub reconnect_attempts: u64,
    pub read_operations: u64,
    pub write_operations: u64,
    pub fsync_operations: u64,
    pub read_average_us: u64,
    pub write_average_us: u64,
    pub fsync_average_us: u64,
}
