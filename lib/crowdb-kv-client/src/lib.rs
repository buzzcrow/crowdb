// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Standalone client library for `CrowDB`.
//!
//! Wraps `crowdb_kv`'s generated `KvService` crowdb-rpc client with:
//! - **Topology cache** (`(store_id, group_id) -> leader_endpoint`) sourced
//!   from `crowdb-kv-server`'s HTTP management API `/topology` (no crowdb-rpc
//!   `DescribeCluster`).
//! - **Retry policy** on `NotLeaderHint` / timeout / other errors, reusing
//!   the same `(client_id, seq)` across retries of one logical write so the
//!   server's dedup cache can do its job.
//! - **`ReadMode` routing**, including client-side `MinSlot` slot
//!   tracking (a bounded per-group high-watermark, not per-key).
//! - A per-endpoint connection pool (crowdb-rpc).
//!
//! `crowdb-console` is expected to depend on this crate rather than rolling
//! its own crowdb-rpc client.

mod binding;
mod client;
mod config;
mod error;
mod hardware;
mod metrics;
mod service;
mod transport;

// FFI module — only compiled with the `ffi` feature. Produces C ABI
// exports for HardwareClient/ServiceRegistryClient (used by crowdb-diskio).
#[cfg(feature = "ffi")]
pub mod ffi;

pub use binding::{
    compute_sub_range_assignment, BindingMonitor, BindingStrategy, ChunkdbRangeBinding, ChunkdbRangeStrategy,
    MonitorTickResult, RangeBindingClient, RangeRouteError, RouteWithFallback, DEFAULT_SUB_RANGE_COUNT,
};
pub use client::{
    new_client_id, BatchOp, CrowdbKvClient, GetOutcome, JournalOp, JournalScanOutcome, ScanOutcome,
    WriteOutcome,
};
pub use config::{ClientConfig, ReadEndpointPolicy, RetryConfig};
pub use error::{Error, Result};
pub use hardware::{
    ClusterUsage, CrowdbSysmdClient, DiskCapacityEntry, DiskGroupCapacityEntry, DiskRecord,
    HardwareCapacitySummary, HardwareClient, NodeCapacityEntry, NodeUsage, RackCapacityEntry, RackUsage,
    SpaceUsageClient,
};
pub use metrics::{ClientMetrics, ClientMetricsSnapshot, LeaderChangeEpisode, WindowLatencySnapshot};
pub use service::{
    ServiceDiscoveryClient, ServiceRegistryClient, WatchNotify, WatchNotifyClient, WatchSubscription,
};
pub use transport::{KVClusterAdmin, KVClusterMetaClient, KvRpcTransport};

/// Re-exported so callers don't need a direct `crowdb_kv` dependency just to
/// pick a read mode or use snapshot DTOs.
pub use crowdb_kv::rpc::ReadMode;
pub use crowdb_kv::rpc::{
    CreateSnapshotResponse, ReleaseSnapshotResponse, SnapshotInfo, SnapshotScanResponse,
};
