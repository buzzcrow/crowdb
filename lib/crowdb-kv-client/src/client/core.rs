// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! [`CrowdbKvClient`]: the C1-C3 client library (—
//! topology cache, retry/idempotency, and `ReadMode` routing on top of
//! `crowdb_kv`'s generated `KvService` client.

#![allow(clippy::cast_possible_truncation)]

mod conditional;
mod operations;
mod owned;
mod scans;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crowdb_common::RequestIdGen;
use crowdb_kv::rpc::ReadMode;

use super::topology::{EndpointStats, TopologyCache};
use crate::config::{ClientConfig, ReadEndpointPolicy, RetryConfig};
use crate::error::{Error, Result};
use crate::metrics::ClientMetrics;

/// Ordered scan traversal direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ScanDirection {
    #[default]
    Forward,
    Reverse,
}

/// Outcome of a successful `put`/`delete`/`batch_write`.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub revision: u64,
    pub request_id: u64,
}

/// Outcome of a successful `get`. `value` is zero-copy `Bytes` from the
/// flatbuffer response frame, not a `Vec<u8>` copy.
#[derive(Debug, Clone)]
pub enum GetOutcome {
    Found { value: Bytes, revision: u64 },
    NotFound,
}

/// Outcome of a successful `scan`. Items are zero-copy `Bytes` from the
/// flatbuffer response frame, not per-entry `Vec<u8>` copies.
#[derive(Debug, Clone)]
pub struct ScanOutcome {
    pub items: Vec<(Bytes, Bytes)>,
    /// Per-item record revisions in the same order as `items`.
    pub commit_slots: Vec<u64>,
    pub truncated: bool,
    pub timed_out: bool,
    /// The applied frontier when the scan ran (page 1's `read_slot`).
    /// Used by `get_applied_slot` to read the data group's frontier via
    /// a linearizable scan.
    pub read_slot: u64,
    pub scan_cutoff: u64,
}

/// One op from a `journal_scan` — a Put or Delete at a specific commit
/// slot. `value` is empty for Delete. Zero-copy `Bytes` from the flatbuffer
/// response frame.
#[derive(Debug, Clone)]
pub struct JournalOp {
    pub key: Bytes,
    pub value: Bytes,
    pub is_delete: bool,
    pub slot: u64,
}

/// Outcome of a successful `journal_scan`. Ops are in slot order
/// (within a slot, in batch order). `truncated` means the caller's
/// `limit` was reached and more ops exist beyond it.
#[derive(Debug, Clone)]
pub struct JournalScanOutcome {
    pub ops: Vec<JournalOp>,
    pub truncated: bool,
    pub read_slot: u64,
}

/// One item of a `batch_write` call.
#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

/// RAII guard that decrements the endpoint's in-flight count on drop.
/// Created before the crowdb-rpc send; dropped at the end of the retry-loop
/// iteration (covers all exit paths: success, error, redirect, `?`).
/// Holds an `Arc<EndpointStats>` so it can live across `.await` points.
pub(crate) struct InFlightGuard {
    stats: Arc<EndpointStats>,
}

impl InFlightGuard {
    fn new(stats: Arc<EndpointStats>) -> Self {
        stats.increment_in_flight();
        Self { stats }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.stats.decrement_in_flight();
    }
}

/// Standalone `CrowDB` client: topology discovery over the HTTP management
/// API, per-group leader cache, retry loop reusing `(client_id, seq)` across
/// retries of one logical write, and `ReadMode` routing including
/// `MinSlot` client-side slot tracking.
pub struct CrowdbKvClient {
    pub(crate) topology: TopologyCache,
    pub(crate) retry: RetryConfig,
    client_id: u64,
    next_seq: AtomicU64,
    request_ids: RequestIdGen,
    pub(crate) metrics: Arc<ClientMetrics>,
    /// `MinSlot` read-endpoint selection policy. `Leader` (default)
    /// preserves the pre-R26 behavior; `AnyReplica` distributes `MinSlot`
    /// reads round-robin across the topology cache's replica list.
    /// Linearizable reads always target the leader regardless of this.
    read_endpoint_policy: ReadEndpointPolicy,
    /// Optional crowdb-rpc transport (R117). When set via
    /// `with_rpc_transport`, the KV methods (`put`/`get`/`delete`/
    /// `batch_write`/`scan`/`scan_count`/`journal_scan`) send via
    /// via crowdb-rpc. The retry/topology/`NotLeaderHint`
    /// logic is unchanged — only the wire send changes.
    rpc_transport: Option<Arc<crate::KvRpcTransport>>,
}

impl CrowdbKvClient {
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self::build(config, None)
    }

    /// Create a client using an existing shared RPC transport.
    #[must_use]
    pub fn new_with_rpc_transport(config: ClientConfig, transport: Arc<crate::KvRpcTransport>) -> Self {
        Self::build(config, Some(transport))
    }

    fn build(config: ClientConfig, transport: Option<Arc<crate::KvRpcTransport>>) -> Self {
        // A standalone client is normal at process startup. Keep the
        // distinction available for diagnostics without warning on each
        // test listener restart or each independently deployed process.
        if transport.is_none() {
            tracing::debug!(
                seed_count = config.mgmt_seeds.len(),
                "CrowdbKvClient: new standalone instance created"
            );
        } else {
            tracing::info!(
                seed_count = config.mgmt_seeds.len(),
                "CrowdbKvClient: new shared instance created"
            );
        }
        Self {
            topology: TopologyCache::new(config.mgmt_seeds, config.topology_min_refresh_interval),
            retry: config.retry,
            client_id: new_client_id(),
            next_seq: AtomicU64::new(1),
            request_ids: RequestIdGen::new(),
            metrics: Arc::new(ClientMetrics::default()),
            read_endpoint_policy: config.read_endpoint_policy,
            rpc_transport: Some(transport.unwrap_or_else(|| {
                std::sync::Arc::new(crate::KvRpcTransport::with_pool_size(
                    config.pool_size_per_endpoint,
                    config.enable_nagle,
                    config.quickack,
                    config.event_write,
                    config.send_queue_capacity,
                    config.rpc_workers,
                ))
            })),
        }
    }

    /// Switch the client to use crowdb-rpc (R117) for KV operations.
    /// When set, `put`/`get`/`delete`/`batch_write`/`scan`/
    /// `scan_count`/`journal_scan` send via the transport instead of
    /// via crowdb-rpc. The retry/topology/`NotLeaderHint` logic is unchanged.
    #[must_use]
    pub fn with_rpc_transport(mut self, transport: Arc<crate::KvRpcTransport>) -> Self {
        self.rpc_transport = Some(transport);
        self
    }

    /// The crowdb-rpc transport, if set via `with_rpc_transport`.
    /// Used by `WatchNotifyClient` to select the crowdb-rpc push path.
    #[must_use]
    pub(crate) fn rpc_transport(&self) -> Option<&Arc<crate::KvRpcTransport>> {
        self.rpc_transport.as_ref()
    }

    /// Sample client-side crowdb-rpc transport stats (syscall counts,
    /// frame aggregation, submit→writev queue wait). Returns `None`
    /// if no RPC transport is configured.
    #[must_use]
    pub fn transport_stats(&self) -> Option<crowdb_rpc_ffi::CrowdbRpcTransportStats> {
        self.rpc_transport.as_ref().map(|t| t.server().transport_stats())
    }

    /// Dump all pending RPC request IDs + deadlines to the C++ log.
    /// For diagnostics: call when the bench stops to see which requests
    /// are still in-flight.
    pub fn dump_pending_requests(&self) {
        if let Some(t) = &self.rpc_transport {
            t.rpc().dump_pending();
        }
    }

    /// The next RPC request ID that will be allocated (for diagnostics).
    #[must_use]
    pub fn next_req_id(&self) -> u64 {
        self.rpc_transport.as_ref().map_or(0, |t| t.next_req_id())
    }

    /// This client session's opaque `client_id`.
    #[must_use]
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Snapshot the client's internal metrics counters (per-op counts,
    /// leader-related retry events, topology refreshes). Values are
    /// cumulative since client creation.
    #[must_use]
    pub fn metrics(&self) -> crate::metrics::ClientMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Drain per-op-kind window latency histograms. Returns one
    /// `Histogram<u64>` per op kind. The caller is expected to
    /// accumulate these into cumulative histograms if desired.
    #[must_use]
    pub fn drain_window(&self) -> crate::metrics::WindowLatencySnapshot {
        self.metrics.drain_window()
    }

    /// Flush per-op-kind window latency histograms to `writer` in the
    /// same column-aligned format as the server `rust` log.
    /// Takes a pre-drained `WindowLatencySnapshot` so the caller can
    /// also use it for cumulative accumulation.
    pub fn flush_latencies<W: std::fmt::Write>(
        &self,
        writer: &mut W,
        snap: &crate::metrics::WindowLatencySnapshot,
        window_secs: f64,
    ) {
        self.metrics.flush_latencies(writer, snap, window_secs);
    }

    /// Force a topology refresh. Not required for normal operation (the
    /// client refreshes on cache miss and `NotLeaderHint` automatically);
    /// exposed for callers that want to warm the cache eagerly at startup.
    ///
    /// # Errors
    /// `Error::Topology` if every seed is unreachable.
    pub async fn refresh_topology(&self) -> Result<()> {
        self.topology.refresh().await
    }

    /// Replace the HTTP management-API seed list used for topology
    /// discovery, without losing already-cached leader endpoints. For
    /// long-lived embedders (e.g. `crowdb-console`) whose set of known
    /// nodes can grow at runtime.
    pub fn set_mgmt_seeds(&self, seeds: Vec<String>) {
        self.topology.set_seeds(seeds);
    }

    /// Directly seed the topology cache with a known leader endpoint for a
    /// group, bypassing `/topology` discovery entirely. For callers that
    /// already resolved an endpoint through some other discovery path
    /// (e.g. `crowdb-console`'s own management API) and just want
    /// `CrowdbKvClient`'s retry/pool machinery on top of it.
    #[allow(clippy::needless_pass_by_value)]
    pub fn seed_leader(&self, store_id: u64, group_id: u64, endpoint: String) {
        self.topology.set_leader(store_id, group_id, &endpoint);
    }

    /// The system KV group's store id (always 0). Group 0 of store 0
    /// is the fixed directory holding hardware/service-registry/
    /// KV-cluster-topology records.
    pub const SYSTEM_STORE: u64 = 0;
    /// The system KV group's group id (always 0).
    pub const SYSTEM_GROUP: u64 = 0;

    /// The system KV group `(store_id, group_id)` — group 0 of store
    /// 0, the fixed directory holding hardware/service-registry/
    /// KV-cluster-topology records. Group-0 service classes
    /// (`HardwareClient`, `ServiceRegistryClient`,
    /// `KVClusterMetaClient`) target this group; callers can use this
    /// instead of hardcoding `(0, 0)`.
    #[must_use]
    pub fn system_group(&self) -> (u64, u64) {
        (Self::SYSTEM_STORE, Self::SYSTEM_GROUP)
    }

    /// Resolve the current leader endpoint for `(store_id, group_id)`,
    /// retrying an "unknown leader" outcome ("100ms-then-retry") rather
    /// than failing on the first miss. A single failed/empty `/topology`
    /// fetch is not conclusive: the group may simply be mid-election (a
    /// real, common case right after a node restart) or the seed just
    /// queried may be transiently down while others are fine. Bounded by
    /// the same `RetryConfig::max_retries` budget used for post-request
    /// retries.
    #[tracing::instrument(level = "debug", skip_all, fields(s = store_id, g = group_id))]
    async fn resolve_leader(&self, store_id: u64, group_id: u64) -> Result<String> {
        if let Some(ep) = self.topology.leader(store_id, group_id) {
            return Ok(ep);
        }
        let mut attempts = 0u32;
        loop {
            // A fetch error and a fetch that succeeded but shows no leader
            // yet (mid-election) are both just "leader unknown right now"
            // from the caller's perspective -- collapse them into the same
            // retry path instead of surfacing the transport error early.
            // Exception: NoSeeds is a configuration error, not a transient
            // failure — fail immediately so the caller sees a clear error
            // instead of retrying for seconds.
            self.metrics.record_leader_query();
            self.metrics.record_topology_refresh();
            if let Err(Error::NoSeeds) = self.topology.refresh().await {
                tracing::warn!(
                    "resolve_leader: no mgmt seeds configured — call set_mgmt_seeds before KV ops"
                );
                return Err(Error::NoSeeds);
            }
            if let Some(ep) = self.topology.leader(store_id, group_id) {
                return Ok(ep);
            }
            attempts += 1;
            if attempts > self.retry.max_retries {
                self.metrics.record_no_leader();
                return Err(Error::NoLeader { store_id, group_id });
            }
            self.metrics.record_unknown_leader_wait();
            tokio::time::sleep(self.retry.unknown_leader_wait).await;
        }
    }

    /// Pick the first endpoint for a read. Linearizable reads always
    /// resolve to the leader (correctness: only the leader can prove a
    /// linearizable read is fresh). `MinSlot` reads under the `Leader`
    /// policy also resolve to the leader (backward-compatible default).
    /// `MinSlot` reads under a distributed policy (`AnyReplica`,
    /// `LeastConnections`, `Latency`) pick from the topology cache's
    /// replica list; if no replica list is known (cache miss) the client
    /// refreshes `/topology` once and retries, falling back to the
    /// leader if still unknown — a single-replica group or a stale
    /// `/topology` never blocks reads.
    pub(crate) async fn resolve_read_endpoint(
        &self,
        store_id: u64,
        group_id: u64,
        read_mode: ReadMode,
    ) -> Result<String> {
        if read_mode == ReadMode::Linearizable || self.read_endpoint_policy == ReadEndpointPolicy::Leader {
            return self.resolve_leader(store_id, group_id).await;
        }
        // `MinSlot` + distributed policy: pick from the replica list.
        if self.topology.replicas(store_id, group_id).is_none() {
            self.metrics.record_topology_refresh();
            let _ = self.topology.refresh().await;
        }
        if let Some(endpoint) = self
            .topology
            .select_replica(store_id, group_id, self.read_endpoint_policy)
        {
            self.metrics.record_read_endpoint_distributed();
            Ok(endpoint)
        } else {
            self.resolve_leader(store_id, group_id).await
        }
    }

    /// Get or create `EndpointStats` for `endpoint` and return an
    /// `InFlightGuard` that decrements the in-flight count on drop.
    /// Used in the get/scan retry loops to track per-endpoint load for
    /// `LeastConnections` selection.
    pub(crate) fn incr_in_flight(&self, store_id: u64, group_id: u64, endpoint: &str) -> InFlightGuard {
        InFlightGuard::new(self.topology.endpoint_stats(store_id, group_id, endpoint))
    }

    /// Record the RTT for `endpoint` into its EWMA. Called on every
    /// `Ok` response (success, not-found, `NotLeader` redirect); not
    /// called on transport errors (a timeout doesn't reflect the
    /// endpoint's serving latency). Used by `Latency` selection.
    fn record_endpoint_rtt(&self, store_id: u64, group_id: u64, endpoint: &str, rtt_us: u64) {
        self.topology
            .record_endpoint_rtt(store_id, group_id, endpoint, rtt_us);
    }

    fn record_write(&self, store_id: u64, group_id: u64, revision: u64) {
        self.topology.record_write(store_id, group_id, revision);
    }

    /// Cached `min_slot` for `MinSlot` reads against this group:
    /// the highest paxos slot this client has observed from its own writes,
    /// or `0` if it has never written to this group.
    #[must_use]
    pub fn read_your_writes_slot(&self, store_id: u64, group_id: u64) -> u64 {
        self.topology.write_slot_highwater(store_id, group_id)
    }

    /// `MinSlot` auto-attaches this client's own last-write watermark
    /// for the group unless the caller already supplied a `min_slot`.
    pub(crate) fn resolve_min_slot(
        &self,
        store_id: u64,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: Option<u64>,
    ) -> u64 {
        if let Some(slot) = min_slot {
            return slot;
        }
        if read_mode == ReadMode::MinSlot {
            return self.read_your_writes_slot(store_id, group_id);
        }
        0
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn process_time_nonce() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

/// A `client_id` unique enough for one client session ("opaque, assigned
/// once per client session"). Derived from the
/// process start time in nanoseconds; not a cryptographic identifier.
#[must_use]
pub fn new_client_id() -> u64 {
    process_time_nonce()
}
