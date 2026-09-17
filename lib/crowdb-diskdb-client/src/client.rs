// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskdbClient` — full client library for CROWDB diskdb operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all diskdb
//! instances from the service registry and atomically publishes the complete
//! `disk_group_id -> rpc_endpoint` snapshot. On cache miss or
//! `Unavailable`, lazily refreshes and retries.
//!
//! All RPCs go through the crowdb-rpc flatbuffer transport.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, CommitBlocksRequest, CommitBlocksResponse, CompactZoneRequest,
    CompactZoneResponse, ExecuteRelocationRequest, ExecuteRelocationResponse, FreeBlocksRequest, FreeFailure,
    FreeFailureReason, FreeResponse, GetDiskGroupInfoResponse, GetDiskInfoResponse, GetScanStatusResponse,
    QueryCapacityStatsRequest, QueryCapacityStatsResponse, RebuildZoneBitmapResponse, RecalcDiskUsageRequest,
    MarkBlocksCorruptRequest, MarkBlocksCorruptResponse, RecalcDiskUsageResponse, TriggerScanResponse,
};
use crowdb_protocol::DiskGroupId;

use crate::routing::{DiskdbRoutingState, EndpointRoute};
use crate::rpc_transport::DiskdbRpcTransport;
use crate::{DiskdbClientError, Result};

const REGISTRY_REFRESH_INTERVAL_MS: u64 = 5_000;

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Retry configuration for transient errors.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(50),
        }
    }
}

/// Client for CROWDB diskdb operations via crowdb-rpc.
#[derive(Clone)]
pub struct DiskdbClient {
    svc: ServiceRegistryClient,
    /// Shared endpoint snapshots and incrementally learned disk routes.
    routing: Arc<DiskdbRoutingState>,
    /// crowdb-rpc transport.
    rpc_transport: Arc<DiskdbRpcTransport>,
    retry: RetryConfig,
    /// Last successful (or in-flight) group-0 route refresh. Shared between
    /// clones so one caller refreshes each generation without a hot-path lock.
    last_registry_refresh_ms: Arc<AtomicU64>,
}

impl DiskdbClient {
    pub async fn mark_blocks_corrupt(&self, req: MarkBlocksCorruptRequest) -> Result<MarkBlocksCorruptResponse> {
        let segment = req.segments.first().ok_or_else(|| DiskdbClientError::Rpc("segment required".into()))?;
        let disk_id = segment.disk_id.ok_or_else(|| DiskdbClientError::Rpc("segment.disk_id required".into()))?;
        let dg_id = self.dg_for_disk(disk_id).await?;
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let request = req.clone();
            async move { rpc.mark_blocks_corrupt(&endpoint, &request).await }
        }).await
    }
    #[must_use]
    pub fn new(svc: ServiceRegistryClient, rpc_transport: Arc<DiskdbRpcTransport>) -> Self {
        Self {
            svc,
            routing: Arc::new(DiskdbRoutingState::new()),
            rpc_transport,
            retry: RetryConfig::default(),
            last_registry_refresh_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set a custom retry config.
    #[must_use]
    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Eager warm: read all diskdb instances, populate the endpoint
    /// cache + disk→dg reverse map.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Unreachable` if the service registry read fails.
    pub async fn refresh_endpoints(&self) -> Result<()> {
        let instances = self
            .svc
            .read_all_diskdb_instances()
            .await
            .map_err(|e| DiskdbClientError::Unreachable(format!("read_all_diskdb_instances: {e}")))?;
        let mut observed = HashMap::new();
        for (_id, value) in instances {
            if let Some(extra) = &value.extra {
                if let Some(diskdb) = &extra.diskdb {
                    for &dg_id in &diskdb.owned_dg_ids {
                        observed.insert(dg_id, value.rpc_endpoint.clone());
                    }
                }
            }
        }
        self.routing.replace_endpoints(observed);
        self.last_registry_refresh_ms
            .store(unix_time_ms(), Ordering::Release);
        Ok(())
    }

    /// Return the currently discovered disk-groups in stable order.
    #[must_use]
    pub fn disk_group_ids(&self) -> Vec<DiskGroupId> {
        self.routing.disk_group_ids()
    }

    /// Look up the endpoint for `dg_id`, refreshing on cache miss.
    async fn endpoint_for(&self, dg_id: DiskGroupId) -> Result<EndpointRoute> {
        if let Some(endpoint) = self.routing.endpoint_for(dg_id) {
            self.refresh_routes_if_due().await;
            return Ok(self.routing.endpoint_for(dg_id).unwrap_or(endpoint));
        }
        // Cache miss — refresh and retry.
        self.refresh_endpoints().await?;
        self.routing
            .endpoint_for(dg_id)
            .ok_or_else(|| DiskdbClientError::Unreachable(format!("no diskdb instance owns dg {dg_id}")))
    }

    async fn refresh_routes_if_due(&self) {
        let observed = self.last_registry_refresh_ms.load(Ordering::Acquire);
        let now = unix_time_ms();
        if now.saturating_sub(observed) < REGISTRY_REFRESH_INTERVAL_MS
            || self
                .last_registry_refresh_ms
                .compare_exchange(observed, now, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        if self.refresh_endpoints().await.is_err() {
            // Retain the last complete RCU snapshot and permit a later caller
            // to retry group-0 rather than extending a failed refresh lease.
            self.last_registry_refresh_ms.store(0, Ordering::Release);
        }
    }

    /// Allocate blocks on a disk-group. Retries on transient errors
    /// (`Unavailable`, deadline-exceeded). `ResourceExhausted` (no
    /// space) is returned to the caller (not retryable).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn allocate_blocks(&self, req: AllocateBlocksRequest) -> Result<AllocateResponse> {
        let dg_id = req.disk_group_id;
        let response = self
            .with_rpc_retry(dg_id, |endpoint, rpc| {
                let req = req.clone();
                async move { rpc.allocate_blocks(&endpoint, &req).await }
            })
            .await?;
        for segment in &response.segments {
            if let Some(disk_id) = segment.disk_id {
                self.routing.learn_disk_route(disk_id, dg_id);
            }
        }
        Ok(response)
    }

    /// Free blocks. The request carries `Segment`s (each with
    /// `disk_id`); routes by looking up which diskdb instance owns
    /// each `disk_id`'s disk-group. If segments span multiple
    /// disk-groups, splits the request per-group and issues one
    /// `FreeBlocks` per group. v1: returns the first error on partial
    /// failure.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn free_blocks(&self, req: FreeBlocksRequest) -> Result<FreeResponse> {
        if req.segments.is_empty() {
            return Ok(FreeResponse {
                freed_count: 0,
                failures: Vec::new(),
            });
        }
        // Group segments by disk-group.
        let mut groups: Vec<(DiskGroupId, Vec<_>)> = Vec::new();
        let mut failures = Vec::new();
        for seg in &req.segments {
            let disk_id = seg
                .disk_id
                .ok_or_else(|| DiskdbClientError::Rpc("segment.disk_id required".into()))?;
            let Ok(dg_id) = self.dg_for_disk(disk_id).await else {
                failures.push(FreeFailure {
                    segment: *seg,
                    reason: FreeFailureReason::Unavailable,
                });
                continue;
            };
            if let Some((_, segs)) = groups.iter_mut().find(|(g, _)| *g == dg_id) {
                segs.push(*seg);
            } else {
                groups.push((dg_id, vec![*seg]));
            }
        }
        let mut total_freed = 0u32;
        for (dg_id, segs) in groups {
            let sub_req = FreeBlocksRequest {
                segments: segs.clone(),
            };
            match self
                .with_rpc_retry(dg_id, |endpoint, rpc| {
                    let req = sub_req.clone();
                    async move { rpc.free_blocks(&endpoint, &req).await }
                })
                .await
            {
                Ok(resp) => {
                    total_freed = total_freed.saturating_add(resp.freed_count);
                    failures.extend(resp.failures);
                }
                Err(_) => failures.extend(segs.into_iter().map(|segment| FreeFailure {
                    segment,
                    reason: FreeFailureReason::OutcomeUnknown,
                })),
            }
        }
        Ok(FreeResponse {
            freed_count: total_freed,
            failures,
        })
    }

    /// Commit tentative blocks. Requests spanning disk-groups are split
    /// and routed to each current owner.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures and
    /// `Unreachable` for routing or connection errors.
    pub async fn commit_blocks(&self, req: CommitBlocksRequest) -> Result<CommitBlocksResponse> {
        if req.segments.is_empty() {
            return Ok(CommitBlocksResponse { committed_count: 0 });
        }
        let mut groups: Vec<(DiskGroupId, Vec<_>)> = Vec::new();
        for seg in &req.segments {
            let disk_id = seg
                .disk_id
                .ok_or_else(|| DiskdbClientError::Rpc("segment.disk_id required".into()))?;
            let dg_id = self.dg_for_disk(disk_id).await?;
            if let Some((_, segs)) = groups.iter_mut().find(|(group_id, _)| *group_id == dg_id) {
                segs.push(*seg);
            } else {
                groups.push((dg_id, vec![*seg]));
            }
        }
        let mut committed_count = 0u32;
        for (dg_id, segments) in groups {
            let sub_req = CommitBlocksRequest { segments };
            let response = self
                .with_rpc_retry(dg_id, |endpoint, rpc| {
                    let request = sub_req.clone();
                    async move { rpc.commit_blocks(&endpoint, &request).await }
                })
                .await?;
            committed_count += response.committed_count;
        }
        Ok(CommitBlocksResponse { committed_count })
    }

    /// Ask the `DiskDB` owning the already-reserved target to durably adopt and
    /// execute a cross-domain relocation.
    ///
    /// # Errors
    /// Returns routing, transport, or server-side validation failures.
    pub async fn execute_relocation(
        &self,
        req: ExecuteRelocationRequest,
    ) -> Result<ExecuteRelocationResponse> {
        let target = req
            .target
            .and_then(|segment| segment.disk_id)
            .ok_or_else(|| DiskdbClientError::Rpc("relocation target disk is required".into()))?;
        let dg_id = self.dg_for_disk(target).await?;
        if dg_id != req.target_disk_group_id {
            return Err(DiskdbClientError::NotOwner(
                "relocation target disk-group does not match routing".into(),
            ));
        }
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let request = req.clone();
            async move { rpc.execute_relocation(&endpoint, &request).await }
        })
        .await
    }

    /// Query capacity stats at the disk-group level (all owned groups
    /// if `dg_id == 0`).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_capacity_stats(
        &self,
        req: QueryCapacityStatsRequest,
    ) -> Result<QueryCapacityStatsResponse> {
        let dg_id = if req.disk_group_id != 0 {
            req.disk_group_id
        } else {
            // Use the first cached endpoint for an all-owned query.
            self.first_cached_dg()?
        };
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let req = req.clone();
            async move { rpc.query_capacity_stats(&endpoint, &req).await }
        })
        .await
    }

    /// Query one disk-group's capacity stats.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_disk_group(&self, dg_id: u64) -> Result<QueryCapacityStatsResponse> {
        self.query_capacity_stats(QueryCapacityStatsRequest {
            disk_group_id: dg_id,
            disk_id: None,
            zone_index: None,
        })
        .await
    }

    /// Query one disk's capacity stats (brief per-zone entries).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_disk(&self, dg_id: u64, disk_id: DiskId) -> Result<QueryCapacityStatsResponse> {
        self.query_capacity_stats(QueryCapacityStatsRequest {
            disk_group_id: dg_id,
            disk_id: Some(disk_id),
            zone_index: None,
        })
        .await
    }

    /// Query one zone's capacity stats (full `usage_bitmap`).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_zone(
        &self,
        dg_id: u64,
        disk_id: DiskId,
        zone_index: u32,
    ) -> Result<QueryCapacityStatsResponse> {
        self.query_capacity_stats(QueryCapacityStatsRequest {
            disk_group_id: dg_id,
            disk_id: Some(disk_id),
            zone_index: Some(zone_index),
        })
        .await
    }

    /// Get disk-group info.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn get_disk_group_info(&self, dg_id: u64) -> Result<GetDiskGroupInfoResponse> {
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.get_disk_group_info(&endpoint, dg_id).await
        })
        .await
    }

    /// Get disk info. `rack_id`/`node_id` are passed as 0 (the
    /// service handler resolves the disk by `disk_group_id` +
    /// `disk_id` only).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn get_disk_info(&self, dg_id: u64, disk_id: DiskId) -> Result<GetDiskInfoResponse> {
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.get_disk_info(&endpoint, dg_id, disk_id).await
        })
        .await
    }

    /// Recalc disk usage (admin RPC).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn recalc_disk_usage(&self, req: RecalcDiskUsageRequest) -> Result<RecalcDiskUsageResponse> {
        // Route to any cached endpoint (recalc covers all owned groups
        // when disk_group_id is None).
        let dg_id = if let Some(id) = req.disk_group_id {
            id
        } else {
            self.first_cached_dg()?
        };
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let req = req.clone();
            async move { rpc.recalc_disk_usage(&endpoint, &req).await }
        })
        .await
    }

    /// Compact one or more zones on a disk (admin RPC). Empty
    /// `zone_indices` = all zones on the disk. Routes by looking up
    /// which diskdb instance owns the disk's disk-group.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn compact_zone(&self, req: CompactZoneRequest) -> Result<CompactZoneResponse> {
        let disk_id = req
            .disk_id
            .ok_or_else(|| DiskdbClientError::Rpc("disk_id required".into()))?;
        let dg_id = self.dg_for_disk(disk_id).await?;
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let req = req.clone();
            async move { rpc.compact_zone(&endpoint, &req).await }
        })
        .await
    }

    /// Trigger a scan on all owned groups (or one group if `dg_id` is
    /// set). Returns the last `ScanSummary` + `scan_in_progress`. If a
    /// scan is already running the server returns `scan_in_progress:
    /// true` (no error, no stacking). Admin/debug call; transient
    /// `Unavailable` is retried per `RetryConfig`.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn trigger_scan(&self, dg_id: Option<DiskGroupId>) -> Result<TriggerScanResponse> {
        let dg_id = match dg_id {
            Some(id) => id,
            None => self.first_cached_dg()?,
        };
        self.with_rpc_retry(
            dg_id,
            |endpoint, rpc| async move { rpc.trigger_scan(&endpoint).await },
        )
        .await
    }

    /// Get the last scan summary + `has_run` flag. `has_run` is false
    /// if no scan has completed yet (the summary is empty in that
    /// case). Admin/debug call; transient `Unavailable` is retried.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn get_scan_status(&self, dg_id: Option<DiskGroupId>) -> Result<GetScanStatusResponse> {
        let dg_id = match dg_id {
            Some(id) => id,
            None => self.first_cached_dg()?,
        };
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.get_scan_status(&endpoint).await
        })
        .await
    }

    /// Rebuild one zone's bitmap on a disk (admin/debug). Routes via
    /// `dg_for_disk` so the call lands on the owning diskdb instance.
    /// `zone_index = u32::MAX` means all zones on the disk.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors (including unknown disk).
    pub async fn rebuild_zone_bitmap(
        &self,
        disk_id: DiskId,
        zone_index: u32,
    ) -> Result<RebuildZoneBitmapResponse> {
        let dg_id = self.dg_for_disk(disk_id).await?;
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.rebuild_zone_bitmap(&endpoint, disk_id, zone_index).await
        })
        .await
    }

    /// Return the first cached disk-group id, or `Unreachable` if the
    /// cache is empty.
    fn first_cached_dg(&self) -> Result<DiskGroupId> {
        self.routing.first_disk_group().ok_or_else(|| {
            DiskdbClientError::Unreachable("no cached endpoints; call refresh_endpoints".into())
        })
    }

    /// Look up which disk-group owns a `disk_id`. Refreshes the
    /// disk→dg reverse map on miss.
    async fn dg_for_disk(&self, disk_id: DiskId) -> Result<DiskGroupId> {
        if let Some(dg_id) = self.routing.disk_group_for(disk_id) {
            return Ok(dg_id);
        }
        // Refresh the reverse map from the hardware hierarchy.
        self.refresh_endpoints().await?;
        // The reverse map is populated during refresh_endpoints from
        // the service registry's DiskdbExtra.owned_dg_ids — but that
        // only gives dg_id→endpoint, not disk_id→dg_id. For v1, we
        // try each cached endpoint's get_disk_group_info to find the
        // disk. This is O(groups) on first miss; subsequent calls hit
        // the cache.
        for (dg_id, endpoint) in self.routing.endpoint_entries() {
            let group_result = match self.rpc_transport.get_disk_group_info(&endpoint, dg_id).await {
                Ok(resp) => resp.group,
                Err(e) => {
                    warn!(disk_id = ?disk_id, dg_id, error = %e, "dg_for_disk: rpc get_disk_group_info failed");
                    continue;
                }
            };
            if let Some(group) = group_result {
                if group.disk_ids.contains(&disk_id) {
                    self.routing.learn_disk_route(disk_id, dg_id);
                    return Ok(dg_id);
                }
            }
        }
        Err(DiskdbClientError::Unreachable(format!(
            "no diskdb instance owns disk {disk_id:?}"
        )))
    }

    /// crowdb-rpc retry wrapper: calls `op` with the endpoint + transport,
    /// retries on transient errors (`Unreachable`).
    async fn with_rpc_retry<F, Fut, T>(&self, dg_id: DiskGroupId, op: F) -> Result<T>
    where
        F: Fn(String, Arc<DiskdbRpcTransport>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let rpc = Arc::clone(&self.rpc_transport);
        let mut backoff = self.retry.initial_backoff;
        let mut last_err = None;
        for attempt in 0..=self.retry.max_retries {
            let route = match self.endpoint_for(dg_id).await {
                Ok(e) => e,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                    continue;
                }
            };
            match op(route.endpoint().to_string(), Arc::clone(&rpc)).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if matches!(
                        e,
                        DiskdbClientError::Unreachable(_) | DiskdbClientError::NotOwner(_)
                    ) {
                        warn!(dg_id, attempt, error = %e, "rpc transient error, retrying");
                        last_err = Some(e);
                        self.routing.evict_endpoint(dg_id, &route);
                        let _ = self.refresh_endpoints().await;
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| DiskdbClientError::Unreachable("max retries exhausted".into())))
    }
}

#[cfg(feature = "test-util")]
impl DiskdbClient {
    /// Atomically replace discovered endpoints in integration tests.
    pub fn replace_endpoints_for_tests(&self, endpoints: Vec<(DiskGroupId, String)>) {
        self.routing.replace_endpoints(endpoints.into_iter().collect());
    }

    /// Return one complete endpoint generation in stable order.
    #[must_use]
    pub fn endpoint_snapshot_for_tests(&self) -> Vec<(DiskGroupId, String)> {
        self.routing.endpoint_entries()
    }

    /// Record an incrementally learned disk route in integration tests.
    pub fn learn_disk_route_for_tests(&self, disk_id: DiskId, disk_group_id: DiskGroupId) {
        self.routing.learn_disk_route(disk_id, disk_group_id);
    }

    /// Resolve an incrementally learned disk route in integration tests.
    #[must_use]
    pub fn disk_group_for_tests(&self, disk_id: DiskId) -> Option<DiskGroupId> {
        self.routing.disk_group_for(disk_id)
    }
}

/// Normalize a service-registry endpoint: rewrite `0.0.0.0`
/// to `127.0.0.1` so the connection goes to a loopback address.
#[must_use]
pub fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}
