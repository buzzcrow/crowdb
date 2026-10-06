// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkdbClient` — client library for CROWDB chunkdb operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all chunkdb
//! instances from the service registry, atomically publishes an endpoint cache
//! (`instance_id -> rpc_endpoint`). On cache miss, lazily refreshes.
//! Retry: exponential backoff on transient errors, up to `max_retries`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::collections::HashMap;

use arc_swap::ArcSwap;

use crowdb_kv_client::{RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AdvanceChunkWriteResponse, AllocateChunkRequest, AllocateChunkResponse,
    AllocateReplacementSegmentRequest, AllocateReplacementSegmentResponse, AppendChunkRequest,
    AppendChunkResponse, CompleteMirrorToEcConversionRequest, CompleteMirrorToEcConversionResponse,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse,
    DiscardReplacementSegmentRequest, DiscardReplacementSegmentResponse, ListChunksRequest,
    ListChunksResponse, MutateStripReservationRequest, MutateStripReservationResponse,
    PrepareMirrorToEcConversionRequest, PrepareMirrorToEcConversionResponse, QueryChunkRequest,
    QueryChunkResponse, QuerySegmentOwnerRequest, QuerySegmentOwnerResponse, RelocateSegmentHandoffRequest,
    RelocateSegmentHandoffResponse, ReplaceChunkStripRangeRequest, ReplaceChunkStripRangeResponse,
    ReserveStripGroupRequest, ReserveStripGroupResponse, SealChunkRequest, SealChunkResponse,
    TriggerConversionBatchRequest, TriggerConversionBatchResponse, TriggerConversionRequest,
    TriggerConversionResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::InstanceId;

use crate::{ChunkdbClientError, ChunkdbRpcTransport, Result};

#[path = "native_routes.rs"]
mod native_routes;
#[path = "retry.rs"]
mod retry;
pub use native_routes::NativeChunkRoutes;

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
            // Range bindings can become visible in group-0 up to one server
            // refresh tick before the new owner installs them locally.
            max_retries: 5,
            initial_backoff: Duration::from_millis(50),
        }
    }
}

/// Client for CROWDB chunkdb operations via crowdb-rpc.
pub struct ChunkdbClient {
    svc: ServiceRegistryClient,
    /// `instance_id -> rpc_endpoint` cache.
    endpoint_cache: ArcSwap<HashMap<InstanceId, String>>,
    retry: RetryConfig,
    /// Service slot authority is required for every client.
    range_binding: RangeBindingClient,
    /// crowdb-rpc transport.
    rpc_transport: Arc<ChunkdbRpcTransport>,
    /// Last successful (or in-flight) Group-0 endpoint refresh.
    last_registry_refresh_ms: AtomicU64,
}

impl ChunkdbClient {
    pub async fn ad_hoc_ec_recovery(
        &self,
        req: crowdb_protocol::chunkdb::rpc::AdHocEcRecoveryRequest,
    ) -> Result<crowdb_protocol::chunkdb::rpc::AdHocEcRecoveryResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let request = req.clone();
            async move { transport.send_ad_hoc_ec_recovery(&endpoint, &request).await }
        })
        .await
    }
    #[must_use]
    pub fn new(svc: ServiceRegistryClient, rpc_transport: Arc<ChunkdbRpcTransport>) -> Self {
        let range_binding = RangeBindingClient::from_shared(svc.shared_kv());
        Self {
            svc,
            endpoint_cache: ArcSwap::from_pointee(HashMap::new()),
            retry: RetryConfig::default(),
            range_binding,
            rpc_transport,
            last_registry_refresh_ms: AtomicU64::new(0),
        }
    }

    /// Override the default retry config.
    #[must_use]
    pub fn with_retry_config(
        svc: ServiceRegistryClient,
        retry: RetryConfig,
        rpc_transport: Arc<ChunkdbRpcTransport>,
    ) -> Self {
        let range_binding = RangeBindingClient::from_shared(svc.shared_kv());
        Self {
            svc,
            endpoint_cache: ArcSwap::from_pointee(HashMap::new()),
            retry,
            range_binding,
            rpc_transport,
            last_registry_refresh_ms: AtomicU64::new(0),
        }
    }

    /// Supply a shared service-slot routing client.
    #[must_use]
    pub fn with_range_binding(mut self, binding: RangeBindingClient) -> Self {
        self.range_binding = binding;
        self
    }

    /// Eager warm: read all chunkdb instances, populate the endpoint cache.
    pub async fn refresh_endpoints(&self) -> Result<()> {
        let instances = self
            .svc
            .read_all_instances("chunkdb")
            .await
            .map_err(|e| ChunkdbClientError::Unreachable(format!("read_all_instances: {e}")))?;
        let refreshed = instances
            .into_iter()
            .map(|(id, value)| (id, value.rpc_endpoint))
            .collect();
        self.endpoint_cache.store(Arc::new(refreshed));
        self.last_registry_refresh_ms
            .store(unix_time_ms(), Ordering::Release);
        Ok(())
    }

    async fn refresh_endpoints_if_due(&self) {
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
            self.last_registry_refresh_ms.store(0, Ordering::Release);
        }
    }

    /// Refresh `ChunkDB` service endpoints and range ownership bindings.
    pub async fn refresh_routes(&self) -> Result<()> {
        self.refresh_endpoints().await?;
        self.range_binding
            .refresh()
            .await
            .map_err(|error| ChunkdbClientError::Unreachable(format!("slot refresh failed: {error}")))?;
        Ok(())
    }

    /// Choose a live service owner with at least one slot for ID-less allocation.
    async fn first_endpoint(&self) -> Result<String> {
        self.refresh_endpoints_if_due().await;
        if self.range_binding.is_empty() {
            self.range_binding
                .refresh()
                .await
                .map_err(|error| ChunkdbClientError::Unreachable(format!("slot refresh failed: {error}")))?;
        }
        let endpoints = self.endpoint_cache.load();
        self.range_binding
            .snapshot()
            .into_iter()
            .filter(|binding| !binding.slots.is_empty())
            .find_map(|binding| endpoints.get(&binding.instance_id).cloned())
            .ok_or_else(|| {
                ChunkdbClientError::Unreachable("no live chunkdb owner with assigned slots".into())
            })
    }

    async fn endpoints_for_chunk(&self, chunk_id: Option<&ChunkId>) -> Result<Vec<String>> {
        self.refresh_endpoints_if_due().await;
        if let Some(id) = chunk_id {
            let binding = &self.range_binding;
            let owner = binding
                .route(id)
                .await
                .map_err(|error| ChunkdbClientError::Unreachable(format!("slot routing failed: {error}")))?;
            let cached = self.endpoint_cache.load();
            let endpoint = cached
                .get(&owner.instance_id)
                .cloned()
                .unwrap_or(owner.rpc_endpoint);
            return Ok(vec![endpoint]);
        }
        Ok(vec![self.first_endpoint().await?])
    }

    pub async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        self.rpc_transport
            .with_request_identity(self.allocate_chunk_retry(req))
            .await
    }

    /// Append strips to an existing chunk.
    pub async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_append_chunk(&ep, &req).await }
        })
        .await
    }

    /// Durably reserve a fenced group of strips without exposing them in the chunk.
    pub async fn reserve_strip_group(
        &self,
        req: ReserveStripGroupRequest,
    ) -> Result<ReserveStripGroupResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_reserve_strip_group(&endpoint, &req).await }
        })
        .await
    }

    /// Apply a fenced state transition to one strip in a reservation group.
    pub async fn mutate_strip_reservation(
        &self,
        req: MutateStripReservationRequest,
    ) -> Result<MutateStripReservationResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_mutate_strip_reservation(&endpoint, &req).await }
        })
        .await
    }

    /// Durably advance a shared chunk's fenced write cursor.
    pub async fn advance_chunk_write(
        &self,
        req: AdvanceChunkWriteRequest,
    ) -> Result<AdvanceChunkWriteResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_advance_chunk_write(&endpoint, &req).await }
        })
        .await
    }

    /// Query a chunk by ID.
    pub async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_read_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_query_chunk(&ep, &req).await }
        })
        .await
    }

    /// Query the current `ChunkDB` owner's disposition for an exact segment.
    pub async fn query_segment_owner(
        &self,
        req: QuerySegmentOwnerRequest,
    ) -> Result<QuerySegmentOwnerResponse> {
        let chunk_id = req.chunk_id;
        self.with_read_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_query_segment_owner(&endpoint, &req).await }
        })
        .await
    }

    /// Deliver or poll one durable exact-segment relocation handoff.
    pub async fn relocate_segment_handoff(
        &self,
        req: RelocateSegmentHandoffRequest,
    ) -> Result<RelocateSegmentHandoffResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_relocate_segment_handoff(&endpoint, &req).await }
        })
        .await
    }

    /// Seal a chunk.
    pub async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_seal_chunk(&ep, &req).await }
        })
        .await
    }

    /// Delete a chunk.
    pub async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_delete_chunk(&ep, &req).await }
        })
        .await
    }

    /// Delete a range within a chunk.
    pub async fn delete_chunk_range(&self, req: DeleteChunkRangeRequest) -> Result<DeleteChunkRangeResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_delete_chunk_range(&ep, &req).await }
        })
        .await
    }

    /// Update a single strip within a chunk.
    pub async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_update_chunk_strip(&ep, &req).await }
        })
        .await
    }

    pub async fn allocate_replacement_segment(
        &self,
        req: AllocateReplacementSegmentRequest,
    ) -> Result<AllocateReplacementSegmentResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_allocate_replacement_segment(&endpoint, &req).await }
        })
        .await
    }

    pub async fn discard_replacement_segment(
        &self,
        req: DiscardReplacementSegmentRequest,
    ) -> Result<DiscardReplacementSegmentResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_discard_replacement_segment(&endpoint, &req).await }
        })
        .await
    }

    pub async fn replace_chunk_strip_range(
        &self,
        req: ReplaceChunkStripRangeRequest,
    ) -> Result<ReplaceChunkStripRangeResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_replace_chunk_strip_range(&endpoint, &req).await }
        })
        .await
    }

    pub async fn prepare_mirror_to_ec_conversion(
        &self,
        req: PrepareMirrorToEcConversionRequest,
    ) -> Result<PrepareMirrorToEcConversionResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move {
                transport
                    .send_prepare_mirror_to_ec_conversion(&endpoint, &req)
                    .await
            }
        })
        .await
    }

    pub async fn complete_mirror_to_ec_conversion(
        &self,
        req: CompleteMirrorToEcConversionRequest,
    ) -> Result<CompleteMirrorToEcConversionResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move {
                transport
                    .send_complete_mirror_to_ec_conversion(&endpoint, &req)
                    .await
            }
        })
        .await
    }

    pub async fn trigger_conversion(
        &self,
        req: TriggerConversionRequest,
    ) -> Result<TriggerConversionResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_trigger_conversion(&endpoint, &req).await }
        })
        .await
    }

    pub async fn trigger_conversion_batch(
        &self,
        req: TriggerConversionBatchRequest,
    ) -> Result<TriggerConversionBatchResponse> {
        self.with_rpc_retry(None, |transport, endpoint| {
            let req = req.clone();
            async move { transport.send_trigger_conversion_batch(&endpoint, &req).await }
        })
        .await
    }

    /// List chunks with pagination.
    pub async fn list_chunks(&self, req: ListChunksRequest) -> Result<ListChunksResponse> {
        self.with_read_retry(None, |t, ep| {
            let req = req.clone();
            async move { t.send_list_chunks(&ep, &req).await }
        })
        .await
    }
}
