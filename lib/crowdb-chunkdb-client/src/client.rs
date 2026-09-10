// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkdbClient` — client library for CROWDB chunkdb operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all chunkdb
//! instances from the service registry, populates a `DashMap` cache
//! (`instance_id -> rpc_endpoint`). On cache miss, lazily refreshes.
//! Retry: exponential backoff on transient errors, up to `max_retries`.

use std::sync::Arc;
use std::time::Duration;

use std::collections::HashMap;

use arc_swap::ArcSwap;

use crowdb_kv_client::{RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunk_id::ChunkIdParts;
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AdvanceChunkWriteResponse, AllocateChunkRequest, AllocateChunkResponse,
    AllocateReplacementSegmentRequest, AllocateReplacementSegmentResponse, AppendChunkRequest,
    AppendChunkResponse, CompleteMirrorToEcConversionRequest, CompleteMirrorToEcConversionResponse,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse,
    DiscardReplacementSegmentRequest, DiscardReplacementSegmentResponse, ListChunksRequest,
    ListChunksResponse, MutateStripReservationRequest, MutateStripReservationResponse,
    PrepareMirrorToEcConversionRequest, PrepareMirrorToEcConversionResponse, QueryChunkRequest,
    QueryChunkResponse, ReplaceChunkStripRangeRequest, ReplaceChunkStripRangeResponse,
    ReserveStripGroupRequest, ReserveStripGroupResponse, SealChunkRequest, SealChunkResponse,
    TriggerConversionBatchRequest, TriggerConversionBatchResponse, TriggerConversionRequest,
    TriggerConversionResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::InstanceId;

use crate::{ChunkdbClientError, ChunkdbRpcTransport, Result};

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
    /// Optional range binding client for R99 sharded mode. When
    /// present, chunk IDs are routed to the owning instance. When
    /// `None`, falls back to "any instance" (v1 behavior).
    range_binding: Option<RangeBindingClient>,
    /// crowdb-rpc transport.
    rpc_transport: Arc<ChunkdbRpcTransport>,
}

impl ChunkdbClient {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient, rpc_transport: Arc<ChunkdbRpcTransport>) -> Self {
        Self {
            svc,
            endpoint_cache: ArcSwap::from_pointee(HashMap::new()),
            retry: RetryConfig::default(),
            range_binding: None,
            rpc_transport,
        }
    }

    /// Override the default retry config.
    #[must_use]
    pub fn with_retry_config(
        svc: ServiceRegistryClient,
        retry: RetryConfig,
        rpc_transport: Arc<ChunkdbRpcTransport>,
    ) -> Self {
        Self {
            svc,
            endpoint_cache: ArcSwap::from_pointee(HashMap::new()),
            retry,
            range_binding: None,
            rpc_transport,
        }
    }

    /// Enable R99 range-based routing. When set, chunk IDs are routed
    /// to the owning chunkdb instance via the `RangeBindingClient`.
    #[must_use]
    pub fn with_range_binding(mut self, binding: RangeBindingClient) -> Self {
        self.range_binding = Some(binding);
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
        Ok(())
    }

    /// Refresh `ChunkDB` service endpoints and range ownership bindings.
    pub async fn refresh_routes(&self) -> Result<()> {
        self.refresh_endpoints().await?;
        if let Some(binding) = &self.range_binding {
            binding
                .refresh()
                .await
                .map_err(|error| ChunkdbClientError::Unreachable(format!("range refresh failed: {error}")))?;
        }
        Ok(())
    }

    /// Get the first cached endpoint (or refresh + pick first).
    async fn first_endpoint(&self) -> Result<String> {
        if let Some(endpoint) = self.endpoint_cache.load().values().next() {
            return Ok(endpoint.clone());
        }
        self.refresh_endpoints().await?;
        self.endpoint_cache
            .load()
            .values()
            .next()
            .cloned()
            .ok_or_else(|| ChunkdbClientError::Unreachable("no chunkdb instances registered".into()))
    }

    async fn endpoints_for_chunk(&self, chunk_id: Option<&ChunkId>) -> Result<Vec<String>> {
        if let (Some(binding), Some(id)) = (&self.range_binding, chunk_id) {
            binding
                .route(id)
                .await
                .map_err(|error| ChunkdbClientError::Unreachable(format!("range routing failed: {error}")))?;
            let bucket = ChunkIdParts::from_proto(id).hash_to_bucket();
            let route = binding
                .route_with_fallback(bucket)
                .map_err(|error| ChunkdbClientError::Unreachable(format!("range routing failed: {error}")))?;
            let mut endpoints = vec![route.primary.rpc_endpoint];
            if let Some(fallback) = route.fallback {
                if fallback.rpc_endpoint != endpoints[0] {
                    endpoints.push(fallback.rpc_endpoint);
                }
            }
            return Ok(endpoints);
        }
        Ok(vec![self.first_endpoint().await?])
    }

    /// Execute a crowdb-rpc call with retry on transient errors.
    async fn with_rpc_retry<T, F, Fut>(&self, chunk_id: Option<&ChunkId>, op: F) -> Result<T>
    where
        F: Fn(Arc<ChunkdbRpcTransport>, String) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let transport = Arc::clone(&self.rpc_transport);
        let mut attempts = 0u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            let endpoints = self.endpoints_for_chunk(chunk_id).await?;
            let mut last_error = None;
            for endpoint in endpoints {
                match op(Arc::clone(&transport), endpoint).await {
                    Ok(value) => return Ok(value),
                    Err(error) if error.is_transient() => last_error = Some(error),
                    Err(error) => return Err(error),
                }
            }
            let Some(error) = last_error else {
                return Err(ChunkdbClientError::Unreachable(
                    "range routing supplied no endpoint".into(),
                ));
            };
            if attempts >= self.retry.max_retries {
                return Err(error);
            }
            attempts += 1;
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2);
            let _ = self.refresh_endpoints().await;
            if matches!(error, ChunkdbClientError::NotMyRange(_)) {
                if let Some(binding) = &self.range_binding {
                    if let Some(id) = chunk_id {
                        let _ = binding.refresh_and_route(id).await;
                    } else {
                        let _ = binding.refresh().await;
                    }
                }
            }
        }
    }

    /// Allocate a new chunk.
    pub async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let chunk_id = req.chunk_id;
        let mut attempts = 0_u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            let endpoints = self.endpoints_for_chunk(chunk_id.as_ref()).await?;
            let mut not_my_range = None;
            for endpoint in endpoints {
                // Allocation is not idempotent at the DiskDB layer. Trying the
                // transition fallback is safe only after NotMyRange, which is
                // rejected before mutation.
                match self.rpc_transport.send_allocate_chunk(&endpoint, &req).await {
                    Ok(response) => return Ok(response),
                    Err(error @ ChunkdbClientError::NotMyRange(_)) => {
                        not_my_range = Some(error);
                    }
                    Err(error) => return Err(error),
                }
            }
            let Some(error) = not_my_range else {
                return Err(ChunkdbClientError::Unreachable(
                    "range routing supplied no endpoint".into(),
                ));
            };
            if attempts >= self.retry.max_retries {
                return Err(error);
            }
            attempts += 1;
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2);
            let _ = self.refresh_endpoints().await;
            if let (Some(binding), Some(id)) = (&self.range_binding, chunk_id.as_ref()) {
                let _ = binding.refresh_and_route(id).await;
            }
        }
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
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_query_chunk(&ep, &req).await }
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
        self.with_rpc_retry(None, |t, ep| {
            let req = req.clone();
            async move { t.send_list_chunks(&ep, &req).await }
        })
        .await
    }
}
