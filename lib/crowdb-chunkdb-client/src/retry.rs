// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Known admission failures reroute with the original request identity; unknown mutations do not replay.
use super::{ChunkdbClient, ChunkdbClientError, ChunkdbRpcTransport, Result};
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, AllocateChunkResponse};
use crowdb_protocol::common::ChunkId;
use std::{sync::Arc, time::Duration};

impl ChunkdbClient {
    pub(super) async fn with_rpc_retry<T, F, Fut>(&self, chunk_id: Option<&ChunkId>, op: F) -> Result<T>
    where
        F: Fn(Arc<ChunkdbRpcTransport>, String) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.rpc_transport
            .with_request_identity(self.retry_rpc(chunk_id, false, op))
            .await
    }

    pub(super) async fn with_read_retry<T, F, Fut>(&self, chunk_id: Option<&ChunkId>, op: F) -> Result<T>
    where
        F: Fn(Arc<ChunkdbRpcTransport>, String) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.rpc_transport
            .with_request_identity(self.retry_rpc(chunk_id, true, op))
            .await
    }

    /// Execute a crowdb-rpc call with retry on transient errors.
    async fn retry_rpc<T, F, Fut>(&self, chunk_id: Option<&ChunkId>, read_only: bool, op: F) -> Result<T>
    where
        F: Fn(Arc<ChunkdbRpcTransport>, String) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let transport = Arc::clone(&self.rpc_transport);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut attempts = 0u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            let endpoints = self.endpoints_for_chunk(chunk_id).await?;
            let mut last_error = None;
            for endpoint in endpoints {
                match tokio::time::timeout_at(deadline, op(Arc::clone(&transport), endpoint))
                    .await
                    .unwrap_or_else(|_| {
                        Err(ChunkdbClientError::OutcomeUnknown(
                            "operation deadline expired after RPC entry".into(),
                        ))
                    }) {
                    Ok(value) => return Ok(value),
                    Err(error)
                        if matches!(
                            error,
                            ChunkdbClientError::ConnectFailed(_)
                                | ChunkdbClientError::NotMyRange(_)
                                | ChunkdbClientError::Unavailable(_)
                        ) || (read_only
                            && (error.is_transient()
                                || matches!(error, ChunkdbClientError::OutcomeUnknown(_)))) =>
                    {
                        last_error = Some(error);
                    }
                    Err(error) => return Err(error),
                }
            }
            let Some(error) = last_error else {
                return Err(ChunkdbClientError::Unreachable(
                    "range routing supplied no endpoint".into(),
                ));
            };
            if matches!(error, ChunkdbClientError::NotMyRange(_)) {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ChunkdbClientError::DeadlineExceeded(
                        "ownership rerouting deadline expired".into(),
                    ));
                }
            } else {
                if attempts >= self.retry.max_retries {
                    return Err(error);
                }
                attempts += 1;
            }
            // Ownership rejection is a local admission outcome: immediately reroute,
            // including to the same process after its slot epoch advances.
            if !matches!(error, ChunkdbClientError::NotMyRange(_)) {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            let _ = self.refresh_endpoints().await;
            // A restarted owner may advertise a new RPC endpoint while the
            // cached range binding still names its old socket. Refresh both
            // sources on transient failures, not only on NotMyRange.
            let _ = self.range_binding.refresh().await;
        }
    }

    /// Allocate a new chunk.
    pub(super) async fn allocate_chunk_retry(
        &self,
        req: AllocateChunkRequest,
    ) -> Result<AllocateChunkResponse> {
        let chunk_id = req.chunk_id;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut attempts = 0_u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            let endpoints = self.endpoints_for_chunk(chunk_id.as_ref()).await?;
            let mut safe_retry = None;
            for endpoint in endpoints {
                // Allocation is not idempotent at the DiskDB layer. Retry only
                // an explicit ownership rejection or a pre-submission failure.
                match tokio::time::timeout_at(
                    deadline,
                    self.rpc_transport.send_allocate_chunk(&endpoint, &req),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(ChunkdbClientError::OutcomeUnknown(
                        "allocation deadline expired after RPC entry".into(),
                    ))
                }) {
                    Ok(response) => return Ok(response),
                    Err(
                        error @ (ChunkdbClientError::NotMyRange(_) | ChunkdbClientError::ConnectFailed(_)),
                    ) => {
                        safe_retry = Some(error);
                    }
                    Err(error) => return Err(error),
                }
            }
            let Some(error) = safe_retry else {
                return Err(ChunkdbClientError::Unreachable(
                    "range routing supplied no endpoint".into(),
                ));
            };
            if matches!(error, ChunkdbClientError::NotMyRange(_)) {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ChunkdbClientError::DeadlineExceeded(
                        "ownership rerouting deadline expired".into(),
                    ));
                }
            } else {
                if attempts >= self.retry.max_retries {
                    return Err(error);
                }
                attempts += 1;
            }
            // Ownership rejection is a local admission outcome: immediately reroute,
            // including to the same process after its slot epoch advances.
            if !matches!(error, ChunkdbClientError::NotMyRange(_)) {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            let _ = self.refresh_endpoints().await;
            let _ = self.range_binding.refresh().await;
        }
    }
}
