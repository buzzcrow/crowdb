// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crowdb_protocol::chunk_kv::{
    ChunkKvResponse, ChunkKvRpcErrorCode, ClientRequestId, PointOperation, PointRequest, RequestRouting,
    RpcCompareCondition, RpcJournalPosition,
};

use crate::{
    CatalogCache, CatalogMap, CatalogSource, ChunkKvTransport, ClientConfig, ClientError,
    RequestIdentityAllocator, Result,
};

pub struct ChunkKvClient {
    config: ClientConfig,
    catalog_source: Arc<dyn CatalogSource>,
    transport: Arc<dyn ChunkKvTransport>,
    cache: Arc<CatalogCache>,
    identities: RequestIdentityAllocator,
}

impl ChunkKvClient {
    /// Creates a cold routed client. It cannot issue a nonempty operation until
    /// its first catalog refresh succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error for an unbounded or zero client configuration.
    pub fn new(
        config: ClientConfig,
        catalog_source: Arc<dyn CatalogSource>,
        transport: Arc<dyn ChunkKvTransport>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            catalog_source,
            transport,
            cache: Arc::new(CatalogCache::default()),
            identities: RequestIdentityAllocator::new(),
        })
    }

    #[must_use]
    pub fn client_instance_id(&self) -> crowdb_protocol::chunk_kv::Id128 {
        self.identities.client_instance_id()
    }

    #[must_use]
    pub fn cached_catalog(&self) -> Option<Arc<CatalogMap>> {
        self.cache.load()
    }

    /// Loads and atomically publishes one complete catalog generation.
    ///
    /// # Errors
    ///
    /// Returns source or validation failure while preserving a valid warm cache.
    pub async fn refresh_catalog(&self) -> Result<Arc<CatalogMap>> {
        let (head, pages) = self.catalog_source.load().await?;
        let map = CatalogMap::decode(&head, &pages)?;
        self.cache.install(map)?;
        self.cache
            .load()
            .ok_or_else(|| ClientError::CatalogUnavailable("refresh produced no catalog".into()))
    }

    /// Executes one get through the current owner and preserves all R143 fields.
    ///
    /// # Errors
    ///
    /// Returns discovery, transport, deadline, identity, or configuration errors.
    pub async fn get(
        &self,
        key: Vec<u8>,
        min_journal_position: Option<RpcJournalPosition>,
    ) -> Result<ChunkKvResponse> {
        let request_id = self.identities.allocate()?;
        self.execute_with_identity(PointOperation::Get { key }, min_journal_position, request_id)
            .await
    }

    /// Executes one put with one identity retained across every retry.
    ///
    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<ChunkKvResponse> {
        self.execute(PointOperation::Put { key, value }).await
    }

    /// Executes one delete with one identity retained across every retry.
    ///
    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn delete(&self, key: Vec<u8>) -> Result<ChunkKvResponse> {
        self.execute(PointOperation::Delete { key }).await
    }

    /// Executes put-if-absent and preserves its condition result.
    ///
    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn put_if_absent(&self, key: Vec<u8>, value: Vec<u8>) -> Result<ChunkKvResponse> {
        self.execute(PointOperation::PutIfAbsent { key, value }).await
    }

    /// Executes compare-exchange and preserves its condition result.
    ///
    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn compare_exchange(
        &self,
        key: Vec<u8>,
        condition: RpcCompareCondition,
        value: Vec<u8>,
    ) -> Result<ChunkKvResponse> {
        self.execute(PointOperation::CompareExchange {
            key,
            condition,
            value,
        })
        .await
    }

    /// Executes conditional delete and preserves its condition result.
    ///
    /// # Errors
    ///
    /// Returns discovery, transport, deadline, or identity exhaustion errors.
    pub async fn conditional_delete(
        &self,
        key: Vec<u8>,
        condition: RpcCompareCondition,
    ) -> Result<ChunkKvResponse> {
        self.execute(PointOperation::ConditionalDelete { key, condition })
            .await
    }

    /// Resubmits an application-persisted identity and operation unchanged.
    ///
    /// # Errors
    ///
    /// Returns validation, discovery, transport, or deadline errors. A server
    /// `RequestConflict` remains a typed response rather than a client error.
    pub async fn execute_with_identity(
        &self,
        operation: PointOperation,
        min_journal_position: Option<RpcJournalPosition>,
        request_id: ClientRequestId,
    ) -> Result<ChunkKvResponse> {
        request_id
            .validate()
            .map_err(|error| ClientError::InvalidRequest(error.to_string()))?;
        let deadline = Instant::now() + self.config.operation_timeout;
        let timeout_ms = u64::try_from(self.config.operation_timeout.as_millis())
            .map_err(|_| ClientError::InvalidRequest("operation timeout exceeds wire range".into()))?;
        let deadline_ms = wall_now_ms()
            .checked_add(timeout_ms)
            .ok_or(ClientError::Deadline)?;
        let mut attempts = 0_u32;
        let mut refreshes = 0_u32;
        let mut last_transport_error = None;
        let mut last_response = None;

        while attempts < self.config.max_attempts {
            attempts += 1;
            let map = match self.cache.load() {
                Some(map) => map,
                None => self.refresh_with_deadline(deadline).await?,
            };
            let entry = map
                .route(operation.key())
                .ok_or_else(|| ClientError::InvalidCatalog("cached map did not route key".into()))?;
            let request = PointRequest {
                routing: RequestRouting {
                    request_id,
                    map_revision: map.generation(),
                    partition_id: entry.partition_id,
                    owner_epoch: entry.owner_epoch,
                    min_journal_position,
                    deadline_ms: Some(deadline_ms),
                },
                operation: operation.clone(),
            };
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(ClientError::Deadline)?;
            match tokio::time::timeout(
                remaining,
                self.transport.point(&entry.owner.rpc_endpoint, &request),
            )
            .await
            {
                Err(_) => return Err(ClientError::Deadline),
                Ok(Err(error)) => {
                    last_transport_error = Some(error);
                    if refreshes < self.config.max_route_refreshes {
                        refreshes += 1;
                        let _ = self.refresh_with_deadline(deadline).await;
                    }
                }
                Ok(Ok(response)) => {
                    let retry = response.result.as_ref().err().is_some_and(|failure| {
                        matches!(
                            failure.code,
                            ChunkKvRpcErrorCode::NotMyRange
                                | ChunkKvRpcErrorCode::Overloaded
                                | ChunkKvRpcErrorCode::WriteStalled
                                | ChunkKvRpcErrorCode::Recovering
                                | ChunkKvRpcErrorCode::LeaseExpired
                        )
                    });
                    if !retry {
                        return Ok(response);
                    }
                    let refresh_route = response.result.as_ref().err().is_some_and(|failure| {
                        matches!(
                            failure.code,
                            ChunkKvRpcErrorCode::NotMyRange | ChunkKvRpcErrorCode::LeaseExpired
                        )
                    });
                    last_response = Some(response);
                    if refresh_route && refreshes < self.config.max_route_refreshes {
                        refreshes += 1;
                        let _ = self.refresh_with_deadline(deadline).await;
                    }
                }
            }
            if attempts < self.config.max_attempts {
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(ClientError::Deadline)?;
                tokio::time::sleep(self.config.retry_backoff.min(remaining)).await;
            }
        }
        if let Some(response) = last_response {
            Ok(response)
        } else {
            Err(last_transport_error
                .unwrap_or_else(|| ClientError::Transport("retry budget exhausted".into())))
        }
    }

    async fn execute(&self, operation: PointOperation) -> Result<ChunkKvResponse> {
        let request_id = self.identities.allocate()?;
        self.execute_with_identity(operation, None, request_id).await
    }

    async fn refresh_with_deadline(&self, deadline: Instant) -> Result<Arc<CatalogMap>> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(ClientError::Deadline)?;
        tokio::time::timeout(remaining, self.refresh_catalog())
            .await
            .map_err(|_| ClientError::Deadline)?
    }
}

fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
