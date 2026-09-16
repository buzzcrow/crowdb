// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production client wiring for stateless S3 handlers.

use std::sync::Arc;

use crowdb_access_s3::metadata::ChunkKvMetadataStore;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig as ChunkKvConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};

#[derive(Clone)]
pub struct S3StorageClients {
    pub control: Arc<CrowdbKvClient>,
    pub metadata: Arc<ChunkKvMetadataStore>,
    pub chunks: Arc<ChunkIoClient>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageConnectError {
    #[error("Chunk-KV client configuration failed: {0}")]
    ChunkKv(String),
    #[error("chunk I/O discovery failed: {0}")]
    ChunkIo(String),
}

impl S3StorageClients {
    /// Connects metadata and chunk clients through one discovery client.
    ///
    /// # Errors
    ///
    /// Returns before readiness on configuration or discovery failure.
    pub async fn connect(
        management_seeds: Vec<String>,
        diskio_connections_per_endpoint: usize,
        diskio_rpc_workers: u32,
        small_write: SmallWritePolicy,
    ) -> Result<Self, StorageConnectError> {
        let kv = Arc::new(CrowdbKvClient::new(KvConfig::new(management_seeds.clone())));
        let config = ChunkKvConfig::default();
        let catalog = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::clone(&kv)));
        let transport = Arc::new(ChunkKvRpcTransport::new(config.max_owner_connections, 1, 2));
        let metadata = ChunkKvClient::new(config, catalog, transport)
            .map_err(|error| StorageConnectError::ChunkKv(error.to_string()))?;
        metadata
            .refresh_catalog()
            .await
            .map_err(|error| StorageConnectError::ChunkKv(error.to_string()))?;
        let chunks = ChunkIoClient::connect_with_kv(
            ChunkIoClientConfig {
                management_seeds,
                diskio_connections_per_endpoint,
                diskio_rpc_workers,
                small_write,
            },
            Arc::clone(&kv),
        )
        .await
        .map_err(|error| StorageConnectError::ChunkIo(error.to_string()))?;
        Ok(Self {
            control: kv,
            metadata: Arc::new(ChunkKvMetadataStore::new(Arc::new(metadata))),
            chunks: Arc::new(chunks),
        })
    }
}
