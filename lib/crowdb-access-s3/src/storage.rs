// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production client wiring for stateless S3 handlers.

use std::sync::Arc;

use crate::metadata::ChunkKvMetadataStore;
use crowdb_chunk_client::{
    ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig as ChunkKvConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::ChunkType;

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

pub fn own_large_write(policy: &mut LargeWritePolicy) {
    Arc::make_mut(&mut policy.client).chunk_type = ChunkType::S3;
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
        Self::connect_with_read_policy(
            management_seeds,
            diskio_connections_per_endpoint,
            diskio_rpc_workers,
            small_write,
            ChunkReadPolicy::default(),
        )
        .await
    }

    /// # Errors
    /// Returns an error when the management or `DiskIO` connection cannot be established.
    pub async fn connect_with_read_policy(
        management_seeds: Vec<String>,
        diskio_connections_per_endpoint: usize,
        diskio_rpc_workers: u32,
        mut small_write: SmallWritePolicy,
        read_policy: ChunkReadPolicy,
    ) -> Result<Self, StorageConnectError> {
        small_write.chunk_type = ChunkType::S3;
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
        let chunks = ChunkIoClient::connect_with_kv_read_policy(
            ChunkIoClientConfig {
                management_seeds,
                diskio_connections_per_endpoint,
                diskio_rpc_workers,
                small_write,
            },
            Arc::clone(&kv),
            read_policy,
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
