// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;

use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::ChunkType;

use super::RuntimeConfig;
use crate::topology::{CatalogObservation, ServiceObservation};
use crate::{Error, Result};

pub(crate) struct Storage {
    pub owns_small_pool: bool,
    pub owns_transports: bool,
    pub control: Arc<CrowdbKvClient>,
    pub metadata: Arc<ChunkKvClient>,
    pub chunks: Arc<ChunkIoClient>,
    pub chunk_type: ChunkType,
    pub policy: SmallWritePolicy,
    pub metadata_transport: Arc<ChunkKvRpcTransport>,
    pub catalog: Arc<ArcSwap<CatalogObservation>>,
    pub services: Arc<ArcSwap<Vec<ServiceObservation>>>,
}

impl Storage {
    pub async fn connect(config: RuntimeConfig) -> Result<Self> {
        let policy = config.small_write.clone();
        let chunk_type = config.small_write.chunk_type;
        let control = Arc::new(CrowdbKvClient::new(KvConfig::new(
            config.management_seeds.clone(),
        )));
        let catalog = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::clone(&control)));
        let metadata_config = ClientConfig::default();
        let transport = Arc::new(
            ChunkKvRpcTransport::with_config(
                metadata_config.max_owner_connections,
                1,
                2,
                &config.chunk_kv_transport,
            )
            .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let metadata = Arc::new(
            ChunkKvClient::new(metadata_config, catalog, transport.clone())
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let catalog = metadata
            .refresh_catalog()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let catalog_observed_at = Instant::now();
        let chunks = ChunkIoClient::connect_with_kv_transport(
            ChunkIoClientConfig {
                management_seeds: config.management_seeds,
                diskio_connections_per_endpoint: config.diskio_connections_per_endpoint,
                diskio_rpc_workers: config.diskio_rpc_workers,
                small_write: config.small_write,
            },
            Arc::clone(&control),
            config.read_policy,
            config.data_rdma,
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(Self {
            owns_small_pool: true,
            owns_transports: true,
            control,
            metadata,
            chunks: Arc::new(chunks),
            chunk_type,
            policy,
            metadata_transport: transport,
            catalog: Arc::new(ArcSwap::from_pointee(CatalogObservation {
                catalog,
                observed_at: Some(catalog_observed_at),
            })),
            services: Arc::new(ArcSwap::from_pointee(Vec::new())),
        })
    }
}
