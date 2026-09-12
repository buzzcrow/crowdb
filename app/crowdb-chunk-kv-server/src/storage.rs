// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Production chunk-stream dependency assembly.

use std::sync::Arc;

use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunk_stream::{ChunkStream, ProductionStreamRuntime, StreamConfig, StreamName};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_tree_ffi::{
    ChunkPageStoreOptions, ChunkRootCatalog, ChunkTransport, OwnedChunkRpcDiskRoute,
    OwnedChunkRpcTransportOptions, PageStore,
};
use thiserror::Error;

use crate::ChunkKvServerConfig;

#[derive(Debug, Error)]
pub enum StorageRuntimeError {
    #[error("failed to connect chunk IO: {0}")]
    ChunkIo(String),
    #[error("failed to configure chunk stream: {0}")]
    Stream(String),
    #[error("failed to configure native tree storage: {0}")]
    Tree(String),
}

/// Process-wide production clients shared by every hosted partition stream.
pub struct ChunkKvStorage {
    kv: Arc<CrowdbKvClient>,
    chunk_io: ChunkIoClient,
    streams: Arc<ProductionStreamRuntime>,
    tree_transport: Arc<ChunkTransport>,
    metadata_store_id: u64,
}

impl ChunkKvStorage {
    /// Discovers KV, `ChunkDB`, and `DiskIO` endpoints and creates the shared
    /// stream runtime used by hosted partitions.
    ///
    /// # Errors
    ///
    /// Returns an endpoint discovery or storage configuration error.
    pub async fn connect(config: &ChunkKvServerConfig) -> Result<Self, StorageRuntimeError> {
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
            config.group0_mgmt_seeds.clone(),
        )));
        let chunk_io = ChunkIoClient::connect_with_kv(
            ChunkIoClientConfig {
                management_seeds: config.group0_mgmt_seeds.clone(),
                diskio_connections_per_endpoint: config.storage.diskio_connections_per_endpoint,
                diskio_rpc_workers: config.storage.diskio_rpc_workers,
                small_write: SmallWritePolicy::default(),
            },
            Arc::clone(&kv),
        )
        .await
        .map_err(|error| StorageRuntimeError::ChunkIo(error.to_string()))?;
        Self::from_parts(
            kv,
            chunk_io,
            config.storage.metadata_store_id,
            config.storage.stream_writer_lease_ms,
        )
        .await
    }

    /// Assembles production adapters from already connected process clients.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid metadata store, lease, or read policy.
    pub async fn from_parts(
        kv: Arc<CrowdbKvClient>,
        chunk_io: ChunkIoClient,
        metadata_store_id: u64,
        writer_lease_ms: u64,
    ) -> Result<Self, StorageRuntimeError> {
        if metadata_store_id == 0 {
            return Err(StorageRuntimeError::Stream(
                "stream metadata store must be nonzero".into(),
            ));
        }
        let streams = Arc::new(
            ProductionStreamRuntime::new(
                Arc::clone(&kv),
                &chunk_io,
                writer_lease_ms,
                ChunkReadPolicy::default(),
                StreamConfig::default(),
            )
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?,
        );
        let (chunkdb, disks) = chunk_io
            .native_storage_routes()
            .await
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?;
        let disk_routes = disks
            .into_iter()
            .map(|(disk_id, route)| OwnedChunkRpcDiskRoute {
                disk_id_high: disk_id.high,
                disk_id_low: disk_id.low,
                route,
            })
            .collect();
        let tree_transport = Arc::new(
            ChunkTransport::open_owned_rpc(OwnedChunkRpcTransportOptions {
                chunkdb,
                disk_routes,
                writer_lease_ms,
                rpc_timeout_ms: writer_lease_ms,
                completion_capacity: 1_024,
            })
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?,
        );
        Ok(Self {
            kv,
            chunk_io,
            streams,
            tree_transport,
            metadata_store_id,
        })
    }

    #[must_use]
    pub fn kv(&self) -> &Arc<CrowdbKvClient> {
        &self.kv
    }

    #[must_use]
    pub fn chunk_io(&self) -> &ChunkIoClient {
        &self.chunk_io
    }

    #[must_use]
    pub fn streams(&self) -> &Arc<ProductionStreamRuntime> {
        &self.streams
    }

    /// Open the R140 page store with the process-owned native RPC transport.
    /// The supplied catalog determines durable generation publication and
    /// ownership fencing for this tree identity.
    ///
    /// # Errors
    ///
    /// Returns an invalid native page-store or transport error.
    pub fn open_tree_page_store(
        &self,
        options: ChunkPageStoreOptions,
        catalog: Arc<ChunkRootCatalog>,
    ) -> Result<Arc<PageStore>, StorageRuntimeError> {
        PageStore::open_chunk(options, catalog, Some(&self.tree_transport))
            .map(Arc::new)
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))
    }

    /// Initializes the stream selected by an Active catalog binding.
    ///
    /// # Errors
    ///
    /// Returns a registry, metadata, chunk IO, or fencing error.
    pub async fn create_registered_stream(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ChunkStream, StorageRuntimeError> {
        self.streams
            .create_registered(stream_name, self.metadata_store_id, writer_epoch)
            .await
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))
    }

    /// Reopens one assigned stream under the supplied ownership epoch.
    ///
    /// # Errors
    ///
    /// Returns a registry, metadata, chunk IO, or fencing error.
    pub async fn open_stream(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ChunkStream, StorageRuntimeError> {
        self.streams
            .open(stream_name, self.metadata_store_id, writer_epoch)
            .await
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))
    }
}
