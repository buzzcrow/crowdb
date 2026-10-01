// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Iceberg catalog and foreground chunk-client wiring.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig, ChunkIoWriter, ChunkReadPolicy, ChunkWriteTiming,
    FramedWriteBuffer, IoError, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::{ChunkType, Location};
use crowdb_protocol::frame::MAX_FRAME_PAYLOAD_BYTES;

use crate::catalog::{CatalogRepository, ClearBounds, RoutedCatalogStore};
use crate::file::{FileBlockStore, NativeFileBlocks};

pub type IcebergStorageError = Box<dyn std::error::Error + Send + Sync>;

pub fn own_large_write(policy: &mut LargeWritePolicy) {
    Arc::make_mut(&mut policy.client).chunk_type = ChunkType::IcebergTable;
}

#[must_use]
pub fn default_large_write() -> LargeWritePolicy {
    let mut policy = LargeWritePolicy {
        ec_scheme: EcScheme::new(8, 4),
        client: Arc::new(ChunkClientConfig::default()),
    };
    own_large_write(&mut policy);
    policy
}

#[must_use]
pub fn foreground_blocks(chunks: ChunkIoClient, store: Arc<RoutedCatalogStore>) -> Arc<dyn FileBlockStore> {
    Arc::new(NativeFileBlocks::new(chunks, store))
}

pub struct IcebergFileWriter {
    inner: Box<dyn ChunkIoWriter>,
}

#[async_trait::async_trait]
impl ChunkIoWriter for IcebergFileWriter {
    async fn on_data(&mut self, buffer: Bytes) -> Result<crowdb_chunk_client::FeedStatus, IoError> {
        self.inner.on_data(buffer).await
    }

    async fn on_framed_data(
        &mut self,
        buffer: Box<dyn FramedWriteBuffer>,
    ) -> Result<crowdb_chunk_client::FeedStatus, IoError> {
        self.inner.on_framed_data(buffer).await
    }

    async fn on_finish(&mut self) -> Result<Vec<Location>, IoError> {
        self.inner.on_finish().await
    }

    async fn on_error(&mut self) -> Result<Vec<Location>, IoError> {
        self.inner.on_error().await
    }

    fn require_data(&self) -> bool {
        self.inner.require_data()
    }

    fn input_complete(&self) -> bool {
        self.inner.input_complete()
    }

    fn write_timing(&self) -> Option<ChunkWriteTiming> {
        self.inner.write_timing()
    }

    async fn wait_for_capacity(&mut self) {
        self.inner.wait_for_capacity().await;
    }
}

impl IcebergFileWriter {
    #[must_use]
    pub fn require_data(&self) -> bool {
        self.inner.require_data()
    }

    #[must_use]
    pub fn input_complete(&self) -> bool {
        self.inner.input_complete()
    }

    #[must_use]
    pub fn write_timing(&self) -> Option<ChunkWriteTiming> {
        self.inner.write_timing()
    }

    pub async fn wait_for_capacity(&mut self) {
        self.inner.wait_for_capacity().await;
    }

    /// # Errors
    /// Returns a chunk write failure.
    pub async fn on_data(&mut self, bytes: Bytes) -> Result<(), IoError> {
        self.inner.on_data(bytes).await.map(|_| ())
    }

    /// # Errors
    /// Returns a chunk write failure.
    pub async fn on_framed_data(&mut self, buffer: Box<dyn FramedWriteBuffer>) -> Result<(), IoError> {
        self.inner.on_framed_data(buffer).await.map(|_| ())
    }

    /// # Errors
    /// Returns a chunk seal failure.
    pub async fn on_finish(&mut self) -> Result<Vec<Location>, IoError> {
        self.inner.on_finish().await
    }

    /// # Errors
    /// Returns a chunk cleanup failure.
    pub async fn on_error(&mut self) -> Result<(), IoError> {
        self.inner.on_error().await.map(|_| ())
    }
}

/// Prepares the chunk writer for one foreground Iceberg file upload.
///
/// # Errors
/// Returns a chunk admission or preparation failure.
pub async fn prepare_file_writer(
    chunks: &ChunkIoClient,
    location_key: &str,
    small_length: Option<usize>,
    declared_length: Option<u64>,
    large_write: &LargeWritePolicy,
) -> Result<IcebergFileWriter, IoError> {
    if let Some(length) = small_length {
        if length <= MAX_FRAME_PAYLOAD_BYTES {
            let mut writer = chunks
                .prepare_small_write_for_key(length, location_key.as_bytes())
                .await?;
            writer.require_durable_completion();
            return Ok(IcebergFileWriter {
                inner: Box::new(writer),
            });
        }
        return Ok(IcebergFileWriter {
            inner: Box::new(
                chunks
                    .prepare_shared_object_write_for_key(length, location_key.as_bytes())
                    .await?,
            ),
        });
    }
    let mut writer = chunks.prepare_large_write(declared_length, large_write.clone());
    writer.wait_until_prepared().await?;
    Ok(IcebergFileWriter {
        inner: Box::new(writer),
    })
}

pub struct IcebergLargeWriteSettings {
    pub ec_data: usize,
    pub ec_code: usize,
    pub disk_block_bytes: usize,
    pub mirror_copies: Option<u32>,
    pub max_chunk_size: Option<u64>,
    pub memory_budget_bytes: Option<usize>,
    pub prefetch_strips_per_chunk: Option<usize>,
    pub prefetch_max_strips_per_batch: Option<usize>,
    pub parallel_strip_writes: Option<usize>,
    pub held_buffers: Option<usize>,
    pub chunk_preparation_depth: Option<usize>,
}

impl IcebergLargeWriteSettings {
    /// # Errors
    /// Rejects invalid Iceberg large-write geometry before connecting storage.
    pub fn policy(self) -> Result<LargeWritePolicy, String> {
        if self.ec_data == 0 || self.ec_data > 32 || self.ec_code == 0 {
            return Err("Iceberg EC data and code counts are invalid".into());
        }
        let mut client = ChunkClientConfig {
            chunk_type: ChunkType::IcebergTable,
            large_mirror_copies: self.mirror_copies,
            read_buffer_size: self.disk_block_bytes,
            ..ChunkClientConfig::default()
        };
        if let Some(value) = self.max_chunk_size {
            client.max_chunk_size = value;
        }
        if let Some(value) = self.memory_budget_bytes {
            client.memory_budget = value;
        }
        if let Some(value) = self.prefetch_strips_per_chunk {
            client.prefetch_strips_per_chunk = value;
        }
        if let Some(value) = self.prefetch_max_strips_per_batch {
            client.large_prefetch_max_strips_per_batch = value;
        }
        if let Some(value) = self.parallel_strip_writes {
            client.large_parallel_strip_writes = value;
        }
        if let Some(value) = self.held_buffers {
            client.large_held_buffers = value;
        }
        if let Some(value) = self.chunk_preparation_depth {
            client.chunk_preparation_depth = value;
        }
        client.validate().map_err(|error| error.to_string())?;
        Ok(LargeWritePolicy {
            ec_scheme: EcScheme::new(self.ec_data, self.ec_code),
            client: Arc::new(client),
        })
    }
}

/// Connects Iceberg metadata and a separately budgeted foreground chunk pool.
///
/// # Errors
/// Returns a discovery, catalog, or chunk-client configuration failure.
pub async fn connect(
    seeds: Vec<String>,
    read_policy: ChunkReadPolicy,
    mut small_write: SmallWritePolicy,
    diskio_connections_per_endpoint: usize,
    diskio_rpc_workers: u32,
) -> Result<(Arc<CatalogRepository>, Arc<RoutedCatalogStore>, ChunkIoClient), IcebergStorageError> {
    small_write.chunk_type = ChunkType::IcebergTable;
    let control = Arc::new(CrowdbKvClient::new(KvConfig::new(seeds.clone())));
    let client_config = ClientConfig::default();
    let source = Arc::new(Group0ChunkKvRangeCatalogSource::from_shared(Arc::clone(&control)));
    let transport = Arc::new(ChunkKvRpcTransport::new(
        client_config.max_owner_connections,
        1,
        2,
    ));
    let client = Arc::new(ChunkKvClient::new(client_config, source, transport)?);
    client.refresh_catalog().await?;
    let store = Arc::new(RoutedCatalogStore::new(client));
    let repository = Arc::new(CatalogRepository::new(
        store.clone(),
        ClearBounds {
            request_ms: 300_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )?);
    let chunks = ChunkIoClient::connect_with_kv_read_policy(
        ChunkIoClientConfig {
            management_seeds: seeds,
            diskio_connections_per_endpoint,
            diskio_rpc_workers,
            small_write,
        },
        control,
        read_policy,
    )
    .await?;
    Ok((repository, store, chunks))
}
