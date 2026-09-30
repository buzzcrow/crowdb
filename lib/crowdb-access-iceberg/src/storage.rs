// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Iceberg catalog and foreground chunk-client wiring.

use std::sync::Arc;

use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig, ChunkIoWriter, ChunkReadPolicy, IoError,
    LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_protocol::frame::MAX_FRAME_PAYLOAD_BYTES;

use crate::catalog::{CatalogRepository, ClearBounds, RoutedCatalogStore};

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
) -> Result<Box<dyn ChunkIoWriter>, IoError> {
    if let Some(length) = small_length {
        if length <= MAX_FRAME_PAYLOAD_BYTES {
            let mut writer = chunks
                .prepare_small_write_for_key(length, location_key.as_bytes())
                .await?;
            writer.require_durable_completion();
            return Ok(Box::new(writer));
        }
        return Ok(Box::new(
            chunks
                .prepare_shared_object_write_for_key(length, location_key.as_bytes())
                .await?,
        ));
    }
    let mut writer = chunks.prepare_large_write(declared_length, large_write.clone());
    writer.wait_until_prepared().await?;
    Ok(Box::new(writer))
}

pub struct IcebergLargeWriteSettings {
    pub ec_data: usize,
    pub ec_code: usize,
    pub disk_block_bytes: usize,
    pub mirror_copies: Option<u32>,
    pub max_chunk_size: Option<u64>,
    pub memory_budget_bytes: Option<usize>,
    pub prefetch_strips_per_chunk: Option<usize>,
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
