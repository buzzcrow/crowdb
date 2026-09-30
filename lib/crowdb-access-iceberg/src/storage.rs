// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Iceberg catalog and foreground chunk-client wiring.

use std::sync::Arc;

use crowdb_chunk_client::{
    ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRpcTransport, ClientConfig, Group0ChunkKvRangeCatalogSource,
};
use crowdb_kv_client::{ClientConfig as KvConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::ChunkType;

use crate::catalog::{CatalogRepository, ClearBounds, RoutedCatalogStore};

pub type IcebergStorageError = Box<dyn std::error::Error + Send + Sync>;

pub fn own_large_write(policy: &mut LargeWritePolicy) {
    Arc::make_mut(&mut policy.client).chunk_type = ChunkType::IcebergTable;
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
