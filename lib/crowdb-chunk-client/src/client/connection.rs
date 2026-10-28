// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared discovery and explicit data-transport construction.

use super::{discover_current_range_bindings, ClientTopology};
use crate::metrics::SmallWriteMetrics;
use crate::negative_list::FailedDiskList;
use crate::writer::small_pool::SmallWritePool;
use crate::{ChunkIoClient, ChunkIoClientConfig, ChunkReadPolicy, ChunkReader, Result, RoutedDiskWriter};
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_paxos_kv_client::{ClientConfig, HardwareClient, PaxosKvClient, ServiceRegistryClient};
use std::sync::Arc;

impl ChunkIoClient {
    /// Discover services and build lock-free DiskIO routing.
    pub async fn connect(config: ChunkIoClientConfig) -> Result<Self> {
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
            config.management_seeds.clone(),
        )));
        Self::connect_with_kv(config, kv).await
    }

    /// Discover services using an existing KV topology client shared with the
    /// embedding process.
    pub async fn connect_with_kv(config: ChunkIoClientConfig, kv: Arc<PaxosKvClient>) -> Result<Self> {
        Self::connect_with_kv_read_policy(config, kv, ChunkReadPolicy::default()).await
    }

    /// Connect with explicit shared read resource limits.
    pub async fn connect_with_kv_read_policy(
        config: ChunkIoClientConfig,
        kv: Arc<PaxosKvClient>,
        read_policy: ChunkReadPolicy,
    ) -> Result<Self> {
        Self::connect_with_kv_transport(config, kv, read_policy, None).await
    }

    /// Connect using explicit data transport settings; None retains environment defaults.
    pub async fn connect_with_kv_transport(
        config: ChunkIoClientConfig,
        kv: Arc<PaxosKvClient>,
        read_policy: ChunkReadPolicy,
        rdma: Option<crowdb_rpc_ffi::RdmaConfig>,
    ) -> Result<Self> {
        let service = ServiceRegistryClient::from_shared(kv.clone());
        let hardware = HardwareClient::from_shared(kv.clone());
        let range_binding = discover_current_range_bindings(&service, kv.clone()).await?;
        let chunkdb_transport = Arc::new(ChunkdbRpcTransport::new());
        let mut chunkdb = ChunkdbClient::new(service.clone(), Arc::clone(&chunkdb_transport));
        if let Some(range_binding) = range_binding {
            chunkdb = chunkdb.with_range_binding(range_binding);
        }
        let chunkdb = Arc::new(chunkdb);
        chunkdb.refresh_endpoints().await?;
        let disk_writer = Arc::new(
            RoutedDiskWriter::connect_with_config(
                &service,
                &hardware,
                crowdb_diskio_client::DiskioClientConfig {
                    normal_connections_per_endpoint: config.diskio_connections_per_endpoint,
                    rpc_workers: config.diskio_rpc_workers,
                    rdma,
                    ..crowdb_diskio_client::DiskioClientConfig::default()
                },
            )
            .await?,
        );
        let failed_disks = Arc::new(FailedDiskList::new(config.small_write.failed_disk_ttl));
        let large_write_repair = Arc::new(crate::metrics::LargeWriteRepairMetrics::default());
        let large_write_buffer = Arc::new(crate::metrics::LargeWriteBufferMetrics::default());
        let small_pool = SmallWritePool::new(
            chunkdb.clone(),
            disk_writer.clone(),
            config.small_write,
            Arc::new(SmallWriteMetrics::default()),
            Arc::clone(&failed_disks),
        )?;
        let reader = ChunkReader::new(chunkdb.clone(), disk_writer.clone(), read_policy)
            .map_err(|error| crate::IoError::Internal(error.to_string()))?;
        Ok(Self {
            allocator: chunkdb.clone(),
            disk_writer: disk_writer.clone(),
            topology: Some(Arc::new(ClientTopology {
                service,
                hardware,
                chunkdb,
                chunkdb_transport,
                disk_writer,
            })),
            metrics: None,
            readiness: Arc::default(),
            small_pool,
            reader,
            failed_disks,
            large_write_repair,
            large_write_buffer,
        })
    }
}
