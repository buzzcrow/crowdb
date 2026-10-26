// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared cluster clients and admission lifetime, independent of object handles.

mod attachment;
mod resources;
mod shutdown;
mod storage;
mod writer_policy;

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crowdb_chunk_client::{ChunkReadPolicy, SmallWritePolicy};
use crowdb_chunk_kv_client::ChunkKvRpcTransportConfig;
use crowdb_protocol::chunkdb::rpc::ChunkType;

use crate::{Error, Result};

pub use attachment::RuntimeClients;
pub use resources::{RequestLease, ResourceStatus, RuntimeLimits, RuntimeResources};
pub(crate) use storage::Storage;

pub struct RuntimeConfig {
    pub management_seeds: Vec<String>,
    pub diskio_connections_per_endpoint: usize,
    pub diskio_rpc_workers: u32,
    pub small_write: SmallWritePolicy,
    pub read_policy: ChunkReadPolicy,
    pub chunk_kv_transport: ChunkKvRpcTransportConfig,
    /// None retains service environment defaults; Some applies before data connections open.
    pub data_rdma: Option<crowdb_rpc_ffi::RdmaConfig>,
    pub limits: RuntimeLimits,
    pub connect_timeout: Duration,
}

impl RuntimeConfig {
    #[must_use]
    pub fn new(management_seeds: Vec<String>) -> Self {
        Self {
            management_seeds,
            diskio_connections_per_endpoint: 1,
            diskio_rpc_workers: 1,
            small_write: SmallWritePolicy::new(ChunkType::S3),
            read_policy: ChunkReadPolicy::default(),
            chunk_kv_transport: ChunkKvRpcTransportConfig::default(),
            data_rdma: None,
            limits: RuntimeLimits::default(),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
pub struct ClientRuntime {
    pub(crate) storage: Arc<Storage>,
    pub(crate) resources: Arc<RuntimeResources>,
    pub(crate) groups: Arc<ArcSwap<Vec<Arc<Storage>>>>,
    shutdown: Arc<shutdown::ShutdownState>,
}

impl ClientRuntime {
    /// Connects discovery and storage without an Access Server listener or
    /// credential lookup. Business handles clone these same client owners.
    /// # Errors
    /// Returns invalid configuration or initial discovery/connection failures.
    pub async fn connect(config: RuntimeConfig) -> Result<Self> {
        let resources = RuntimeResources::new(config.limits)?;
        if config.management_seeds.is_empty() {
            return Err(Error::Configuration("management endpoints are empty".into()));
        }
        let timeout = config.connect_timeout;
        if timeout.is_zero() {
            return Err(Error::Configuration("connect timeout must be positive".into()));
        }
        let storage = Arc::new(
            tokio::time::timeout(timeout, Storage::connect(config))
                .await
                .map_err(|_| Error::Deadline)??,
        );
        Ok(Self {
            shutdown: Arc::new(shutdown::ShutdownState::default()),
            groups: Arc::new(ArcSwap::from_pointee(vec![Arc::clone(&storage)])),
            storage,
            resources,
        })
    }

    #[must_use]
    pub fn resources(&self) -> &Arc<RuntimeResources> {
        &self.resources
    }

    /// Snapshots cumulative layout, streaming-credit and decode work on the shared reader.
    /// Writer-policy groups share these counters; snapshots perform no RPC or reset.
    #[must_use]
    pub fn read_flow_metrics(&self) -> crowdb_chunk_client::ReadFlowMetricsSnapshot {
        self.storage.chunks.read_flow_metrics()
    }

    /// Allocates unique producer storage from this runtime's data transport.
    /// # Errors
    /// Returns unsupported allocation, exhausted actual-capacity budget or shutdown.
    pub fn allocate_producer(&self, capacity: u32) -> Result<crate::buffer::WritableLease> {
        self.resources.check_open()?;
        let allocator =
            self.storage.chunks.producer_allocator().ok_or_else(|| {
                Error::Configuration("data transport has no native producer allocator".into())
            })?;
        self.resources.allocate(&allocator, capacity)
    }

    #[must_use]
    pub fn writer_group_count(&self) -> usize {
        self.groups.load().len()
    }
}
