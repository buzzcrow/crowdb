// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared cluster clients and admission lifetime, independent of object handles.

mod attachment;
mod resources;
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
            groups: Arc::new(ArcSwap::from_pointee(vec![Arc::clone(&storage)])),
            storage,
            resources,
        })
    }

    #[must_use]
    pub fn resources(&self) -> &Arc<RuntimeResources> {
        &self.resources
    }

    #[must_use]
    pub fn writer_group_count(&self) -> usize {
        self.groups.load().len()
    }

    /// Closes shared admission and wakes outstanding cancellable operations.
    /// Caller-owned leases retain their backing storage after this call.
    /// # Errors
    /// Returns a writer group's durability/drain failure after closing admission.
    pub async fn shutdown(&self) -> Result<()> {
        self.resources.shutdown();
        let groups = self.groups.load_full();
        let mut failure = None;
        for group in groups.iter() {
            if let Err(error) = group.chunks.shutdown_small_writes().await {
                failure.get_or_insert_with(|| Error::Storage(error.to_string()));
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
