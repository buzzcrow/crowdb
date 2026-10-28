// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arc_swap::ArcSwap;
use crowdb_chunk_client::ChunkIoClient;
use crowdb_chunk_kv_client::{ChunkKvClient, ChunkKvRpcTransport};
use crowdb_paxos_kv_client::PaxosKvClient;

use super::{ClientRuntime, RuntimeLimits, RuntimeResources, Storage};
use crate::topology::CatalogObservation;
use crate::{Error, Result};

/// Existing client owners attached by an embedding system. The catalog must
/// already be validated; this path never reconnects or creates HTTP listeners.
pub struct RuntimeClients {
    pub control: Arc<PaxosKvClient>,
    pub metadata: Arc<ChunkKvClient>,
    pub metadata_transport: Arc<ChunkKvRpcTransport>,
    pub chunks: Arc<ChunkIoClient>,
}

impl ClientRuntime {
    /// Borrows client groups; SDK shutdown never stops their original owners.
    /// # Errors
    /// Rejects invalid resource limits or a missing initial catalog observation.
    pub fn from_clients(clients: RuntimeClients, limits: RuntimeLimits) -> Result<Self> {
        Self::attach_clients(clients, limits, false)
    }

    /// Transfers shutdown responsibility for these client groups to the runtime.
    /// All external Arc clones of the adopted transports close with this runtime.
    /// # Errors
    /// Rejects invalid limits or a missing initial catalog observation.
    pub fn adopt_clients(clients: RuntimeClients, limits: RuntimeLimits) -> Result<Self> {
        Self::attach_clients(clients, limits, true)
    }

    fn attach_clients(clients: RuntimeClients, limits: RuntimeLimits, owned: bool) -> Result<Self> {
        let catalog = clients
            .metadata
            .cached_catalog()
            .ok_or_else(|| Error::Configuration("attached metadata catalog is not initialized".into()))?;
        let policy = clients.chunks.small_write_policy().as_ref().clone();
        let storage = Arc::new(Storage {
            owns_small_pool: owned,
            owns_transports: owned,
            control: clients.control,
            metadata: clients.metadata,
            chunks: clients.chunks,
            metadata_transport: clients.metadata_transport,
            chunk_type: policy.chunk_type,
            policy,
            catalog: Arc::new(ArcSwap::from_pointee(CatalogObservation {
                catalog,
                observed_at: None,
            })),
            services: Arc::new(ArcSwap::from_pointee(Vec::new())),
        });
        Ok(Self {
            shutdown: Arc::new(super::shutdown::ShutdownState::default()),
            groups: Arc::new(ArcSwap::from_pointee(vec![Arc::clone(&storage)])),
            storage,
            resources: RuntimeResources::new(limits)?,
        })
    }
}
