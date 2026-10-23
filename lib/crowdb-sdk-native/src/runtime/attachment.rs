// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arc_swap::ArcSwap;
use crowdb_chunk_client::ChunkIoClient;
use crowdb_chunk_kv_client::{ChunkKvClient, ChunkKvRpcTransport};
use crowdb_kv_client::CrowdbKvClient;

use super::{ClientRuntime, RuntimeLimits, RuntimeResources, Storage};
use crate::topology::CatalogObservation;
use crate::{Error, Result};

/// Existing client owners attached by an embedding system. The catalog must
/// already be validated; this path never reconnects or creates HTTP listeners.
pub struct RuntimeClients {
    pub control: Arc<CrowdbKvClient>,
    pub metadata: Arc<ChunkKvClient>,
    pub metadata_transport: Arc<ChunkKvRpcTransport>,
    pub chunks: Arc<ChunkIoClient>,
}

impl ClientRuntime {
    /// # Errors
    /// Rejects invalid resource limits or a missing initial catalog observation.
    pub fn from_clients(clients: RuntimeClients, limits: RuntimeLimits) -> Result<Self> {
        let catalog = clients
            .metadata
            .cached_catalog()
            .ok_or_else(|| Error::Configuration("attached metadata catalog is not initialized".into()))?;
        let policy = clients.chunks.small_write_policy().as_ref().clone();
        let storage = Arc::new(Storage {
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
            groups: Arc::new(ArcSwap::from_pointee(vec![Arc::clone(&storage)])),
            storage,
            resources: RuntimeResources::new(limits)?,
        })
    }
}
