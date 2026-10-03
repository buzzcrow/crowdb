// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Client-to-service slot routing. Storage placement is never read here.

use crate::{ChunkSlotMapClient, CrowdbKvClient, Error, Result, ServiceRegistryClient};
use arc_swap::ArcSwapOption;
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotBitmap, ChunkSlotMap};
use crowdb_protocol::common::ChunkId;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkdbRangeBinding {
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub generation: u64,
    pub slots: ChunkSlotBitmap,
}

#[derive(Debug, thiserror::Error)]
pub enum RangeRouteError {
    #[error("chunk service slot map is not initialized")]
    NoBinding,
    #[error("chunk service owner {0} has no live endpoint")]
    NoEndpoint(u64),
    #[error("binding refresh failed: {0}")]
    Refresh(String),
}

struct ServiceSnapshot {
    map: ChunkSlotMap<u64>,
    bindings: HashMap<u64, ChunkdbRangeBinding>,
}

pub struct RangeBindingClient {
    kv: Arc<CrowdbKvClient>,
    snapshot: ArcSwapOption<ServiceSnapshot>,
}

impl RangeBindingClient {
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbKvClient>) -> Self {
        Self {
            kv,
            snapshot: ArcSwapOption::empty(),
        }
    }

    #[must_use]
    pub fn kv(&self) -> &CrowdbKvClient {
        &self.kv
    }

    /// Refresh live endpoints without changing initialized slot authority.
    ///
    /// # Errors
    /// Rejects incomplete layouts, storage-independent service read failures,
    /// or unsupported changes to the fixed service assignment.
    pub async fn refresh(&self) -> Result<()> {
        let map = ChunkSlotMapClient::new(Arc::clone(&self.kv))
            .read_service()
            .await?;
        let endpoints: HashMap<_, _> = ServiceRegistryClient::from_shared(Arc::clone(&self.kv))
            .read_all_chunkdb_instances()
            .await?
            .into_iter()
            .map(|(id, instance)| (id, instance.rpc_endpoint))
            .collect();
        let bindings = map
            .bindings()
            .iter()
            .map(|binding| {
                (
                    binding.owner,
                    ChunkdbRangeBinding {
                        instance_id: binding.owner,
                        rpc_endpoint: endpoints.get(&binding.owner).cloned().unwrap_or_default(),
                        generation: map.head().generation,
                        slots: binding.slots.clone(),
                    },
                )
            })
            .collect();
        let replacement = Arc::new(ServiceSnapshot { map, bindings });
        loop {
            let current = self.snapshot.load_full();
            if let Some(current) = &current {
                if current.map.head() != replacement.map.head()
                    || current
                        .map
                        .bindings()
                        .iter()
                        .any(|binding| !replacement.map.bindings().contains(binding))
                {
                    return Err(Error::SysdataDecode {
                        key: "/chunkdb/slot_service/".into(),
                        reason: "service slot assignment is fixed; handoff is required".into(),
                    });
                }
            }
            let previous = self
                .snapshot
                .compare_and_swap(&current, Some(Arc::clone(&replacement)));
            let unchanged = match (&*previous, &current) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            };
            if unchanged {
                return Ok(());
            }
        }
    }

    /// Resolve an existing chunk using only the service map.
    ///
    /// # Errors
    /// Returns a layout/refresh error or an unavailable-owner error.
    pub async fn route(
        &self,
        chunk_id: &ChunkId,
    ) -> std::result::Result<ChunkdbRangeBinding, RangeRouteError> {
        if self.is_empty() {
            self.refresh()
                .await
                .map_err(|error| RangeRouteError::Refresh(error.to_string()))?;
        }
        self.route_slot(ChunkSlot::for_chunk(chunk_id))
    }

    /// Refresh after a rejected or stale-endpoint request, then resolve its owner.
    ///
    /// # Errors
    /// Returns a layout/refresh error or an unavailable-owner error.
    pub async fn refresh_and_route(
        &self,
        chunk_id: &ChunkId,
    ) -> std::result::Result<ChunkdbRangeBinding, RangeRouteError> {
        self.refresh()
            .await
            .map_err(|error| RangeRouteError::Refresh(error.to_string()))?;
        self.route_slot(ChunkSlot::for_chunk(chunk_id))
    }

    /// Resolve a validated slot without locking or reading the storage map.
    ///
    /// # Errors
    /// Rejects missing initialization or a slot owner without a live endpoint.
    pub fn route_slot(&self, slot: ChunkSlot) -> std::result::Result<ChunkdbRangeBinding, RangeRouteError> {
        let snapshot = self.snapshot.load();
        let snapshot = snapshot.as_ref().ok_or(RangeRouteError::NoBinding)?;
        let owner = snapshot.map.owner(slot);
        let binding = snapshot
            .bindings
            .get(&owner)
            .ok_or(RangeRouteError::NoEndpoint(owner))?;
        if binding.rpc_endpoint.is_empty() {
            return Err(RangeRouteError::NoEndpoint(owner));
        }
        Ok(binding.clone())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.snapshot.load().is_none()
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<ChunkdbRangeBinding> {
        let mut bindings: Vec<_> = self
            .snapshot
            .load()
            .as_ref()
            .map_or_else(Vec::new, |snapshot| snapshot.bindings.values().cloned().collect());
        bindings.sort_by_key(|binding| binding.instance_id);
        bindings
    }
}
