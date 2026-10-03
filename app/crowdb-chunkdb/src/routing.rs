// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk ID -> fixed logical slot -> explicitly selected nonzero KV group.

use arc_swap::ArcSwap;
use crowdb_protocol::chunk_slot::{ChunkSlot, ChunkSlotMap, ChunkStorageGroup};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct BindingTable {
    map: Option<ChunkSlotMap<ChunkStorageGroup>>,
    destinations: Vec<Route>,
}

impl BindingTable {
    #[must_use]
    pub fn new(map: ChunkSlotMap<ChunkStorageGroup>) -> Self {
        let destinations = map
            .bindings()
            .iter()
            .filter(|binding| !binding.slots.is_empty())
            .map(|binding| Route::from(binding.owner))
            .collect();
        Self {
            map: Some(map),
            destinations,
        }
    }

    #[must_use]
    pub fn bindings(&self) -> &[Route] {
        &self.destinations
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_none()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.destinations.len()
    }

    fn same_layout(&self, other: &Self) -> bool {
        match (&self.map, &other.map) {
            (Some(left), Some(right)) => {
                left.head() == right.head()
                    && left
                        .bindings()
                        .iter()
                        .all(|binding| right.bindings().contains(binding))
            }
            (None, None) => true,
            _ => false,
        }
    }
}

/// One immutable initialized layout. Refresh may confirm it, never remap it.
#[derive(Clone, Default)]
pub struct BindingCache {
    inner: Arc<ArcSwap<BindingTable>>,
}

impl BindingCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a validated map once; concurrent conflicting installs fail closed.
    ///
    /// # Errors
    /// Returns an error for missing routing or an unsupported placement change.
    pub fn replace(&self, table: BindingTable) -> Result<(), RouteError> {
        if table.is_empty() {
            return Err(RouteError::NoBinding);
        }
        let replacement = Arc::new(table);
        loop {
            let current = self.inner.load_full();
            if !current.is_empty() {
                return if current.same_layout(&replacement) {
                    Ok(())
                } else {
                    Err(RouteError::FixedLayout)
                };
            }
            let previous = self.inner.compare_and_swap(&current, Arc::clone(&replacement));
            if Arc::ptr_eq(&previous, &current) {
                return Ok(());
            }
        }
    }

    #[must_use]
    pub fn route(&self, chunk_id: &ChunkId) -> Option<Route> {
        self.route_slot(ChunkSlot::for_chunk(chunk_id))
    }

    #[must_use]
    pub fn route_slot(&self, slot: ChunkSlot) -> Option<Route> {
        self.inner
            .load()
            .map
            .as_ref()
            .map(|map| Route::from(map.owner(slot)))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }

    #[must_use]
    pub fn snapshot(&self) -> BindingTable {
        (**self.inner.load()).clone()
    }
}

/// Hashing is shared by lifecycle admission and storage placement.
#[must_use]
pub fn hash_to_bucket(id: &ChunkId) -> u16 {
    ChunkSlot::for_chunk(id).value()
}

/// Construct a single-group test fixture; production loads its map from group 0.
#[cfg(feature = "test-util")]
#[must_use]
pub fn default_binding_table(store_id: u64, group_id: u64) -> BindingTable {
    use crowdb_protocol::chunk_slot::ChunkSlotBootstrap;
    BindingTable::new(
        ChunkSlotBootstrap {
            service_instances: vec![1],
            storage_groups: vec![ChunkStorageGroup { store_id, group_id }],
        }
        .storage_map()
        .expect("test storage group must be nonzero"),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("chunk storage slot map is not initialized")]
    NoBinding,
    #[error("chunk storage slot map is fixed; remapping requires migration")]
    FixedLayout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route {
    pub kv_store_id: u64,
    pub kv_group_id: u64,
}

impl From<ChunkStorageGroup> for Route {
    fn from(group: ChunkStorageGroup) -> Self {
        Self {
            kv_store_id: group.store_id,
            kv_group_id: group.group_id,
        }
    }
}

/// Resolve every chunk record and associated task using the same owning ID.
///
/// # Errors
/// Returns an error until the complete storage map is initialized.
pub fn route(cache: &BindingCache, chunk_id: &ChunkId) -> Result<Route, RouteError> {
    cache.route(chunk_id).ok_or(RouteError::NoBinding)
}
