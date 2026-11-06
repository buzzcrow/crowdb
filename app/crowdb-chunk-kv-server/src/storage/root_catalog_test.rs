// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Feature-gated access to the production process-bound root authority.
use super::{KvRootCatalogStore, StorageRuntimeError};
use crowdb_chunk_kv::Partition;
use crowdb_paxos_kv_client::{PaxosKvClient, StreamProcessAuthority};
use crowdb_protocol::chunk_kv::ChunkKvRangeCatalogEntry;
use crowdb_tree_ffi::{CtError, RootCatalogStore};
use std::sync::Arc;

pub struct TestOwnedRootCatalog {
    catalog: Arc<KvRootCatalogStore>,
}
impl TestOwnedRootCatalog {
    #[must_use]
    pub fn store(&self) -> &dyn RootCatalogStore {
        self.catalog.as_ref()
    }
    /// # Errors
    /// Requires matching, actually recovered root and replay frontiers.
    pub async fn complete_recovery(
        &self,
        partition: &Partition,
        root_generation: u64,
    ) -> Result<(), CtError> {
        self.catalog.complete_recovery(partition, root_generation).await
    }

    /// # Errors
    /// Returns native manifest inspection or recovery receipt errors.
    pub async fn complete_tree_recovery_for_tests(
        &self,
        partition: &Partition,
        store: &crowdb_tree_ffi::PageStore,
    ) -> Result<(), StorageRuntimeError> {
        super::ChunkKvStorage::complete_tree_recovery(partition, store, &self.catalog).await
    }
}
/// # Errors
/// Returns identity, process registration or KV availability errors.
pub async fn open_root_catalog_with_process_for_tests(
    kv: Arc<PaxosKvClient>,
    store_id: u64,
    group_id: u64,
    entry: &ChunkKvRangeCatalogEntry,
    process: Arc<StreamProcessAuthority>,
) -> Result<TestOwnedRootCatalog, StorageRuntimeError> {
    let catalog = KvRootCatalogStore::for_assignment(kv, store_id, group_id, entry)
        .await?
        .with_process(Some(process))
        .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?;
    catalog
        .register_writer()
        .await
        .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?;
    Ok(TestOwnedRootCatalog {
        catalog: Arc::new(catalog),
    })
}
