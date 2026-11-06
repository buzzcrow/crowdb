// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One fixed process lease covers tree writers and independent read windows.
use super::{ChunkKvStorage, KvRootCatalogStore, StorageRuntimeError};
use crowdb_chunk_kv::Partition;
use crowdb_paxos_kv_client::StreamProcessAuthority;
use crowdb_tree_ffi::{ChunkPageStoreOptions, ChunkRootCatalog, CtError, PageStore};
use std::sync::Arc;

impl ChunkKvStorage {
    pub(super) fn tree_config(&self) -> crowdb_tree_ffi::Config {
        crowdb_tree_ffi::Config {
            runtime: Some(self.tree_runtime.clone()),
            ..self.tree_options.clone()
        }
    }

    /// Bind the same fixed process authority used by serving grants and WAL.
    /// Must run before opening any hosted tree.
    /// # Errors
    /// Rejects a conflicting process binding or invalid WAL authority.
    pub fn bind_owner_process(
        &self,
        process: Arc<StreamProcessAuthority>,
    ) -> Result<(), StorageRuntimeError> {
        self.streams
            .registry()
            .bind_owner_process(process.clone())
            .map_err(|error| StorageRuntimeError::Stream(error.to_string()))?;
        let expected = process.clone();
        let previous = self
            .owner_process
            .compare_and_swap(&None::<Arc<StreamProcessAuthority>>, Some(process));
        if previous.as_ref().is_some_and(|old| !Arc::ptr_eq(old, &expected)) {
            return Err(StorageRuntimeError::Tree(
                "tree process authority is already bound".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn bind_catalog(
        &self,
        catalog: KvRootCatalogStore,
    ) -> Result<Arc<KvRootCatalogStore>, StorageRuntimeError> {
        let process = Some(
            self.owner_process
                .load_full()
                .ok_or_else(|| StorageRuntimeError::Tree("tree process authority is not bound".into()))?,
        );
        catalog
            .with_process(process)
            .map(Arc::new)
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))
    }

    pub(super) async fn open_durable_tree(
        &self,
        options: ChunkPageStoreOptions,
        metadata_group_id: u64,
    ) -> Result<(Arc<PageStore>, Arc<KvRootCatalogStore>), StorageRuntimeError> {
        let catalog = KvRootCatalogStore::claim(
            self.kv.clone(),
            self.metadata_store_id,
            metadata_group_id,
            options.tree_id,
            options.owner_epoch,
        )
        .await?;
        let catalog = self.bind_catalog(catalog)?;
        let store = self.open_tree_page_store(
            options,
            Arc::new(
                ChunkRootCatalog::open_callback(catalog.clone())
                    .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?,
            ),
        )?;
        catalog
            .register_writer()
            .await
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?;
        Ok((store, catalog))
    }

    pub(super) async fn open_assignment_tree(
        &self,
        entry: &crowdb_protocol::chunk_kv::ChunkKvRangeCatalogEntry,
        metadata_group_id: u64,
    ) -> Result<(Arc<PageStore>, Arc<KvRootCatalogStore>), StorageRuntimeError> {
        let open_generation = entry
            .artifact
            .tail_overlay
            .as_ref()
            .map_or(0, |overlay| overlay.base_root_manifest_generation);
        let options = ChunkPageStoreOptions {
            tree_id: entry.artifact.tree_id,
            owner_epoch: entry.owner_epoch,
            open_generation,
            pack_bytes: 0,
            iu_size: 0,
            max_concurrent_packs: 0,
            materialization_bytes_per_pass: 0,
            mirror_copies: 0,
            max_chunk_bytes: 0,
        };
        let catalog = KvRootCatalogStore::for_assignment(
            self.kv.clone(),
            self.metadata_store_id,
            metadata_group_id,
            entry,
        )
        .await
        .map_err(|error| {
            tracing::warn!(tree_id = entry.artifact.tree_id, error = %error, "assignment root claim failed");
            error
        })?;
        let catalog = self.bind_catalog(catalog)?;
        let page_store = self.open_tree_page_store(
            options,
            Arc::new(
                ChunkRootCatalog::open_callback(catalog.clone())
                    .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?,
            ),
        ).map_err(|error| {
            tracing::warn!(tree_id = entry.artifact.tree_id, error = %error, "assignment PageStore open failed");
            error
        })?;
        catalog
            .register_writer()
            .await
            .map_err(|error| {
                tracing::warn!(tree_id = entry.artifact.tree_id, error = %error, "assignment writer registration failed");
                StorageRuntimeError::Tree(error.to_string())
            })?;
        Ok((page_store, catalog))
    }

    pub(super) async fn complete_tree_recovery(
        partition: &Partition,
        store: &PageStore,
        catalog: &KvRootCatalogStore,
    ) -> Result<(), StorageRuntimeError> {
        // Each caller has already awaited root and WAL recovery. A prepared
        // split retains an overlay artifact but has no live transfer catch-up
        // request to complete its separate initialization future.
        let generation = match store.chunk_manifest_generation() {
            Ok(value) => value,
            Err(CtError::NotFound) => 0,
            Err(error) => return Err(StorageRuntimeError::Tree(error.to_string())),
        };
        catalog
            .complete_recovery(partition, generation)
            .await
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))
    }
}

#[cfg(feature = "test-util")]
impl ChunkKvStorage {
    /// Assemble the production runtime backend with already connected test services.
    /// # Errors
    /// Returns native route, shared host budget, or storage initialization errors.
    pub async fn from_parts_parallel_for_tests(
        kv: Arc<crowdb_paxos_kv_client::PaxosKvClient>,
        chunk_io: crowdb_chunk_client::ChunkIoClient,
        metadata_store_id: u64,
        writer_lease_ms: u64,
        mirrors: u32,
    ) -> Result<Self, StorageRuntimeError> {
        Self::from_parts_with_options(
            kv,
            chunk_io,
            metadata_store_id,
            writer_lease_ms,
            super::TreeStorageOptions {
                mirror_copies: mirrors,
                chunk_capacity_bytes: 256 * 1024 * 1024,
                strip_capacity_bytes: 0,
                ..super::TreeStorageOptions::default()
            },
            crowdb_chunk_stream::StreamConfig::default(),
            crowdb_chunk_client::ChunkReadPolicy::default(),
        )
        .await
    }
}
