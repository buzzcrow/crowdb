// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_iceberg::file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError};
use crowdb_chunk_client::ReclaimOutcome;

#[path = "iceberg_gc_scheduler/store.rs"]
mod store;
use crowdb_access_iceberg::{
    catalog::{ActiveCatalogRecord, CatalogAuthority, CatalogContext, CatalogStore, RootState},
    file::TableLocation,
    gc::{GcLimits, GcRepository, GcTask},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, NamespaceId, OperationId, SystemScope, TableId},
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableHead, TableLifecycle, TablePurgeTask},
};
use crowdb_access_server::iceberg::TestGcSweep;
use std::sync::Arc;
pub use store::TestStore;

#[path = "iceberg_gc_scheduler/timing.rs"]
pub mod timing;

pub struct TestBlocks;

#[async_trait]
impl FileBlockStore for TestBlocks {
    async fn put(&self, _: FileIdentity, _: u8, _: &[u8]) -> Result<ChunkRoot, FileIoError> {
        Err(FileIoError::Bounds)
    }
    async fn read(&self, _: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        Err(FileIoError::Bounds)
    }
    async fn reclaim(&self, _: &ChunkRoot) -> Result<ReclaimOutcome, FileIoError> {
        Err(FileIoError::Bounds)
    }
}

pub async fn put(store: &TestStore, key: &[u8], bytes: &[u8]) {
    store
        .compare_exchange(key, None, bytes, mutation_identity(key, None, bytes))
        .await
        .unwrap();
}

pub fn scheduler(store: Arc<TestStore>, context: CatalogContext) -> TestGcSweep {
    TestGcSweep::new(
        store,
        Arc::new(TestBlocks),
        context.catalog,
        Some(context),
        GcLimits {
            page_items: 1,
            ..GcLimits::default()
        },
    )
    .unwrap()
}

pub async fn advance(sweep: &mut TestGcSweep, windows: usize) {
    for _ in 0..windows {
        if let Err(error) = sweep.advance_for_tests().await {
            eprintln!("scheduler retained error: {error}");
        }
    }
}

pub async fn assert_progress(store: Arc<TestStore>, before: &GcTask) {
    let current = GcRepository::new(store)
        .task(before.context.catalog, before.identity)
        .await
        .unwrap()
        .unwrap();
    assert!(
        current.revision > before.revision,
        "healthy task did not advance: {current:?}"
    );
}

pub async fn purge(store: &TestStore, context: CatalogContext, identity: u8) -> TablePurgeTask {
    let table = TableId::from_bytes(&[identity; 16]).unwrap();
    let metadata_file = FileId::random();
    let marker = TablePurgeTask {
        activation_epoch: context.activation_epoch,
        dropped_ms: 1000,
        head: TableHead {
            catalog: context.catalog,
            table,
            namespace: NamespaceId::random(),
            name: "dropped".into(),
            name_epoch: 1,
            lifecycle: TableLifecycle::Tombstone,
            generation: 1,
            metadata_file,
            metadata_location: TableLocation {
                catalog: context.catalog,
                table,
                storage_namespace: None,
            }
            .file(&format!("metadata/1-{metadata_file}.metadata.json"))
            .unwrap(),
            metadata_digest: [1; 32],
            commit_binding: None,
            format_version: 1,
            table_uuid: None,
            operation_fence: 1,
            pending_operation: Some(OperationId::random()),
        },
    };
    put(
        store,
        &storage_head_key(marker.head.storage_location()).encode().unwrap(),
        &StorageRecord::TableHead(Box::new(marker.head.clone()))
            .encode()
            .unwrap(),
    )
    .await;
    put(
        store,
        &marker.key().encode().unwrap(),
        &StorageRecord::TablePurgeTask(Box::new(marker.clone()))
            .encode()
            .unwrap(),
    )
    .await;
    marker
}

pub fn purge_task_key(marker: &TablePurgeTask) -> Vec<u8> {
    IcebergKey::Catalog {
        catalog: marker.head.catalog,
        scope: CatalogScope::GcTask,
        suffix: marker.head.table.as_bytes().to_vec(),
    }
    .encode()
    .unwrap()
}

pub async fn setup() -> (Arc<TestStore>, CatalogContext, GcTask) {
    let store = Arc::new(TestStore::default());
    let context = CatalogContext {
        catalog: CatalogId::random(),
        activation_epoch: 1,
    };
    for (key, record) in [
        (
            IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                suffix: Vec::new(),
            },
            StorageRecord::Active(ActiveCatalogRecord {
                context,
                operation: OperationId::random(),
                state: RootState::Ready,
            }),
        ),
        (
            IcebergKey::Catalog {
                catalog: context.catalog,
                scope: CatalogScope::Authority,
                suffix: Vec::new(),
            },
            StorageRecord::Authority(CatalogAuthority::new(context.catalog, "current".into()).unwrap()),
        ),
    ] {
        let key = key.encode().unwrap();
        let bytes = record.encode().unwrap();
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
    }
    store.inner.reset_counts();
    let task = GcRepository::new(store.clone())
        .admit_multipart(context, 1000, GcLimits::default())
        .await
        .unwrap();
    assert!(store.inner.counts().0 > 0);
    (store, context, task)
}
