// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{common::TestStore, creation::TestCreation};
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    gc::{GcScan, GcStore, GcSystemScan},
    key::{IcebergKey, OperationId, StorageKey},
    operation::RequestIdentity,
    record::StorageRecord,
    table::{
        storage_head_key, TableHead, TableLifecycle, TableLifecycleAction, TableLifecycleRequest,
        TablePurgeTask, TableRepository,
    },
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

pub struct TestDrop {
    pub creation: TestCreation,
    pub request: TableLifecycleRequest,
    pub before: TableHead,
}

impl TestDrop {
    pub async fn new(purge: bool) -> Self {
        let creation = TestCreation::new().await;
        creation.creator().create(&creation.request).await.unwrap();
        let before = TableRepository::new(creation.fixture.store.clone())
            .select(creation.fixture.context, creation.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap()
            .head;
        let request = TableLifecycleRequest {
            context: creation.fixture.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            namespace: creation.parent.identifier.clone(),
            name: "events".into(),
            action: TableLifecycleAction::Drop {
                purge_requested: purge,
            },
        };
        Self {
            creation,
            request,
            before,
        }
    }

    pub fn store(&self) -> Arc<TestStore> {
        self.creation.fixture.store.clone()
    }

    pub async fn head(&self) -> TableHead {
        let key = storage_head_key(self.before.storage_location());
        let value = self.store().get(&key.encode().unwrap()).await.unwrap().unwrap();
        let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
            panic!("head");
        };
        *head
    }

    pub fn marker(&self) -> TablePurgeTask {
        self.store()
            .values
            .load()
            .iter()
            .find_map(|(key, value)| {
                let key = IcebergKey::decode(key).ok()?;
                match StorageRecord::decode(&key, &value.bytes).ok()? {
                    StorageRecord::TablePurgeTask(marker) if marker.head.table == self.before.table => {
                        Some(*marker)
                    }
                    _ => None,
                }
            })
            .unwrap()
    }

    pub async fn selected(&self) -> Option<TableHead> {
        TableRepository::new(self.store())
            .select(self.request.context, self.creation.parent.namespace, "events")
            .await
            .unwrap()
            .map(|selected| selected.head)
    }

    pub async fn recreate(&self) -> TableHead {
        let mut request = self.creation.request.clone();
        request.identity.operation = OperationId::random();
        self.creation.creator().create(&request).await.unwrap();
        self.selected().await.unwrap()
    }
}

pub struct TestPurgeStore {
    pub inner: Arc<TestStore>,
    pub pause_fence: AtomicBool,
    pub entered: Notify,
    pub release: Notify,
    pub reads: AtomicUsize,
    pub mutations: AtomicUsize,
}

impl TestPurgeStore {
    pub fn new(inner: Arc<TestStore>) -> Self {
        Self {
            inner,
            pause_fence: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            reads: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
        }
    }
    pub fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::SeqCst),
            self.mutations.load(Ordering::SeqCst),
        )
    }
}

#[async_trait]
impl CatalogStore for TestPurgeStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        bytes: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        if expected.is_some()
            && matches!(StorageKey::decode(key).and_then(|key| StorageRecord::decode(&key, bytes)),
            Ok(StorageRecord::TableHead(head)) if head.lifecycle == TableLifecycle::Ready)
            && self.pause_fence.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.compare_exchange(key, expected, bytes, identity).await
    }
}

#[async_trait]
impl GcStore for TestPurgeStore {
    async fn scan_gc(&self, scan: GcScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc(scan).await
    }
    async fn scan_gc_system(&self, scan: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc_system(scan).await
    }
    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_gc_record(key, expected, identity).await
    }
}
