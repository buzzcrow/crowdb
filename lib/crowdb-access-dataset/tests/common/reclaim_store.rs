// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Default)]
pub(super) struct TestReclaimStore {
    inner: dataset_store::TestDatasetStore,
    pub pause_retention: AtomicBool,
    pub observed: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl DatasetStore for TestReclaimStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let value = self.inner.get(key).await?;
        if key.starts_with(b"DS\0R\0") && self.pause_retention.swap(false, Ordering::Relaxed) {
            self.observed.notify_one();
            self.resume.notified().await;
        }
        Ok(value)
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}
pub(super) async fn fixture() -> (
    Arc<TestReclaimStore>,
    Arc<DatasetAuthority>,
    DatasetIdentity,
    SnapshotId,
) {
    let store = Arc::new(TestReclaimStore::default());
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "reclaim-race").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, crate::publication::publication_manifest(1))
        .await
        .unwrap();
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            crate::publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    let attempt = CommitAttempt {
        version: 1,
        id: OperationId::random(),
        target: CommitTarget::Snapshot { snapshot: root },
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    (store, authority, identity, snapshot)
}
