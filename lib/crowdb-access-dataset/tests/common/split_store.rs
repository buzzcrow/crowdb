// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(super) struct TestDraftStore {
    inner: dataset_store::TestDatasetStore,
    pub lose_edit_reply: AtomicBool,
}
#[async_trait::async_trait]
impl DatasetStore for TestDraftStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
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
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if key.starts_with(b"DS\0SD\0")
            && expected.is_some()
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_edit_reply.swap(false, Ordering::Relaxed)
        {
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
}

pub(super) async fn fixture() -> (
    std::sync::Arc<TestDraftStore>,
    DatasetAuthority,
    DatasetIdentity,
    SnapshotId,
) {
    let store = std::sync::Arc::new(TestDraftStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "drafts").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, crate::publication::publication_manifest(1))
        .await
        .unwrap();
    (store, authority, identity, snapshot)
}
