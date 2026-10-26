// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use async_trait::async_trait;
use crowdb_access_dataset::{
    CasOutcome, DatasetAuthority, DatasetIdentity, DatasetStore, NamespacePath, StoreError, StoredValue,
};
use std::sync::Arc;
struct TestReadStore {
    inner: upload::TestStore,
    denied: arc_swap::ArcSwapOption<Vec<u8>>,
}
#[async_trait]
impl DatasetStore for TestReadStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        if self
            .denied
            .load()
            .as_ref()
            .is_some_and(|denied| denied.as_slice() == key)
        {
            return Err(StoreError::Rejected);
        }
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: crowdb_access_dataset::MetadataScanBudget,
        cancel: &crowdb_access_dataset::ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get_bounded(key, budget, cancel).await
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
#[tokio::test]
async fn complete_projection_does_not_fetch_unused_ancestry() {
    let store = Arc::new(TestReadStore {
        inner: upload::TestStore::default(),
        denied: arc_swap::ArcSwapOption::empty(),
    });
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "projection").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let parent = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(parent), publication::publication_manifest(2))
        .await
        .unwrap();
    store.denied.store(Some(Arc::new({
        let mut key = b"DS\0S\0".to_vec();
        key.extend(identity.key().unwrap());
        key.extend(parent.as_bytes());
        key
    })));
    let values = authority
        .resolve_batch_locators(&identity, child, &[b"sample-2".to_vec()], &["marker".into()])
        .await
        .unwrap();
    assert_eq!(values.len(), 1);
    assert!(values[0].fields.contains_key("marker"));
}
