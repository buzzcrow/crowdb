// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/upload.rs"]
mod upload;
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{
    CasOutcome, DatasetIdentity, DatasetStore, DatasetUploads, NamespacePath, OperationId, StoreError,
    StoredValue,
};
use md5::{Digest, Md5};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;
#[derive(Default)]
struct TestRegistryStore {
    inner: upload::TestStore,
    pause: AtomicBool,
    entered: Notify,
    resume: Notify,
}
#[async_trait]
impl DatasetStore for TestRegistryStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        if key.starts_with(b"DS\0UPAGE\0") {
            let count = |bytes: &[u8]| {
                serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["entries"]
                    .as_array()
                    .unwrap()
                    .len()
            };
            if expected.is_some_and(|old| count(old) > count(value))
                && self.pause.swap(false, Ordering::AcqRel)
            {
                self.entered.notify_one();
                self.resume.notified().await;
            }
        }
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}
#[tokio::test]
async fn untracking_old_registration_cannot_remove_a_new_cleanup_registration() {
    let store = Arc::new(TestRegistryStore::default());
    let uploads = Arc::new(DatasetUploads::new(
        store.clone(),
        Arc::new(upload::TestPayload::default()),
    ));
    let identity = DatasetIdentity::new(NamespacePath::root(), "registry").unwrap();
    let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
    uploads
        .create(&location, 4, 4, Md5::digest(b"data").into(), 10)
        .await
        .unwrap();
    uploads
        .put_part(&location, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    uploads.complete(&location, 2).await.unwrap();
    let owner = OperationId::random();
    uploads.claim(&location, owner, 3).await.unwrap();
    store.pause.store(true, Ordering::Release);
    let old = uploads.clone();
    let key = location.clone();
    let untrack = tokio::spawn(async move { old.untrack(&key).await });
    store.entered.notified().await;
    uploads.release(&location, owner).await.unwrap();
    store.resume.notify_one();
    untrack.await.unwrap().unwrap();
    uploads.expire(20).await.unwrap();
    assert!(uploads.get(&location).await.unwrap().cleanup_complete);
}
