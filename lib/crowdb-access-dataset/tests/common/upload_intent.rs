// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::upload::{TestPayload, TestStore};
use bytes::Bytes;
use crowdb_access_dataset::{
    CasOutcome, DatasetIdentity, DatasetStore, DatasetUploads, NamespacePath, OperationId, PayloadDurability,
    PayloadStorage, StoreError, StoredValue, UploadError,
};
use crowdb_chunk_client::SmallWriteIntent;
use crowdb_protocol::chunkdb::rpc::Location;
use md5::{Digest, Md5};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestFault {
    None,
    AbortBeforeIntent,
    AbortAfterIntent,
    ReclaimBeforeIntent,
    UnknownIntent,
    AbortBeforeAccept,
}
pub struct TestPartStore {
    pub inner: Arc<TestStore>,
    pub location: Vec<u8>,
    pub part: Vec<u8>,
    fault: TestFault,
    fired: AtomicBool,
    unknown: AtomicBool,
    pub reads: AtomicUsize,
    pub mutations: AtomicUsize,
}
impl TestPartStore {
    async fn replace(&self, key: &[u8], state: &str) {
        let stored = self.inner.get(key).await.unwrap().unwrap();
        let mut record: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
        record["state"] = state.into();
        assert!(matches!(
            self.inner
                .compare_exchange(key, Some(&stored.bytes), &serde_json::to_vec(&record).unwrap())
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
}
#[async_trait::async_trait]
impl DatasetStore for TestPartStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(!self.unknown.load(Ordering::Relaxed), "GET after uncertain CAS");
        let read = self.reads.fetch_add(1, Ordering::Relaxed) + 1;
        if self.fault == TestFault::AbortBeforeAccept && read == 4 {
            assert_eq!(key, self.location);
            self.fired.store(true, Ordering::Relaxed);
            self.replace(&self.location, "aborted").await;
        }
        self.inner.get(key).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        assert!(
            !self.unknown.load(Ordering::Relaxed),
            "mutation after uncertain CAS"
        );
        self.mutations.fetch_add(1, Ordering::Relaxed);
        let intent = key == self.part
            && serde_json::from_slice::<serde_json::Value>(value).unwrap()["state"] == "Intent";
        let inject =
            intent && self.fault != TestFault::AbortBeforeAccept && !self.fired.swap(true, Ordering::Relaxed);
        if inject
            && matches!(
                self.fault,
                TestFault::AbortBeforeIntent | TestFault::ReclaimBeforeIntent
            )
        {
            self.replace(&self.location, "aborted").await;
            if self.fault == TestFault::ReclaimBeforeIntent {
                self.replace(&self.part, "Reclaimed").await;
            }
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if inject {
            assert_eq!(key, self.part);
            if self.fault == TestFault::AbortAfterIntent {
                self.replace(&self.location, "aborted").await;
            }
            if self.fault == TestFault::UnknownIntent {
                assert!(matches!(result, CasOutcome::Applied(_)));
                self.unknown.store(true, Ordering::Relaxed);
                return Err(StoreError::Rejected);
            }
        }
        Ok(result)
    }
}
#[derive(Default)]
pub struct TestIntentPayload {
    inner: TestPayload,
    pub callbacks: Arc<AtomicUsize>,
    pub rejections: Arc<AtomicUsize>,
    pub physical_writes: AtomicUsize,
}
struct TestObservedIntent {
    inner: Arc<dyn SmallWriteIntent>,
    callbacks: Arc<AtomicUsize>,
    rejections: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl SmallWriteIntent for TestObservedIntent {
    async fn before_write(&self, location: &Location) -> crowdb_chunk_client::Result<()> {
        self.callbacks.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.before_write(location).await;
        if result.is_err() {
            self.rejections.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}
#[async_trait::async_trait]
impl PayloadStorage for TestIntentPayload {
    async fn write(
        &self,
        key: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        let locations = self
            .inner
            .write(
                key,
                data,
                Arc::new(TestObservedIntent {
                    inner: intent,
                    callbacks: self.callbacks.clone(),
                    rejections: self.rejections.clone(),
                }),
            )
            .await?;
        self.physical_writes.fetch_add(1, Ordering::Relaxed);
        Ok(locations)
    }
    async fn read(&self, locations: &[Location]) -> Result<Bytes, UploadError> {
        self.inner.read(locations).await
    }
    async fn durability(&self, locations: &[Location]) -> Result<PayloadDurability, UploadError> {
        self.inner.durability(locations).await
    }
    async fn discard(&self, locations: &[Location]) -> Result<bool, UploadError> {
        self.inner.discard(locations).await
    }
}
pub async fn fixture(fault: TestFault) -> (Arc<TestPartStore>, Arc<TestIntentPayload>, DatasetUploads) {
    let inner = Arc::new(TestStore::default());
    let payload = Arc::new(TestIntentPayload::default());
    let location = DatasetUploads::location(
        &DatasetIdentity::new(NamespacePath::root(), "intent-cost").unwrap(),
        OperationId::random(),
    )
    .unwrap();
    DatasetUploads::new(inner.clone(), payload.clone())
        .create(&location, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    let mut part = location.clone();
    part.extend_from_slice(b"\0P\0\0\0\0");
    let store = Arc::new(TestPartStore {
        inner,
        location,
        part,
        fault,
        fired: AtomicBool::new(false),
        unknown: AtomicBool::new(false),
        reads: AtomicUsize::new(0),
        mutations: AtomicUsize::new(0),
    });
    (
        store.clone(),
        payload.clone(),
        DatasetUploads::new(store, payload),
    )
}
