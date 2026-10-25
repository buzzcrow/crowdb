// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_dataset::{
    CasOutcome, DatasetStore, DatasetUploads, MetadataPage, MetadataScan, OperationId, ReadCancellation,
    StoreError, StoredValue, UploadRecord,
};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU64, AtomicU8, Ordering},
    Arc,
};

use super::upload::{TestPayload, TestStore};

pub const MAGIC: &[u8; 8] = b"DSUP\0\0\0\x06";
pub const HEADER_BYTES: usize = 52;

pub struct TestCodecStore {
    pub inner: Arc<TestStore>,
    payloads: Arc<TestPayload>,
    pub fault: AtomicU8,
    reads: AtomicU64,
    exchanges: AtomicU64,
}
impl TestCodecStore {
    pub fn counts(&self) -> (u64, u64) {
        (
            self.reads.load(Ordering::Relaxed),
            self.exchanges.load(Ordering::Relaxed),
        )
    }
}
#[async_trait]
impl DatasetStore for TestCodecStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key).await
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
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        let current = self.inner.get(key).await?;
        assert_eq!(current.as_ref().map(|row| row.bytes.as_slice()), expected);
        let fault = self.fault.swap(0, Ordering::Relaxed);
        if fault == 1 {
            return Err(StoreError::Rejected);
        }
        if fault == 3 {
            DatasetUploads::new(self.inner.clone(), self.payloads.clone())
                .release(key, owner(0xb1))
                .await
                .unwrap();
        }
        if fault == 4 {
            let previous = current.as_ref().unwrap();
            let mut legacy: Value = serde_json::from_slice(&previous.bytes).unwrap();
            let mut owners: Vec<OperationId> = serde_json::from_value(legacy["owners"].clone()).unwrap();
            owners.push(owner(0xc0));
            owners.sort();
            legacy["owners"] = json!(owners);
            legacy["state"] = json!("claimed");
            let record: UploadRecord = serde_json::from_value(legacy).unwrap();
            let legacy = serde_json::to_vec(&record).unwrap();
            assert!(matches!(
                self.inner
                    .compare_exchange(key, Some(&previous.bytes), &legacy)
                    .await?,
                CasOutcome::Applied(_)
            ));
        }
        if fault == 5 {
            DatasetUploads::new(self.inner.clone(), self.payloads.clone())
                .retire_owner(key, historical(1))
                .await
                .unwrap();
        }
        let outcome = self.inner.compare_exchange(key, expected, value).await?;
        if fault == 2 {
            assert!(matches!(outcome, CasOutcome::Applied(_)));
            return Err(StoreError::Rejected);
        }
        Ok(outcome)
    }
}

pub struct TestCodecFixture {
    pub store: Arc<TestCodecStore>,
    pub uploads: DatasetUploads,
    pub key: Vec<u8>,
    pub legacy: Value,
}
impl TestCodecFixture {
    pub async fn new(version: u16, retired: u16) -> Self {
        let inner = Arc::new(TestStore::default());
        let payloads = Arc::new(TestPayload::default());
        let legacy = json!({
            "version": version,
            "length": 4,
            "part_size": 4,
            "md5": <[u8; 16]>::from(Md5::digest(b"data")),
            "expires_at": 1000,
            "state": "claimed",
            "owners": [owner(0xb0), owner(0xb1)],
            "retired_owners": (0..retired).map(historical).collect::<Vec<_>>(),
            "cleanup_complete": false,
        });
        let store = Arc::new(TestCodecStore {
            inner,
            payloads: payloads.clone(),
            fault: AtomicU8::new(0),
            reads: AtomicU64::new(0),
            exchanges: AtomicU64::new(0),
        });
        let fixture = Self {
            uploads: DatasetUploads::new(store.clone(), payloads),
            store,
            key: b"DS\0U\0codec".to_vec(),
            legacy,
        };
        fixture.install_json(&fixture.legacy).await;
        fixture
    }
    pub fn restart(&self) -> DatasetUploads {
        DatasetUploads::new(self.store.clone(), self.store.payloads.clone())
    }
    pub async fn raw(&self) -> Vec<u8> {
        self.store.inner.get(&self.key).await.unwrap().unwrap().bytes
    }
    pub async fn install(&self, bytes: &[u8]) {
        let old = self.store.inner.get(&self.key).await.unwrap();
        assert!(matches!(
            self.store
                .inner
                .compare_exchange(&self.key, old.as_ref().map(|value| value.bytes.as_slice()), bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
    pub async fn install_json(&self, value: &Value) {
        let record: UploadRecord = serde_json::from_value(value.clone()).unwrap();
        self.install(&serde_json::to_vec(&record).unwrap()).await;
    }
    pub async fn public(&self) -> Value {
        serde_json::to_value(self.restart().metadata(&self.key).await.unwrap()).unwrap()
    }
}
pub fn owner(domain: u8) -> OperationId {
    OperationId::from_bytes(&[domain; 16]).unwrap()
}
pub fn historical(index: u16) -> OperationId {
    let mut bytes = [0xa0; 16];
    bytes[14..].copy_from_slice(&index.to_be_bytes());
    OperationId::from_bytes(&bytes).unwrap()
}

pub async fn assert_binary(store: &TestStore, key: &[u8]) {
    let stored = store.get(key).await.unwrap().unwrap();
    assert!(stored.bytes.starts_with(MAGIC));
}
