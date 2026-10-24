// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{
    CasOutcome, DatasetIdentity, DatasetStore, DatasetUploads, MetadataPage, MetadataScan,
    MetadataScanBudget, NamespacePath, OperationId, ReadCancellation, StoreError, StoredValue, UploadRecord,
};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use super::upload::{TestPayload, TestStore};

pub struct TestSaturationStore {
    inner: Arc<TestStore>,
    payloads: Arc<TestPayload>,
    pub lose_reply: AtomicBool,
    pub rival: AtomicUsize,
    gets: AtomicUsize,
    exchanges: AtomicUsize,
    bounded: AtomicUsize,
    pub budget: AtomicUsize,
}

impl TestSaturationStore {
    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.gets.load(Ordering::Relaxed),
            self.exchanges.load(Ordering::Relaxed),
            self.bounded.load(Ordering::Relaxed),
        )
    }

    pub fn delta(&self, before: (usize, usize, usize)) -> (usize, usize, usize) {
        let current = self.counts();
        (current.0 - before.0, current.1 - before.1, current.2 - before.2)
    }
}

#[async_trait]
impl DatasetStore for TestSaturationStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key).await
    }

    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.bounded.fetch_add(1, Ordering::Relaxed);
        self.budget.store(budget.bytes, Ordering::Relaxed);
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
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        if let Some(expected) = expected {
            assert!(
                self.inner
                    .get(key)
                    .await?
                    .as_ref()
                    .is_some_and(|value| value.bytes == expected),
                "a retry must use the last observed resource"
            );
        } else {
            let candidate: Value = serde_json::from_slice(value).unwrap();
            assert_eq!(candidate["state"], "open");
            assert_eq!(candidate["version"], 4);
        }
        let rival = DatasetUploads::new(self.inner.clone(), self.payloads.clone());
        match self.rival.swap(0, Ordering::Relaxed) {
            0 => {}
            1 => rival.retire_owner(key, owner(0xb0)).await.unwrap(),
            2 => rival.release(key, owner(0xb1)).await.unwrap(),
            _ => unreachable!(),
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if matches!(result, CasOutcome::Applied(_)) && self.lose_reply.swap(false, Ordering::Relaxed) {
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

pub struct TestSaturation {
    pub store: Arc<TestSaturationStore>,
    pub uploads: DatasetUploads,
    pub key: Vec<u8>,
}

impl TestSaturation {
    pub async fn new(retired: u16, long_key: bool) -> Self {
        let inner = Arc::new(TestStore::default());
        let payloads = Arc::new(TestPayload::default());
        let uploads = DatasetUploads::new(inner.clone(), payloads.clone());
        let identity = if long_key {
            let mut segments = vec!["n".repeat(255); 253];
            segments.push("n".repeat(227));
            DatasetIdentity::new(NamespacePath::new(segments).unwrap(), "d".repeat(255)).unwrap()
        } else {
            DatasetIdentity::new(NamespacePath::root(), "saturated").unwrap()
        };
        let key = DatasetUploads::location(&identity, OperationId::random()).unwrap();
        uploads
            .create(&key, 4, 4, Md5::digest(b"data").into(), 1000)
            .await
            .unwrap();
        uploads
            .put_part(&key, 0, Bytes::from_static(b"data"), 1)
            .await
            .unwrap();
        uploads.complete(&key, 2).await.unwrap();
        for identity in [owner(0xb0), owner(0xb1)] {
            uploads.claim(&key, identity, 3).await.unwrap();
        }
        let current = inner.get(&key).await.unwrap().unwrap();
        let mut value: Value = serde_json::from_slice(&current.bytes).unwrap();
        value["retired_owners"] = json!((0..retired).map(historical).collect::<Vec<_>>());
        assert!(matches!(
            inner
                .compare_exchange(&key, Some(&current.bytes), &canonical(value))
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
        let store = Arc::new(TestSaturationStore {
            inner,
            payloads,
            lose_reply: AtomicBool::new(false),
            rival: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            exchanges: AtomicUsize::new(0),
            bounded: AtomicUsize::new(0),
            budget: AtomicUsize::new(0),
        });
        Self {
            uploads: DatasetUploads::new(store.clone(), store.payloads.clone()),
            store,
            key,
        }
    }

    pub fn restart(&self) -> DatasetUploads {
        DatasetUploads::new(self.store.clone(), self.store.payloads.clone())
    }

    pub async fn raw(&self) -> Vec<u8> {
        self.store.inner.get(&self.key).await.unwrap().unwrap().bytes
    }

    pub async fn value(&self) -> Value {
        serde_json::from_slice(&self.raw().await).unwrap()
    }

    pub async fn install(&self, value: Value) {
        self.install_bytes(&canonical(value)).await;
    }

    pub async fn install_bytes(&self, bytes: &[u8]) {
        let current = self.raw().await;
        assert!(matches!(
            self.store
                .inner
                .compare_exchange(&self.key, Some(&current), bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }

    pub async fn stale_exchange(&self, expected: &[u8], proposed: &[u8]) -> CasOutcome {
        self.store
            .inner
            .compare_exchange(&self.key, Some(expected), proposed)
            .await
            .unwrap()
    }
}

pub fn owner(domain: u8) -> OperationId {
    OperationId::from_bytes(&[domain; 16]).unwrap()
}

pub fn historical(index: u16) -> OperationId {
    let mut bytes = [255; 16];
    bytes[14..].copy_from_slice(&(0xf000 + index).to_be_bytes());
    OperationId::from_bytes(&bytes).unwrap()
}

pub fn canonical(value: Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::from_value::<UploadRecord>(value).unwrap()).unwrap()
}
