// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{
    CasOutcome, DatasetIdentity, DatasetStore, DatasetUploads, MetadataPage, MetadataScan, NamespacePath,
    OperationId, ReadCancellation, StoreError, StoredValue, UploadRecord,
};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use super::upload::{TestPayload, TestStore};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TestOwnerCost {
    pub gets: u64,
    pub exchanges: u64,
    pub read_bytes: u64,
    pub observation_bytes: u64,
    pub expected_bytes: u64,
    pub value_bytes: u64,
}

impl TestOwnerCost {
    pub fn since(self, before: Self) -> Self {
        Self {
            gets: self.gets - before.gets,
            exchanges: self.exchanges - before.exchanges,
            read_bytes: self.read_bytes - before.read_bytes,
            observation_bytes: self.observation_bytes - before.observation_bytes,
            expected_bytes: self.expected_bytes - before.expected_bytes,
            value_bytes: self.value_bytes - before.value_bytes,
        }
    }
}

pub struct TestOwnerStore {
    inner: Arc<TestStore>,
    payloads: Arc<TestPayload>,
    key: Vec<u8>,
    races: u64,
    lose_reply: bool,
    gets: AtomicU64,
    exchanges: AtomicU64,
    read_bytes: AtomicU64,
    observation_bytes: AtomicU64,
    expected_bytes: AtomicU64,
    value_bytes: AtomicU64,
}

impl TestOwnerStore {
    pub fn cost(&self) -> TestOwnerCost {
        TestOwnerCost {
            gets: self.gets.load(Ordering::Relaxed),
            exchanges: self.exchanges.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            observation_bytes: self.observation_bytes.load(Ordering::Relaxed),
            expected_bytes: self.expected_bytes.load(Ordering::Relaxed),
            value_bytes: self.value_bytes.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl DatasetStore for TestOwnerStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let stored = self.inner.get(key).await?;
        if key == self.key {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.read_bytes.fetch_add(
                stored.as_ref().map_or(0, |value| value.bytes.len() as u64),
                Ordering::Relaxed,
            );
        }
        Ok(stored)
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
        assert_eq!(key, self.key, "ownership must mutate only its upload resource");
        let attempt = self.exchanges.fetch_add(1, Ordering::Relaxed);
        self.expected_bytes
            .fetch_add(expected.map_or(0, |bytes| bytes.len() as u64), Ordering::Relaxed);
        self.value_bytes.fetch_add(value.len() as u64, Ordering::Relaxed);
        let current = self.inner.get(key).await?;
        assert_eq!(
            current.as_ref().map(|stored| stored.bytes.as_slice()),
            expected,
            "every retry must consume the latest conflict observation"
        );
        if attempt < self.races {
            DatasetUploads::new(self.inner.clone(), self.payloads.clone())
                .claim(key, owner(0xc0, u32::try_from(attempt).unwrap()), 5)
                .await
                .unwrap();
            let observed = self.inner.get(key).await?;
            self.observation_bytes.fetch_add(
                observed.as_ref().map_or(0, |value| value.bytes.len() as u64),
                Ordering::Relaxed,
            );
            return Ok(CasOutcome::Conflict(observed));
        }
        let outcome = self.inner.compare_exchange(key, expected, value).await?;
        if self.lose_reply && attempt == 0 {
            assert!(matches!(outcome, CasOutcome::Applied(_)));
            return Err(StoreError::Rejected);
        }
        Ok(outcome)
    }
}

pub struct TestOwnerFixture {
    pub store: Arc<TestOwnerStore>,
    pub payloads: Arc<TestPayload>,
    pub uploads: DatasetUploads,
    pub key: Vec<u8>,
    pub live: OperationId,
    pub victim: OperationId,
}

impl TestOwnerFixture {
    pub async fn new(retired: u32, races: u64, lose_reply: bool) -> Self {
        let inner = Arc::new(TestStore::default());
        let payloads = Arc::new(TestPayload::default());
        let uploads = DatasetUploads::new(inner.clone(), payloads.clone());
        let key = DatasetUploads::location(
            &DatasetIdentity::new(NamespacePath::root(), "owner-cost").unwrap(),
            OperationId::random(),
        )
        .unwrap();
        uploads
            .create(&key, 4, 4, Md5::digest(b"data").into(), 1000)
            .await
            .unwrap();
        uploads
            .put_part(&key, 0, Bytes::from_static(b"data"), 1)
            .await
            .unwrap();
        uploads.complete(&key, 2).await.unwrap();
        let live = owner(0xb0, 0);
        let victim = owner(0xb0, 1);
        uploads.claim(&key, live, 3).await.unwrap();
        uploads.claim(&key, victim, 3).await.unwrap();
        let stored = inner.get(&key).await.unwrap().unwrap();
        let mut historical: Value = serde_json::from_slice(&stored.bytes).unwrap();
        historical["retired_owners"] =
            json!((0..retired).map(|index| owner(0xa0, index)).collect::<Vec<_>>());
        let bytes = canonical(historical);
        assert!(matches!(
            inner
                .compare_exchange(&key, Some(&stored.bytes), &bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
        let store = Arc::new(TestOwnerStore {
            inner,
            payloads: payloads.clone(),
            key: key.clone(),
            races,
            lose_reply,
            gets: AtomicU64::new(0),
            exchanges: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            observation_bytes: AtomicU64::new(0),
            expected_bytes: AtomicU64::new(0),
            value_bytes: AtomicU64::new(0),
        });
        Self {
            uploads: DatasetUploads::new(store.clone(), payloads.clone()),
            store,
            payloads,
            key,
            live,
            victim,
        }
    }

    pub fn restart(&self) -> DatasetUploads {
        DatasetUploads::new(self.store.clone(), self.payloads.clone())
    }

    pub async fn raw(&self) -> Vec<u8> {
        self.store.inner.get(&self.key).await.unwrap().unwrap().bytes
    }
}

pub fn owner(domain: u8, index: u32) -> OperationId {
    let mut bytes = [domain; 16];
    bytes[12..].copy_from_slice(&index.to_be_bytes());
    OperationId::from_bytes(&bytes).unwrap()
}

pub fn canonical(value: Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::from_value::<UploadRecord>(value).unwrap()).unwrap()
}

pub fn add_owner(bytes: &[u8], identity: OperationId) -> Vec<u8> {
    let mut value: Value = serde_json::from_slice(bytes).unwrap();
    let mut owners: Vec<OperationId> = serde_json::from_value(value["owners"].clone()).unwrap();
    owners.push(identity);
    owners.sort();
    value["owners"] = json!(owners);
    value["state"] = json!("claimed");
    canonical(value)
}

pub fn retire(bytes: &[u8], identity: OperationId) -> Vec<u8> {
    let mut value: Value = serde_json::from_slice(bytes).unwrap();
    let mut owners: Vec<OperationId> = serde_json::from_value(value["owners"].clone()).unwrap();
    let mut retired: Vec<OperationId> =
        serde_json::from_value(value.get("retired_owners").cloned().unwrap_or_else(|| json!([]))).unwrap();
    owners.retain(|owner| *owner != identity);
    retired.push(identity);
    retired.sort();
    value["owners"] = json!(owners);
    value["retired_owners"] = json!(retired);
    canonical(value)
}
