// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_dataset::{
    CasOutcome, DatasetStore, DatasetUploads, FieldLocator, ManifestRecord, OperationId, StoreError,
    StoredValue, UploadRecord,
};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

use super::upload::{TestPayload, TestStore};

pub struct TestRetirementStore {
    inner: Arc<TestStore>,
    pub fault: AtomicU8,
    reads: AtomicUsize,
    exchanges: AtomicUsize,
}

impl TestRetirementStore {
    pub fn new(inner: Arc<TestStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fault: AtomicU8::new(0),
            reads: AtomicUsize::new(0),
            exchanges: AtomicUsize::new(0),
        })
    }

    pub fn uploads(self: &Arc<Self>) -> DatasetUploads {
        DatasetUploads::new(self.clone(), Arc::new(TestPayload::default()))
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.exchanges.load(Ordering::Relaxed),
        )
    }

    pub async fn value(&self, key: &[u8]) -> Value {
        serde_json::to_value(
            UploadRecord::decode_stored_for_tests(&self.inner.get(key).await.unwrap().unwrap().bytes)
                .unwrap(),
        )
        .unwrap()
    }

    pub async fn install(&self, key: &[u8], value: Value) {
        let current = self.inner.get(key).await.unwrap().unwrap();
        let bytes = serde_json::from_value::<UploadRecord>(value)
            .unwrap()
            .encode_stored_for_tests()
            .unwrap();
        assert!(matches!(
            self.inner
                .compare_exchange(key, Some(&current.bytes), &bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }

    pub async fn fill_history(&self, key: &[u8], version: u16, absent: Option<OperationId>) {
        let mut value = self.value(key).await;
        value["version"] = json!(version);
        value["retired_owners"] = json!((0..4096u16).map(historical).collect::<Vec<_>>());
        if let Some(absent) = absent {
            let mut owners: Vec<OperationId> = serde_json::from_value(value["owners"].clone()).unwrap();
            owners.retain(|owner| *owner != absent);
            value["owners"] = json!(owners);
        }
        self.install(key, value).await;
    }
}

#[async_trait]
impl DatasetStore for TestRetirementStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(key.starts_with(b"DS\0U\0"));
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        let fault = self.fault.swap(0, Ordering::Relaxed);
        if fault == 1 {
            return Err(StoreError::Rejected);
        }
        if fault == 3 {
            let mut current = self.value(key).await;
            let mut owners: Vec<OperationId> = serde_json::from_value(current["owners"].clone()).unwrap();
            owners.push(rival());
            current["owners"] = json!(owners);
            self.install(key, current).await;
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if fault == 2 {
            assert!(matches!(result, CasOutcome::Applied(_)));
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

pub fn other() -> OperationId {
    OperationId::from_bytes(&[2; 16]).unwrap()
}

pub fn rival() -> OperationId {
    OperationId::from_bytes(&[3; 16]).unwrap()
}

fn historical(index: u16) -> OperationId {
    let mut bytes = [255; 16];
    bytes[14..].copy_from_slice(&(0xf000 + index).to_be_bytes());
    OperationId::from_bytes(&bytes).unwrap()
}

pub async fn complete_upload(test: &super::TestCleanup, location: &[u8]) {
    test.uploads
        .create(location, 4, 4, Md5::digest(b"data").into(), u64::MAX)
        .await
        .unwrap();
    test.uploads
        .put_part(location, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    test.uploads.complete(location, 2).await.unwrap();
    test.uploads
        .claim(
            location,
            OperationId::from_bytes(test.snapshot.as_bytes()).unwrap(),
            3,
        )
        .await
        .unwrap();
}

pub async fn replace_locations(test: &super::TestCleanup, locations: Vec<Vec<u8>>) {
    super::snapshot_resource::mutate(&*test.store, &test.identity, test.snapshot, |publication| {
        let mut manifest: ManifestRecord = bincode::deserialize(&publication.manifest).unwrap();
        assert!(locations.len() <= manifest.samples.len());
        manifest.samples.truncate(locations.len());
        for (sample, location) in manifest.samples.iter_mut().zip(locations) {
            let FieldLocator::Chunk {
                location: existing, ..
            } = &mut sample.fields[0].value
            else {
                panic!("fixture lost its Chunk field");
            };
            *existing = location;
        }
        manifest.validate().unwrap();
        publication.manifest = bincode::serialize(&manifest).unwrap();
    })
    .await;
}
