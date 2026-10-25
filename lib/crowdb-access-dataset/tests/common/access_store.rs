// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Default)]
pub struct TestAccessStore {
    inner: dataset_store::TestDatasetStore,
    pub writes: AtomicU64,
    pub reads: AtomicU64,
    pub admission_reads: AtomicU64,
    pub bounded_reads: AtomicU64,
    pub schema_scans: AtomicU64,
    pub member_scans: AtomicU64,
    pub upload_conflicts: AtomicU64,
}
impl TestAccessStore {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }
}
#[async_trait::async_trait]
impl DatasetStore for TestAccessStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if key.starts_with(b"DS\0L\0") {
            self.admission_reads.fetch_add(1, Ordering::Relaxed);
        }
        tokio::task::yield_now().await;
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.bounded_reads.fetch_add(1, Ordering::Relaxed);
        if key.starts_with(b"DS\0L\0") {
            self.admission_reads.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        if request.prefix.starts_with(b"DS\0I\0") {
            match request.prefix.last() {
                Some(b'S') => {
                    self.schema_scans.fetch_add(1, Ordering::Relaxed);
                }
                Some(b'M') => {
                    self.member_scans.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        if self
            .upload_conflicts
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            let old = self.inner.get(key).await?.expect("upload exists");
            let mut record = UploadRecord::decode_stored_for_tests(&old.bytes).unwrap();
            record.expires_at += 1;
            assert!(matches!(
                self.inner
                    .compare_exchange(key, Some(&old.bytes), &record.encode_stored_for_tests().unwrap())
                    .await?,
                CasOutcome::Applied(_)
            ));
        }
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key).await
    }
}
