// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

pub struct TestAccessStore {
    pub inner: Arc<dyn DatasetStore>,
    pub race: AtomicBool,
    pub renewals: AtomicU64,
    pub identity: DatasetIdentity,
    pub snapshot: SnapshotId,
    pub other: OperationId,
}

pub fn store() -> Arc<dyn DatasetStore> {
    Arc::new(dataset_store::TestDatasetStore::default())
}

#[async_trait::async_trait]
impl DatasetStore for TestAccessStore {
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

    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let value = self.inner.get(key).await?;
        if self.race.load(Ordering::Relaxed) {
            // A different reader renews between our point read and conditional write.
            let time = u64::MAX / 2 + self.renewals.fetch_add(1, Ordering::Relaxed);
            DatasetAuthority::new(self.inner.clone())
                .touch_read_handle(&self.identity, self.snapshot, self.other, time, 3600)
                .await
                .unwrap();
        }
        Ok(value)
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.inner.compare_exchange(key, expected, value).await
    }
}
