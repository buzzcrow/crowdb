// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_dataset::{
    CasOutcome, DatasetStore, MetadataPage, MetadataScan, MetadataScanBudget, ReadCancellation, StoreError,
    StoredValue,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub struct TestInputStore {
    calls: AtomicUsize,
}

impl TestInputStore {
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn reject<T>(&self) -> Result<T, StoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(StoreError::Rejected)
    }
}

#[async_trait]
impl DatasetStore for TestInputStore {
    async fn get(&self, _: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.reject()
    }

    async fn get_bounded(
        &self,
        _: &[u8],
        _: MetadataScanBudget,
        _: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.reject()
    }

    async fn scan_page(&self, _: &MetadataScan, _: &ReadCancellation) -> Result<MetadataPage, StoreError> {
        self.reject()
    }

    async fn compare_exchange(&self, _: &[u8], _: Option<&[u8]>, _: &[u8]) -> Result<CasOutcome, StoreError> {
        self.reject()
    }

    async fn delete(&self, _: &[u8]) -> Result<(), StoreError> {
        self.reject()
    }
}
