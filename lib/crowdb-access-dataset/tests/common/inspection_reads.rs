// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::dataset_store::TestDatasetStore;
use crowdb_access_dataset::*;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

#[derive(Default)]
pub struct TestIndexReads {
    pub inner: TestDatasetStore,
    pub reading: AtomicBool,
    pub stalled: AtomicBool,
    pub fail_value: AtomicBool,
    pub pending: AtomicUsize,
    reserved: AtomicUsize,
    pub peak_pending: AtomicUsize,
    pub peak_reserved: AtomicUsize,
    pub total_bytes: AtomicUsize,
    pub reads: Mutex<BTreeMap<Vec<u8>, usize>>,
}

struct TestReadGuard<'a> {
    store: &'a TestIndexReads,
    bytes: usize,
}
impl Drop for TestReadGuard<'_> {
    fn drop(&mut self) {
        self.store.pending.fetch_sub(1, Ordering::Relaxed);
        self.store.reserved.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl TestIndexReads {
    pub fn begin(&self) {
        self.reads.lock().unwrap().clear();
        self.total_bytes.store(0, Ordering::Relaxed);
        self.reading.store(true, Ordering::Relaxed);
    }
    pub fn key(identity: &DatasetIdentity, snapshot: SnapshotId, kind: u8, suffix: &[&[u8]]) -> Vec<u8> {
        let mut key = b"DS\0I\0".to_vec();
        key.extend(identity.key().unwrap());
        key.extend(snapshot.as_bytes());
        key.push(kind);
        for part in suffix {
            for byte in *part {
                if *byte == 0 {
                    key.extend([0, 255]);
                } else {
                    key.push(*byte);
                }
            }
            key.extend([0, 0]);
        }
        key
    }
}

#[async_trait::async_trait]
impl DatasetStore for TestIndexReads {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(!self.reading.load(Ordering::Relaxed), "unbounded inspection GET");
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        assert!(!key.starts_with(b"DS\0S\0") && !key.starts_with(b"DS\0M\0"));
        *self.reads.lock().unwrap().entry(key.to_vec()).or_default() += 1;
        let _guard = TestReadGuard {
            store: self,
            bytes: budget.bytes,
        };
        self.peak_pending.fetch_max(
            self.pending.fetch_add(1, Ordering::Relaxed) + 1,
            Ordering::Relaxed,
        );
        self.peak_reserved.fetch_max(
            self.reserved.fetch_add(budget.bytes, Ordering::Relaxed) + budget.bytes,
            Ordering::Relaxed,
        );
        if self.reading.load(Ordering::Relaxed) {
            // Different completion times exercise caller order and held response capacity.
            let delay = if key.last().is_some_and(|b| b % 3 == 0) {
                6
            } else {
                1
            };
            tokio::time::sleep(Duration::from_millis(delay)).await;
            while self.stalled.load(Ordering::Relaxed)
                && matches!(budget.bytes, 131_072 | 524_288)
                && !cancel.is_cancelled()
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let value = self.inner.get_bounded(key, budget, cancel).await?;
        if self.fail_value.load(Ordering::Relaxed) && budget.bytes == 512 * 1024 {
            return Err(StoreError::Rejected);
        }
        self.total_bytes.fetch_add(
            key.len() + value.as_ref().map_or(0, |v| v.bytes.len()),
            Ordering::Relaxed,
        );
        Ok(value)
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        let page = self.inner.scan_page(request, cancel).await?;
        self.total_bytes.fetch_add(
            page.entries
                .iter()
                .map(|(key, value)| key.len() + value.bytes.len())
                .sum::<usize>(),
            Ordering::Relaxed,
        );
        Ok(page)
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        assert!(!self.reading.load(Ordering::Relaxed), "inspection mutation");
        self.inner.compare_exchange(key, expected, value).await
    }
}
