// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_dataset::{CasOutcome, DatasetStore, StoreError, StoredValue};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::dataset_store::TestDatasetStore;

pub struct TestIndexWrites {
    pub inner: TestDatasetStore,
    scope_offset: usize,
    pending: AtomicUsize,
    bytes: AtomicUsize,
    values: AtomicUsize,
    pub peak_pending: AtomicUsize,
    pub peak_bytes: AtomicUsize,
    pub peak_values: AtomicUsize,
    pub rows: AtomicUsize,
    pub fail_value: AtomicBool,
}

impl TestIndexWrites {
    pub fn new(identity_bytes: usize) -> Self {
        Self {
            inner: TestDatasetStore::default(),
            scope_offset: b"DS\0I\0".len() + identity_bytes + 16,
            pending: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            values: AtomicUsize::new(0),
            peak_pending: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
            peak_values: AtomicUsize::new(0),
            rows: AtomicUsize::new(0),
            fail_value: AtomicBool::new(false),
        }
    }
}

struct TestWriteGuard<'store> {
    store: &'store TestIndexWrites,
    bytes: usize,
    value: bool,
}

impl Drop for TestWriteGuard<'_> {
    fn drop(&mut self) {
        self.store.pending.fetch_sub(1, Ordering::Relaxed);
        self.store.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        if self.value {
            self.store.values.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl DatasetStore for TestIndexWrites {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        let kind = key.get(self.scope_offset).copied();
        if !key.starts_with(b"DS\0I\0") || !matches!(kind, Some(b'S' | b'M' | b'F' | b'V')) {
            return self.inner.compare_exchange(key, expected, value).await;
        }
        let guard = TestWriteGuard {
            store: self,
            bytes: key.len() + value.len(),
            value: kind == Some(b'V'),
        };
        self.rows.fetch_add(1, Ordering::Relaxed);
        self.peak_pending.fetch_max(
            self.pending.fetch_add(1, Ordering::Relaxed) + 1,
            Ordering::Relaxed,
        );
        self.peak_bytes.fetch_max(
            self.bytes.fetch_add(guard.bytes, Ordering::Relaxed) + guard.bytes,
            Ordering::Relaxed,
        );
        if guard.value {
            self.peak_values
                .fetch_max(self.values.fetch_add(1, Ordering::Relaxed) + 1, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if self.fail_value.swap(false, Ordering::Relaxed) {
                return Err(StoreError::Rejected);
            }
        } else {
            tokio::task::yield_now().await;
        }
        self.inner.compare_exchange(key, expected, value).await
    }
}
