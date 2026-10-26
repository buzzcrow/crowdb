// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::{TestExpiryFixture, TestExpiryStore};
use crowdb_access_dataset::*;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

pub const DELETE_UNKNOWN: usize = 1;
pub const DELETE_TIMEOUT: usize = 2;
pub const CANCEL_AFTER_RETIREMENT_READ: usize = 3;

pub struct TestChildStore {
    pub inner: Arc<TestExpiryStore>,
    pub index: Vec<u8>,
    pub retirement: Vec<u8>,
    pub fault: AtomicUsize,
    pub hit: AtomicBool,
    pub deletes: AtomicUsize,
    pub after_unknown: AtomicUsize,
    unknown: AtomicBool,
}
impl TestChildStore {
    pub fn new(f: &TestExpiryFixture, parent: SnapshotId, child: SnapshotId) -> Arc<Self> {
        Arc::new(Self {
            inner: f.store.clone(),
            index: child_key(&f.identity, parent, child),
            retirement: retired_key(&f.identity, child),
            fault: AtomicUsize::new(0),
            hit: AtomicBool::new(false),
            deletes: AtomicUsize::new(0),
            after_unknown: AtomicUsize::new(0),
            unknown: AtomicBool::new(false),
        })
    }
    pub fn reset_unknown(&self) {
        self.unknown.store(false, Ordering::Relaxed);
    }
    fn observe(&self) {
        if self.unknown.load(Ordering::Relaxed) {
            self.after_unknown.fetch_add(1, Ordering::Relaxed);
        }
    }
}
#[async_trait::async_trait]
impl DatasetStore for TestChildStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.observe();
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.observe();
        let value = self.inner.get_bounded(key, budget, cancel).await?;
        if key == self.retirement
            && self
                .fault
                .compare_exchange(
                    CANCEL_AFTER_RETIREMENT_READ,
                    0,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            assert_eq!(value.as_ref().unwrap().bytes, [1]);
            self.hit.store(true, Ordering::Relaxed);
            cancel.cancel();
        }
        Ok(value)
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.observe();
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.observe();
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.observe();
        if key != self.index {
            return self.inner.delete(key).await;
        }
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key).await?;
        match self.fault.swap(0, Ordering::Relaxed) {
            DELETE_UNKNOWN => {
                self.hit.store(true, Ordering::Relaxed);
                self.unknown.store(true, Ordering::Relaxed);
                Err(StoreError::Response)
            }
            DELETE_TIMEOUT => {
                self.hit.store(true, Ordering::Relaxed);
                self.unknown.store(true, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

pub fn child_key(identity: &DatasetIdentity, parent: SnapshotId, child: SnapshotId) -> Vec<u8> {
    [
        b"DS\0IC\0".as_slice(),
        &identity.key().unwrap(),
        &[1],
        parent.as_bytes(),
        child.as_bytes(),
    ]
    .concat()
}
pub fn retired_key(identity: &DatasetIdentity, child: SnapshotId) -> Vec<u8> {
    [b"DS\0RT\0".as_slice(), &identity.key().unwrap(), child.as_bytes()].concat()
}
pub async fn pair() -> (TestExpiryFixture, SnapshotId, SnapshotId) {
    let f = TestExpiryFixture::new().await;
    let parent = f.candidate(None, 2).await;
    let child = f.candidate(Some(parent), 3).await;
    f.authority.begin_reclaim(&f.identity, child).await.unwrap();
    (f, parent, child)
}
