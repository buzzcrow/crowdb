// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "access_store.rs"]
#[allow(dead_code)]
mod access_store;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
pub struct TestExpiryStore {
    inner: access_store::TestAccessStore,
    pub gate: AtomicUsize,
    pub started: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
    pub lose_mark: AtomicBool,
    pub unknown: AtomicBool,
    pub reads_after_unknown: AtomicU64,
}
impl TestExpiryStore {
    pub fn writes(&self) -> u64 {
        self.inner.writes.load(Ordering::Relaxed)
    }
    fn read(&self) {
        if self.unknown.load(Ordering::Relaxed) {
            self.reads_after_unknown.fetch_add(1, Ordering::Relaxed);
        }
    }
}
#[async_trait::async_trait]
impl DatasetStore for TestExpiryStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.read();
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.read();
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.read();
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        let gate = self.gate.load(Ordering::Relaxed);
        if ((gate == 1 && key.starts_with(b"DS\0H\0")) || (gate == 2 && key.starts_with(b"DS\0L\0")))
            && self
                .gate
                .compare_exchange(gate, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.started.notify_one();
            self.resume.notified().await;
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if key.starts_with(b"DS\0L\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_mark.swap(false, Ordering::Relaxed)
        {
            self.unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

pub struct TestExpiryFixture {
    pub store: Arc<TestExpiryStore>,
    pub authority: Arc<DatasetAuthority>,
    pub clock: Arc<AtomicU64>,
    pub identity: DatasetIdentity,
    pub root: SnapshotId,
}
impl TestExpiryFixture {
    pub async fn new() -> Self {
        let store = Arc::new(TestExpiryStore::default());
        let clock = Arc::new(AtomicU64::new(1000));
        let authority =
            Arc::new(DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone()));
        let identity = DatasetIdentity::new(NamespacePath::root(), "results").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let root = authority
            .publish_snapshot(&identity, None, crate::publication::publication_manifest(1))
            .await
            .unwrap();
        Self {
            store,
            authority,
            clock,
            identity,
            root,
        }
    }
    pub async fn candidate(&self, parent: Option<SnapshotId>, marker: u8) -> SnapshotId {
        self.authority
            .prepare_snapshot_candidate(
                &self.identity,
                parent,
                crate::publication::publication_manifest(marker),
                OperationId::from_bytes(&[marker; 16]).unwrap(),
            )
            .await
            .unwrap()
    }
    pub async fn expire(&self) -> Result<(), AuthorityError> {
        self.authority
            .expire_snapshot_candidates_page(&mut SnapshotExpirySweep::default(), &ReadCancellation::new())
            .await
    }
}

pub fn key(identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    [
        b"DS\0CB\0".as_slice(),
        &identity.key().unwrap(),
        snapshot.as_bytes(),
    ]
    .concat()
}
