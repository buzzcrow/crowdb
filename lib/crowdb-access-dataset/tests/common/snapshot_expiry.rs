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
    pub lose_owner_retirement: AtomicBool,
    pub lose_deadline: AtomicBool,
    pub resource_pages: AtomicUsize,
    pub scan_peak_bytes: AtomicUsize,
    pub scan_peak_items: AtomicUsize,
    pub unknown: AtomicBool,
    pub reads_after_unknown: AtomicU64,
    pub writes_after_unknown: AtomicU64,
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
        let page = self.inner.scan_page(request, cancel).await?;
        if request.prefix == b"DS\0S\0" {
            self.resource_pages.fetch_add(1, Ordering::Relaxed);
            self.scan_peak_items
                .fetch_max(page.entries.len(), Ordering::Relaxed);
            self.scan_peak_bytes.fetch_max(
                page.entries
                    .iter()
                    .map(|(key, value)| key.len() + value.bytes.len())
                    .sum(),
                Ordering::Relaxed,
            );
        }
        Ok(page)
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        if self.unknown.load(Ordering::Relaxed) {
            self.writes_after_unknown.fetch_add(1, Ordering::Relaxed);
        }
        let gate = self.gate.load(Ordering::Relaxed);
        let pause = match gate {
            1 => key.starts_with(b"DS\0H\0"),
            2 => key.starts_with(b"DS\0L\0"),
            3 => {
                key.starts_with(b"DS\0U\0")
                    && serde_json::from_slice::<serde_json::Value>(value)
                        .is_ok_and(|value| value["state"] == "claimed")
            }
            4 => key.starts_with(b"DS\0I\0"),
            5 => key.starts_with(b"DS\0M\0"),
            _ => false,
        };
        if pause
            && self
                .gate
                .compare_exchange(gate, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.started.notify_one();
            self.resume.notified().await;
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if key.starts_with(b"DS\0S\0")
            && expected.is_some_and(|bytes| bytes.starts_with(&1_u16.to_le_bytes()))
            && value.starts_with(&2_u16.to_le_bytes())
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_deadline.swap(false, Ordering::Relaxed)
        {
            self.unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        if key.starts_with(b"DS\0L\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_mark.swap(false, Ordering::Relaxed)
        {
            self.unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        if key.starts_with(b"DS\0U\0")
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_owner_retirement.load(Ordering::Relaxed)
            && serde_json::from_slice::<serde_json::Value>(value).is_ok_and(|value| {
                value["retired_owners"]
                    .as_array()
                    .is_some_and(|owners| !owners.is_empty())
            })
        {
            self.lose_owner_retirement.store(false, Ordering::Relaxed);
            self.unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        if self.unknown.load(Ordering::Relaxed) {
            self.writes_after_unknown.fetch_add(1, Ordering::Relaxed);
        }
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
