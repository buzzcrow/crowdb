// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
use crate::publication::publication_manifest;
use crowdb_access_dataset::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Default)]
pub(super) struct TestStore {
    inner: dataset_store::TestDatasetStore,
    pub(super) inspecting: AtomicBool,
}
#[async_trait::async_trait]
impl DatasetStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(
            !self.inspecting.load(Ordering::Relaxed),
            "inspection used unbounded get"
        );
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        assert!(
            !key.starts_with(b"DS\0S\0") && !key.starts_with(b"DS\0M\0"),
            "inspection loaded Manifest"
        );
        self.inner.get_bounded(key, budget, cancel).await
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
        assert!(
            !self.inspecting.load(Ordering::Relaxed),
            "inspection wrote metadata"
        );
        self.inner.compare_exchange(key, expected, value).await
    }
}
pub(super) fn budget(items: usize) -> MetadataScanBudget {
    MetadataScanBudget {
        items,
        bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
    }
}
pub(super) async fn fixture() -> (Arc<TestStore>, DatasetAuthority, DatasetIdentity) {
    let store = Arc::new(TestStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "inspection-pages").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    (store, authority, identity)
}
pub(super) async fn prepare(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    parent: Option<SnapshotId>,
    marker: u8,
) -> SnapshotId {
    authority
        .prepare_snapshot_candidate(
            identity,
            parent,
            publication_manifest(marker),
            OperationId::random(),
        )
        .await
        .unwrap()
}
pub(super) async fn publish(authority: &DatasetAuthority, identity: &DatasetIdentity, snapshot: SnapshotId) {
    authority
        .publish_snapshot_candidate(
            identity,
            &CommitAttempt {
                version: 1,
                id: OperationId::random(),
                target: CommitTarget::Snapshot { snapshot },
                expected_head: authority.inspect_head(identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
}

impl TestStore {
    #[allow(dead_code)]
    pub(super) fn storage(&self) -> &dyn DatasetStore {
        &self.inner
    }
}
