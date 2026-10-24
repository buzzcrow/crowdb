// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{legacy, publication, upload};
use arc_swap::{ArcSwap, ArcSwapOption};
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

#[derive(Default)]
pub struct TestReclaimStore {
    pub inner: upload::TestStore,
    pub frozen: AtomicBool,
    pub stalled: ArcSwapOption<Vec<u8>>,
    pub blocked: ArcSwap<Vec<Vec<u8>>>,
    pub observed: ArcSwapOption<Vec<u8>>,
    pub observations: AtomicU64,
}
#[async_trait::async_trait]
impl DatasetStore for TestReclaimStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        if self
            .observed
            .load()
            .as_ref()
            .is_some_and(|target| target.as_slice() == key)
        {
            self.observations.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .stalled
            .load()
            .as_ref()
            .is_some_and(|target| target.as_slice() == key)
            || self.blocked.load().iter().any(|target| target.as_slice() == key)
        {
            std::future::pending::<()>().await;
        }
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        scan: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.inner.scan_page(scan, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        if self.frozen.load(Ordering::Relaxed) {
            assert!(
                ![
                    b"DS\0CD\0".as_slice(),
                    b"DS\0SD\0",
                    b"DS\0SCG\0",
                    b"DS\0CCP\0",
                    b"DS\0CCG\0",
                    b"DS\0CEG\0",
                    b"DS\0CGF\0"
                ]
                .iter()
                .any(|prefix| key.starts_with(prefix)),
                "cleanup wrote a process checkpoint"
            );
        }
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

pub struct TestCleanup {
    pub store: Arc<TestReclaimStore>,
    pub authority: DatasetAuthority,
    pub uploads: DatasetUploads,
    pub identity: DatasetIdentity,
    pub parent: SnapshotId,
}
impl TestCleanup {
    pub async fn new() -> Self {
        let store = Arc::new(TestReclaimStore::default());
        let authority = DatasetAuthority::new(store.clone());
        let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
        let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-reclaim").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let parent = authority
            .publish_snapshot(&identity, None, publication::publication_manifest(1))
            .await
            .unwrap();
        Self {
            store,
            authority,
            uploads,
            identity,
            parent,
        }
    }
    pub async fn draft(&self, operation: OperationId) -> Vec<u8> {
        legacy::editing_composition(
            self.store.as_ref(),
            &self.authority,
            &self.identity,
            operation,
            self.parent,
        )
        .await;
        legacy::composition_edit(self.store.as_ref(), &self.identity, operation, vec![], false).await;
        let manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(1)).unwrap();
        legacy::composition_window(self.store.as_ref(), &self.identity, operation, &manifest.samples).await;
        self.authority
            .abandon_composition(&self.identity, operation, 1)
            .await
            .unwrap();
        for prefix in [b"DS\0CCP\0".as_slice(), b"DS\0CCG\0", b"DS\0CEG\0"] {
            self.store
                .compare_exchange(
                    &legacy::key(prefix, &self.identity, operation),
                    None,
                    b"corrupt obsolete progress",
                )
                .await
                .unwrap();
        }
        self.store
            .get(&legacy::key(b"DS\0CD\0", &self.identity, operation))
            .await
            .unwrap()
            .unwrap()
            .bytes
    }
}
