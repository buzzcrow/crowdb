// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;

use crowdb_access_dataset::*;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

pub const RETIRED: &[u8] = b"retired-snapshot-content-v1";

#[derive(Default)]
pub struct Store {
    inner: dataset_store::TestDatasetStore,
    pub gate: AtomicU8,
    pub started: Notify,
    pub release: Notify,
    pub lose_final_reply: AtomicBool,
    pub unknown: AtomicBool,
    pub reads_after_unknown: AtomicUsize,
    pub seal_winner: arc_swap::ArcSwapOption<Vec<u8>>,
    pub seal_conflicted: AtomicBool,
    pub cb_reads_after_conflict: AtomicUsize,
    pub reads_only: AtomicBool,
    pub bounded_reads: AtomicUsize,
}

impl Store {
    pub fn storage(&self) -> &dyn DatasetStore {
        &self.inner
    }
    fn check_read(&self, key: &[u8]) {
        if self.unknown.load(Ordering::Relaxed) {
            self.reads_after_unknown.fetch_add(1, Ordering::Relaxed);
        }
        if key.starts_with(b"DS\0CB\0") && self.seal_conflicted.load(Ordering::Relaxed) {
            self.cb_reads_after_conflict.fetch_add(1, Ordering::Relaxed);
        }
        if self.reads_only.load(Ordering::Relaxed) {
            assert!(
                ![
                    b"DS\0SC\0".as_slice(),
                    b"DS\0CA\0",
                    b"DS\0CR\0",
                    b"DS\0LC\0",
                    b"DS\0H\0"
                ]
                .iter()
                .any(|prefix| key.starts_with(prefix)),
                "complete ID read queried publication evidence"
            );
        }
    }
}

#[async_trait::async_trait]
impl DatasetStore for Store {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.check_read(key);
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.check_read(key);
        self.bounded_reads.fetch_add(1, Ordering::Relaxed);
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
            !self.reads_only.load(Ordering::Relaxed),
            "read installed metadata"
        );
        let final_root = key.starts_with(b"DS\0CB\0")
            && value.first() == Some(&b'{')
            && serde_json::from_slice::<serde_json::Value>(value).unwrap()["version"] == 2;
        let gate = self.gate.load(Ordering::Relaxed);
        if (gate == 1 && final_root) || (gate == 2 && key.starts_with(b"DS\0IC\0")) {
            self.gate.store(0, Ordering::Relaxed);
            self.started.notify_one();
            self.release.notified().await;
        }
        if key.starts_with(b"DS\0CB\0") && value == RETIRED {
            if let Some(winner) = self.seal_winner.swap(None) {
                assert!(matches!(
                    self.inner.compare_exchange(key, expected, &winner).await?,
                    CasOutcome::Applied(_)
                ));
                self.seal_conflicted.store(true, Ordering::Relaxed);
            }
        }
        let result = self.inner.compare_exchange(key, expected, value).await?;
        if final_root
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_final_reply.swap(false, Ordering::Relaxed)
        {
            self.unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        assert!(!self.reads_only.load(Ordering::Relaxed));
        self.inner.delete(key).await
    }
}

pub fn key(prefix: &[u8], identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    [prefix, identity.key().unwrap().as_slice(), snapshot.as_bytes()].concat()
}
pub fn candidate(operation: OperationId) -> OperationId {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-composition-candidate-v1\0");
    hash.update(operation.as_bytes());
    hash.update(1u64.to_be_bytes());
    OperationId::from_bytes(&hash.finalize()[..16]).unwrap()
}
pub async fn fixture() -> (Arc<Store>, Arc<DatasetAuthority>, DatasetIdentity, SnapshotId) {
    let store = Arc::new(Store::default());
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "complete-snapshots").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let parent = authority
        .publish_snapshot(&identity, None, crate::publication::publication_manifest(1))
        .await
        .unwrap();
    (store, authority, identity, parent)
}
pub async fn finish_cleanup(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    operation: OperationId,
) {
    for _ in 0..100 {
        if authority
            .cleanup_composition_candidates_step(identity, operation, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("candidate cleanup did not finish");
}
