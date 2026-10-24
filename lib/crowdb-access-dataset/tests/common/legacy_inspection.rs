// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#![allow(dead_code)]
use super::dataset_store::TestDatasetStore;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Arc,
};

pub struct TestLegacyStore {
    pub inner: TestDatasetStore,
    scope_offset: usize,
    pub reads: AtomicUsize,
    pub source_reads: AtomicUsize,
    pub progress_reads: AtomicUsize,
    pub mutations: AtomicUsize,
    pub fail_value: AtomicBool,
    pub allow_legacy_get: AtomicBool,
    pub lose_final_reply: AtomicBool,
    pub reject_after_unknown: AtomicBool,
    pub pause_kind: AtomicU8,
    pub observed: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
    pub pending: AtomicUsize,
    pub peak_pending: AtomicUsize,
    pub released_conflicts: AtomicUsize,
}
impl TestLegacyStore {
    pub fn new(identity: &DatasetIdentity) -> Self {
        Self {
            inner: TestDatasetStore::default(),
            scope_offset: b"DS\0I\0".len() + identity.key().unwrap().len() + 16,
            reads: AtomicUsize::default(),
            source_reads: AtomicUsize::default(),
            progress_reads: AtomicUsize::default(),
            mutations: AtomicUsize::default(),
            fail_value: AtomicBool::default(),
            allow_legacy_get: AtomicBool::default(),
            lose_final_reply: AtomicBool::default(),
            reject_after_unknown: AtomicBool::default(),
            pause_kind: AtomicU8::default(),
            observed: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            pending: AtomicUsize::default(),
            peak_pending: AtomicUsize::default(),
            released_conflicts: AtomicUsize::default(),
        }
    }
    fn kind(&self, key: &[u8]) -> Option<u8> {
        key.starts_with(b"DS\0I\0")
            .then(|| key.get(self.scope_offset).copied())
            .flatten()
    }
    fn reading(&self, key: &[u8]) {
        assert!(
            !self.reject_after_unknown.load(Ordering::Relaxed),
            "confirmation after unknown write"
        );
        self.reads.fetch_add(1, Ordering::Relaxed);
        if key.starts_with(b"DS\0S\0") {
            self.source_reads.fetch_add(1, Ordering::Relaxed);
        }
        if self.kind(key) == Some(b'P') {
            self.progress_reads.fetch_add(1, Ordering::Relaxed);
        }
    }
}
struct TestPending<'a>(&'a TestLegacyStore);
impl Drop for TestPending<'_> {
    fn drop(&mut self) {
        self.0.pending.fetch_sub(1, Ordering::Relaxed);
    }
}
#[async_trait::async_trait]
impl DatasetStore for TestLegacyStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        assert!(
            !key.starts_with(b"DS\0S\0") || self.allow_legacy_get.load(Ordering::Relaxed),
            "legacy source must be bounded"
        );
        self.reading(key);
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.reading(key);
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        scan: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        assert!(!self.reject_after_unknown.load(Ordering::Relaxed));
        self.inner.scan_page(scan, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        bytes: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        assert!(!self.reject_after_unknown.load(Ordering::Relaxed));
        let kind = self.kind(key);
        assert_ne!(kind, Some(b'P'), "index preparation wrote process progress");
        self.mutations.fetch_add(1, Ordering::Relaxed);
        let _pending = TestPending(self);
        self.peak_pending.fetch_max(
            self.pending.fetch_add(1, Ordering::Relaxed) + 1,
            Ordering::Relaxed,
        );
        let ready = kind == Some(b'H')
            && serde_json::from_slice::<serde_json::Value>(bytes)
                .is_ok_and(|value| value["state"] == "Ready");
        let paused = (kind != Some(b'H') || ready)
            && self
                .pause_kind
                .compare_exchange(kind.unwrap_or(0), 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            && kind.is_some();
        if paused {
            self.observed.notify_one();
            self.resume.notified().await;
        }
        if kind == Some(b'V') {
            tokio::task::yield_now().await;
            if self.fail_value.swap(false, Ordering::Relaxed) {
                return Err(StoreError::Rejected);
            }
        }
        let result = self.inner.compare_exchange(key, expected, bytes).await?;
        if paused && matches!(result, CasOutcome::Conflict(_)) {
            self.released_conflicts.fetch_add(1, Ordering::Relaxed);
        }
        if ready
            && matches!(result, CasOutcome::Applied(_))
            && self.lose_final_reply.swap(false, Ordering::Relaxed)
        {
            self.reject_after_unknown.store(true, Ordering::Relaxed);
            return Err(StoreError::Rejected);
        }
        Ok(result)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

#[derive(serde::Serialize)]
pub struct TestVersioned<T> {
    pub version: u16,
    pub payload: T,
}

pub fn snapshot_key(identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    [
        b"DS\0S\0".as_slice(),
        &identity.key().unwrap(),
        snapshot.as_bytes(),
    ]
    .concat()
}
pub fn index_key(identity: &DatasetIdentity, snapshot: SnapshotId, kind: u8) -> Vec<u8> {
    [
        b"DS\0I\0".as_slice(),
        &identity.key().unwrap(),
        snapshot.as_bytes(),
        &[kind],
    ]
    .concat()
}
pub fn manifest(snapshot: SnapshotId) -> ManifestRecord {
    ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: false,
            }],
        },
        samples: (0..3)
            .map(|id| SampleRecord {
                sample_id: vec![id, 0, 255],
                fields: vec![FieldRecord {
                    name: "value".into(),
                    value: FieldLocator::Tombstone,
                }],
            })
            .collect(),
    }
}
pub async fn fixture(
    opaque: bool,
) -> (
    Arc<TestLegacyStore>,
    Arc<DatasetAuthority>,
    DatasetIdentity,
    SnapshotId,
    Vec<u8>,
) {
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-index").unwrap();
    let store = Arc::new(TestLegacyStore::new(&identity));
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = SnapshotId::random();
    let publication = SnapshotPublication {
        snapshot,
        parent: None,
        operation: OperationId::random(),
        manifest: if opaque {
            b"opaque".to_vec()
        } else {
            bincode::serialize(&manifest(snapshot)).unwrap()
        },
        state: PublicationState::Published,
    };
    let bytes = bincode::serialize(&TestVersioned {
        version: 1,
        payload: SnapshotRecord { publication },
    })
    .unwrap();
    store
        .inner
        .compare_exchange(&snapshot_key(&identity, snapshot), None, &bytes)
        .await
        .unwrap();
    (store, authority, identity, snapshot, bytes)
}
pub fn old_progress(snapshot: SnapshotId, ready: bool) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "version": 1, "source_sha256": vec![0; 32],
        "progress": { "snapshot": snapshot, "revision": 7, "rows_written": 0, "state": if ready { "Ready" } else { "Preparing" } },
        "position": { "schema": 0, "sample": 0, "sample_written": false, "field": 0, "field_value": false, "rows": 0 },
    })).unwrap()
}
