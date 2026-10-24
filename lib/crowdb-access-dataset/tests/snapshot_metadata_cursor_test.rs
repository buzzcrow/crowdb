// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/publication.rs"]
mod publication;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;

use async_trait::async_trait;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

const CHECKPOINTS: [&[u8]; 4] = [b"DS\0GUP\0", b"DS\0GMC\0", b"DS\0GSD\0", b"DS\0CGF\0"];

struct TestFenceFailure {
    inner: Arc<upload::TestStore>,
    target: Vec<u8>,
    fail: AtomicBool,
}
#[async_trait]
impl DatasetStore for TestFenceFailure {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
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
        if key == self.target && self.fail.swap(false, Ordering::AcqRel) {
            return Err(StoreError::Failure("injected fence write failure".into()));
        }
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}

fn scoped(prefix: &[u8], identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend(identity.key().unwrap());
    key.extend(snapshot.as_bytes());
    key
}

async fn finish(
    authority: &DatasetAuthority,
    uploads: &DatasetUploads,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) {
    let mut cursor = SnapshotReclaimCursor::new(identity.clone(), snapshot);
    for _ in 0..32 {
        if authority
            .advance_snapshot_reclaim(uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("local metadata cleanup did not complete");
}

#[tokio::test]
async fn failed_index_fence_retries_the_same_row_without_a_checkpoint_write() {
    let store = Arc::new(upload::TestStore::default());
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "failed-fence").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(1),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .mark_snapshot_reclaim(&identity, snapshot)
        .await
        .unwrap();
    let mut target = scoped(b"DS\0I\0", &identity, snapshot);
    target.push(b'M');
    target.extend(b"sample-1");
    target.extend([0, 0]);
    assert!(store.get(&target).await.unwrap().is_some());
    let fault = Arc::new(TestFenceFailure {
        inner: store.clone(),
        target: target.clone(),
        fail: AtomicBool::new(true),
    });
    let authority = DatasetAuthority::new(fault.clone()).with_reclaim_clock_for_tests(clock.clone());
    let uploads = DatasetUploads::new(fault, Arc::new(upload::TestPayload::default()));
    clock.store(1300, Ordering::Release);
    for prefix in CHECKPOINTS {
        store
            .compare_exchange(
                &scoped(prefix, &identity, snapshot),
                None,
                b"obsolete corrupt progress",
            )
            .await
            .unwrap();
    }
    let mut cursor = SnapshotReclaimCursor::new(identity.clone(), snapshot);
    assert!(authority
        .advance_snapshot_reclaim(&uploads, &mut cursor, &ReadCancellation::new())
        .await
        .is_err());
    assert_ne!(
        store.get(&target).await.unwrap().unwrap().bytes,
        b"snapshot-metadata-retired-v1"
    );
    for _ in 0..32 {
        if authority
            .advance_snapshot_reclaim(&uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            assert_eq!(
                store.get(&target).await.unwrap().unwrap().bytes,
                b"snapshot-metadata-retired-v1"
            );
            for prefix in CHECKPOINTS {
                assert_eq!(
                    store
                        .get(&scoped(prefix, &identity, snapshot))
                        .await
                        .unwrap()
                        .unwrap()
                        .bytes,
                    b"obsolete corrupt progress"
                );
            }
            return;
        }
    }
    panic!("index fence failure did not recover");
}

async fn completed_snapshots(
    authority: &DatasetAuthority,
    uploads: &DatasetUploads,
    identity: &DatasetIdentity,
    clock: &AtomicU64,
) -> Vec<SnapshotId> {
    let mut completed = Vec::new();
    for marker in 1..=21 {
        let snapshot = authority
            .prepare_snapshot_candidate(
                identity,
                None,
                publication::publication_manifest(marker),
                OperationId::random(),
            )
            .await
            .unwrap();
        authority.mark_snapshot_reclaim(identity, snapshot).await.unwrap();
        completed.push(snapshot);
    }
    clock.store(1300, Ordering::Release);
    for snapshot in &completed {
        finish(authority, uploads, identity, *snapshot).await;
    }
    completed
}

#[tokio::test(start_paused = true)]
async fn obsolete_checkpoint_sweep_is_bounded_restartable_and_requires_completion() {
    let store = Arc::new(upload::TestStore::default());
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone());
    let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "obsolete-progress").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let completed = completed_snapshots(&authority, &uploads, &identity, &clock).await;
    let pending = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(22),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority.mark_snapshot_reclaim(&identity, pending).await.unwrap();
    let missing = SnapshotId::random();
    for snapshot in completed.iter().copied().chain([pending, missing]) {
        for prefix in CHECKPOINTS {
            store
                .compare_exchange(
                    &scoped(prefix, &identity, snapshot),
                    None,
                    b"obsolete corrupt progress",
                )
                .await
                .unwrap();
        }
    }
    let mut cursor = SnapshotReclaimSweep::default();
    let writes = store.mutations.load(Ordering::Relaxed);
    authority
        .reclaim_due_page(&uploads, &mut cursor, &ReadCancellation::new())
        .await
        .unwrap();
    let deleted = store.mutations.load(Ordering::Relaxed) - writes;
    assert!(
        (1..=20).contains(&deleted),
        "one bounded legacy page deleted {deleted} records"
    );
    assert!(cursor.has_more());
    // Discarding a continuation repeats only idempotent deletes and permanent fences.
    cursor = SnapshotReclaimSweep::default();
    for _ in 0..16 {
        authority
            .reclaim_due_page(&uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if !cursor.has_more() {
            break;
        }
    }
    assert!(!cursor.has_more());
    for prefix in CHECKPOINTS {
        for snapshot in &completed {
            assert!(store
                .get(&scoped(prefix, &identity, *snapshot))
                .await
                .unwrap()
                .is_none());
        }
        for snapshot in [pending, missing] {
            assert!(store
                .get(&scoped(prefix, &identity, snapshot))
                .await
                .unwrap()
                .is_some());
        }
    }
    let mut manifest = scoped(b"DS\0M\0", &identity, completed[0]);
    manifest.extend(0u32.to_be_bytes());
    assert_eq!(
        store.get(&manifest).await.unwrap().unwrap().bytes,
        b"snapshot-metadata-retired-v1"
    );
    let scans = store.scans.load(Ordering::Relaxed);
    authority
        .reclaim_due_page(&uploads, &mut cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(
        store.scans.load(Ordering::Relaxed) - scans,
        1,
        "empty legacy scans must wait for the idle interval"
    );
    tokio::time::advance(std::time::Duration::from_secs(15)).await;
    authority
        .reclaim_due_page(&uploads, &mut cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(store.scans.load(Ordering::Relaxed) - scans, 3);
}
