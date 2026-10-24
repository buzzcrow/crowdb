// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/legacy.rs"]
mod legacy;
#[path = "common/publication.rs"]
mod publication;

use crowdb_access_dataset::*;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

async fn fixture() -> (
    Arc<access_store::TestAccessStore>,
    DatasetIdentity,
    SnapshotId,
    SnapshotId,
) {
    let store = access_store::TestAccessStore::shared();
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-read").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let root = authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(root), publication::publication_manifest(2))
        .await
        .unwrap();
    legacy::emulate_legacy(store.as_ref(), &authority, &identity, &[root, child], child).await;
    (store, identity, root, child)
}

fn budget(bytes: usize) -> MetadataScanBudget {
    MetadataScanBudget {
        items: 20,
        bytes,
        timeout: Duration::from_secs(2),
    }
}

#[tokio::test]
async fn repeated_legacy_authority_and_inspection_reads_never_fill_visibility_records() {
    let (store, identity, root, child) = fixture().await;
    let writes = store.writes.load(Ordering::Relaxed);
    for _ in 0..2 {
        let authority = DatasetAuthority::new(store.clone());
        for snapshot in [root, child] {
            authority
                .require_committed_snapshot(&identity, snapshot)
                .await
                .unwrap();
            let summary = authority
                .snapshot_inspection_summary(&identity, snapshot, budget(1 << 20), &ReadCancellation::new())
                .await
                .unwrap();
            assert_eq!(summary.snapshot, snapshot);
            let key = [
                b"DS\0LC\0".as_slice(),
                identity.key().unwrap().as_slice(),
                snapshot.as_bytes(),
            ]
            .concat();
            assert!(store.get(&key).await.unwrap().is_none());
        }
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

async fn replace_snapshot(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    update: impl FnOnce(&mut SnapshotRecord),
) {
    let key = [
        b"DS\0S\0".as_slice(),
        identity.key().unwrap().as_slice(),
        snapshot.as_bytes(),
    ]
    .concat();
    let stored = store.get(&key).await.unwrap().unwrap();
    let (version, mut record): (u16, SnapshotRecord) = bincode::deserialize(&stored.bytes).unwrap();
    update(&mut record);
    assert!(matches!(
        store
            .compare_exchange(
                &key,
                Some(&stored.bytes),
                &bincode::serialize(&(version, record)).unwrap()
            )
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn legacy_fallback_rejects_unpublished_corrupt_and_cyclic_authority_without_writes() {
    for case in 0..4 {
        let (store, identity, _, child) = fixture().await;
        replace_snapshot(store.as_ref(), &identity, child, |record| match case {
            0 => record.publication.state = PublicationState::Prepared,
            1 => record.publication.state = PublicationState::Aborted,
            2 => record.publication.snapshot = SnapshotId::random(),
            _ => record.publication.parent = Some(child),
        })
        .await;
        let target = if case == 3 { SnapshotId::random() } else { child };
        let authority = DatasetAuthority::new(store.clone());
        let writes = store.writes.load(Ordering::Relaxed);
        assert!(
            authority
                .require_committed_snapshot(&identity, target)
                .await
                .is_err(),
            "case {case}"
        );
        assert!(
            authority
                .snapshot_inspection_summary(&identity, target, budget(1 << 20), &ReadCancellation::new())
                .await
                .is_err(),
            "case {case}"
        );
        assert_eq!(store.writes.load(Ordering::Relaxed), writes);
    }
}

#[tokio::test]
async fn legacy_inspection_fallback_obeys_aggregate_budget_and_cancellation() {
    let (store, identity, root, _) = fixture().await;
    let authority = DatasetAuthority::new(store.clone());
    let writes = store.writes.load(Ordering::Relaxed);
    assert!(authority
        .snapshot_inspection_summary(&identity, root, budget(32), &ReadCancellation::new())
        .await
        .is_err());
    let cancelled = ReadCancellation::new();
    cancelled.cancel();
    assert!(authority
        .snapshot_inspection_summary(&identity, root, budget(1 << 20), &cancelled)
        .await
        .is_err());
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn unpublished_modern_candidate_cannot_fall_back_to_legacy_visibility() {
    let (store, identity, _, _) = fixture().await;
    let authority = DatasetAuthority::new(store.clone());
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    assert!(authority
        .require_committed_snapshot(&identity, snapshot)
        .await
        .is_err());
    assert!(authority
        .snapshot_inspection_summary(&identity, snapshot, budget(1 << 20), &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}
