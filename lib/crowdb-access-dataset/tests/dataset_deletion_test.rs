// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/access_store.rs"]
mod store;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

fn identity(name: &str) -> DatasetIdentity {
    DatasetIdentity::new(NamespacePath::root(), name).unwrap()
}

#[tokio::test]
async fn deletion_persists_deadline_and_fences_name_reuse() {
    let store = store::TestAccessStore::shared();
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone());
    let name = identity("empty");
    let cancel = ReadCancellation::new();
    let id = authority.create_dataset(name.clone()).await.unwrap();
    let mark = authority.delete_dataset(&name, id, &cancel).await.unwrap();
    assert_eq!(mark.reclaim_after, 2200);
    assert!(!mark.complete);
    assert!(matches!(
        authority.get_dataset(&name).await,
        Err(AuthorityError::NotFound)
    ));
    assert!(matches!(
        authority.create_dataset(name.clone()).await,
        Err(AuthorityError::AlreadyExists)
    ));
    let restarted = DatasetAuthority::new(store)
        .with_reclaim_grace_seconds(1)
        .with_reclaim_clock_for_tests(clock.clone());
    clock.store(2199, Ordering::Release);
    restarted
        .reclaim_datasets_page(&mut DatasetDeletionSweep::default(), &cancel)
        .await
        .unwrap();
    assert_eq!(restarted.delete_dataset(&name, id, &cancel).await.unwrap(), mark);
    clock.store(2200, Ordering::Release);
    restarted
        .reclaim_datasets_page(&mut DatasetDeletionSweep::default(), &cancel)
        .await
        .unwrap();
    assert!(
        restarted
            .dataset_deletion(&name, id)
            .await
            .unwrap()
            .unwrap()
            .complete
    );
    let new_id = restarted.create_dataset(name.clone()).await.unwrap();
    assert_ne!(new_id, id);
    assert!(matches!(
        restarted.delete_dataset(&name, id, &cancel).await,
        Err(AuthorityError::HeadConflict)
    ));
    assert_eq!(restarted.get_dataset(&name).await.unwrap().id, new_id);
    assert_eq!(
        restarted
            .delete_dataset(&name, new_id, &cancel)
            .await
            .unwrap()
            .reclaim_after,
        2201
    );
}

#[tokio::test]
async fn snapshots_and_uploads_block_empty_delete() {
    let store = store::TestAccessStore::shared();
    let authority = DatasetAuthority::new(store.clone());
    let uploads = DatasetUploads::new(store, Arc::new(upload::TestPayload::default()));
    let cancel = ReadCancellation::new();
    for published in [false, true] {
        let name = identity(if published { "published" } else { "candidate" });
        let id = authority.create_dataset(name.clone()).await.unwrap();
        if published {
            authority
                .publish_snapshot(&name, None, publication::publication_manifest(1))
                .await
                .unwrap();
        } else {
            authority
                .prepare_snapshot_candidate(
                    &name,
                    None,
                    publication::publication_manifest(1),
                    OperationId::random(),
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            authority.delete_dataset(&name, id, &cancel).await,
            Err(AuthorityError::DatasetNotEmpty)
        ));
    }
    let name = identity("upload");
    let id = authority.create_dataset(name.clone()).await.unwrap();
    let operation = OperationId::random();
    authority.reserve_upload_creation(&name, operation).await.unwrap();
    let key = DatasetUploads::location(&name, operation).unwrap();
    uploads.create(&key, 4, 4, [1; 16], u64::MAX).await.unwrap();
    assert!(matches!(
        authority.delete_dataset(&name, id, &cancel).await,
        Err(AuthorityError::DatasetNotEmpty)
    ));
}

#[tokio::test]
async fn abandoned_creation_is_fenced_before_name_can_be_reused() {
    let store = store::TestAccessStore::shared();
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(store.clone())
        .with_reclaim_grace_seconds(1)
        .with_reclaim_clock_for_tests(clock.clone());
    let uploads = DatasetUploads::new(store, Arc::new(upload::TestPayload::default()));
    let name = identity("interrupted");
    let cancel = ReadCancellation::new();
    let id = authority.create_dataset(name.clone()).await.unwrap();
    let operation = OperationId::random();
    authority.reserve_upload_creation(&name, operation).await.unwrap();
    assert!(matches!(
        authority.delete_dataset(&name, id, &cancel).await,
        Err(AuthorityError::DatasetNotEmpty)
    ));
    // Creation reservations have their own twenty-minute lifetime, independent of delete grace.
    clock.store(2200, Ordering::Release);
    authority.delete_dataset(&name, id, &cancel).await.unwrap();
    clock.store(2201, Ordering::Release);
    authority
        .reclaim_datasets_page(&mut DatasetDeletionSweep::default(), &cancel)
        .await
        .unwrap();
    authority.create_dataset(name.clone()).await.unwrap();
    let key = DatasetUploads::location(&name, operation).unwrap();
    assert!(matches!(
        uploads.create(&key, 4, 4, [1; 16], u64::MAX).await,
        Err(UploadError::Conflict)
    ));
    assert!(authority.reserve_upload_creation(&name, operation).await.is_err());
}

#[tokio::test]
async fn concurrent_creation_and_delete_cannot_both_win() {
    let store = store::TestAccessStore::shared();
    let authority = DatasetAuthority::new(store);
    let cancel = ReadCancellation::new();
    for index in 0..32 {
        let name = identity(&format!("race-{index}"));
        let id = authority.create_dataset(name.clone()).await.unwrap();
        let (create, delete) = tokio::join!(
            authority.reserve_upload_creation(&name, OperationId::random()),
            authority.delete_dataset(&name, id, &cancel)
        );
        assert_ne!(
            create.is_ok(),
            delete.is_ok(),
            "exactly one root CAS must win: {create:?}, {delete:?}"
        );
    }
}

#[tokio::test]
async fn failed_mark_and_cleanup_resume_without_releasing_the_name_early() {
    let store = store::TestAccessStore::shared();
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(store.clone())
        .with_reclaim_grace_seconds(1)
        .with_reclaim_clock_for_tests(clock.clone());
    let name = identity("failures");
    let cancel = ReadCancellation::new();
    let id = authority.create_dataset(name.clone()).await.unwrap();
    store.failed_dataset_writes.store(1, Ordering::Release);
    assert!(matches!(
        authority.delete_dataset(&name, id, &cancel).await,
        Err(AuthorityError::Store(_))
    ));
    assert_eq!(authority.get_dataset(&name).await.unwrap().id, id);
    let mark = authority.delete_dataset(&name, id, &cancel).await.unwrap();
    clock.store(1001, Ordering::Release);
    store.failed_dataset_writes.store(1, Ordering::Release);
    assert!(authority
        .reclaim_datasets_page(&mut DatasetDeletionSweep::default(), &cancel)
        .await
        .is_err());
    assert_eq!(
        authority.dataset_deletion(&name, id).await.unwrap().unwrap(),
        mark
    );
    assert!(matches!(
        authority.create_dataset(name.clone()).await,
        Err(AuthorityError::AlreadyExists)
    ));
    let restarted = DatasetAuthority::new(store).with_reclaim_clock_for_tests(clock);
    restarted
        .reclaim_datasets_page(&mut DatasetDeletionSweep::default(), &cancel)
        .await
        .unwrap();
    assert_ne!(restarted.create_dataset(name).await.unwrap(), id);
}

#[tokio::test]
async fn reclaimed_candidate_allows_dataset_delete_and_uses_configured_grace() {
    let store = store::TestAccessStore::shared();
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(store.clone())
        .with_reclaim_grace_seconds(1)
        .with_reclaim_clock_for_tests(clock.clone());
    let uploads = DatasetUploads::new(store, Arc::new(upload::TestPayload::default()));
    let name = identity("reclaimed");
    let cancel = ReadCancellation::new();
    let id = authority.create_dataset(name.clone()).await.unwrap();
    let snapshot = authority
        .prepare_snapshot_candidate(
            &name,
            None,
            publication::publication_manifest(1),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority.mark_snapshot_reclaim(&name, snapshot).await.unwrap();
    assert_eq!(
        authority.snapshot_reclaim_after(&name, snapshot).await.unwrap(),
        1001
    );
    assert!(matches!(
        authority.delete_dataset(&name, id, &cancel).await,
        Err(AuthorityError::DatasetNotEmpty)
    ));
    clock.store(1001, Ordering::Release);
    let mut cursor = SnapshotReclaimCursor::new(name.clone(), snapshot);
    for _ in 0..100 {
        if authority
            .advance_snapshot_reclaim(&uploads, &mut cursor, &cancel)
            .await
            .unwrap()
            .complete
        {
            assert_eq!(
                authority
                    .delete_dataset(&name, id, &cancel)
                    .await
                    .unwrap()
                    .reclaim_after,
                1002
            );
            return;
        }
    }
    panic!("Snapshot cleanup did not finish");
}

#[tokio::test]
async fn corrupt_root_does_not_starve_other_due_datasets() {
    let store = store::TestAccessStore::shared();
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(store.clone())
        .with_reclaim_grace_seconds(1)
        .with_reclaim_clock_for_tests(clock.clone());
    let bad = identity("a");
    let good = identity("b");
    let cancel = ReadCancellation::new();
    let mut bad_key = b"DS\0D\0".to_vec();
    bad_key.extend(bad.key().unwrap());
    store.compare_exchange(&bad_key, None, b"corrupt").await.unwrap();
    let id = authority.create_dataset(good.clone()).await.unwrap();
    authority.delete_dataset(&good, id, &cancel).await.unwrap();
    clock.store(1001, Ordering::Release);
    let mut cursor = DatasetDeletionSweep::default();
    assert!(authority
        .reclaim_datasets_page(&mut cursor, &cancel)
        .await
        .is_err());
    authority
        .reclaim_datasets_page(&mut cursor, &cancel)
        .await
        .unwrap();
    assert!(
        authority
            .dataset_deletion(&good, id)
            .await
            .unwrap()
            .unwrap()
            .complete
    );
}
