// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/legacy_drafts.rs"]
#[allow(dead_code)]
mod legacy;
#[path = "common/legacy_composition_upload.rs"]
mod legacy_upload;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/composition_resource.rs"]
mod resource;
#[path = "common/split_store.rs"]
mod split_store;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use crowdb_access_dataset::*;
use legacy_upload::TestComposition;

#[tokio::test]
async fn legacy_staging_cleanup_preserves_the_complete_resource_owner_and_payload() {
    let f = TestComposition::new().await;
    assert!(matches!(
        f.uploads.abort(&f.location).await,
        Err(UploadError::Conflict)
    ));
    let snapshot = f.prepare().await;
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_ok());
    assert!(
        f.authority
            .composition_upload_status(&f.identity, f.operation)
            .await
            .unwrap()
            .publication_ready
    );
    let cleanup = f.cleanup().await;
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_owners, 1);
    f.publish(snapshot).await;
    assert_eq!(f.cleanup().await, cleanup);
    assert!(matches!(
        f.uploads.abort(&f.location).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(
        f.uploads
            .read_part_verified(&f.location, 0)
            .await
            .unwrap()
            .0
            .as_ref(),
        b"data"
    );
    assert_eq!(
        f.authority.inspect_head(&f.identity).await.unwrap().latest,
        Some(snapshot)
    );
}

#[tokio::test]
async fn cancelling_complete_composition_retires_staging_and_preserves_resource_owner() {
    let f = TestComposition::new().await;
    let snapshot = f.prepare().await;
    f.authority
        .abandon_composition(&f.identity, f.operation, 1)
        .await
        .unwrap();
    let cleanup = f.cleanup().await;
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_owners, 1);
    assert_eq!(f.cleanup().await, cleanup);
    assert!(matches!(
        f.uploads.abort(&f.location).await,
        Err(UploadError::Conflict)
    ));
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_ok());
}

#[tokio::test]
async fn cancelling_editing_composition_releases_staged_upload_without_candidate() {
    let f = TestComposition::new().await;
    f.authority
        .abandon_composition(&f.identity, f.operation, 1)
        .await
        .unwrap();
    let cleanup = f.cleanup().await;
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_owners, 1);
    assert!(f.uploads.abort(&f.location).await.unwrap().cleanup_complete);
}

#[tokio::test]
async fn child_candidate_claims_inherited_upload_and_cancellation_preserves_parent_owner() {
    let f = TestComposition::new().await;
    let parent = f.prepare().await;
    f.publish(parent).await;
    assert!(f.cleanup().await.complete);
    let operation = OperationId::random();
    legacy::editing_composition(f.store.as_ref(), &f.authority, &f.identity, operation, parent).await;
    let child = SnapshotId::random();
    f.authority
        .prepare_composition_resource(
            &f.identity,
            child,
            &common::request(parent),
            Some(&f.uploads),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    f.attach(operation, child, vec![]).await;
    f.authority
        .abandon_composition(&f.identity, operation, 1)
        .await
        .unwrap();
    let cleanup = f
        .authority
        .cleanup_composition_uploads_step(&f.identity, operation, &f.uploads)
        .await
        .unwrap();
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_owners, 0);
    assert!(matches!(
        f.uploads.abort(&f.location).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(
        f.uploads
            .read_part_verified(&f.location, 0)
            .await
            .unwrap()
            .0
            .as_ref(),
        b"data"
    );
    assert_eq!(
        f.authority.inspect_head(&f.identity).await.unwrap().latest,
        Some(parent)
    );
}

#[tokio::test]
async fn reclaimed_snapshot_retires_its_upload_owner_after_index_cleanup() {
    let mut f = TestComposition::new().await;
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1000));
    f.authority = f.authority.with_reclaim_clock_for_tests(clock.clone());
    let snapshot = f.prepare().await;
    f.publish(snapshot).await;
    assert!(f.cleanup().await.complete);
    let record = f.authority.get_snapshot(&f.identity, snapshot).await.unwrap();
    let mut upload_cursor = SnapshotUploadCleanup::default();
    let other_owner = OperationId::random();
    f.uploads.claim(&f.location, other_owner, 3).await.unwrap();
    assert!(matches!(
        f.authority
            .reclaim_snapshot_uploads_step(&f.identity, snapshot, &f.uploads, &mut upload_cursor)
            .await,
        Err(UploadReclaimError::Authority(AuthorityError::SnapshotProtected))
    ));
    let root = f
        .authority
        .prepare_snapshot_candidate(
            &f.identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    f.authority
        .publish_snapshot_candidate(
            &f.identity,
            &common::attempt(&f.authority, &f.identity, root).await,
        )
        .await
        .unwrap();
    f.authority.begin_reclaim(&f.identity, snapshot).await.unwrap();
    clock.store(1300, std::sync::atomic::Ordering::Release);
    f.authority
        .reclaim_snapshot_metadata_for_tests(&f.identity, snapshot)
        .await
        .unwrap();
    let progress = f
        .authority
        .reclaim_snapshot_uploads_step(&f.identity, snapshot, &f.uploads, &mut upload_cursor)
        .await
        .unwrap();
    assert!(progress.complete);
    assert_eq!(progress.retired_references, 1);
    let mut retired = false;
    let mut cursor = SnapshotReclaimCursor::new(f.identity.clone(), snapshot);
    for _ in 0..100 {
        let status = f
            .authority
            .advance_snapshot_reclaim(&f.uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if status.complete {
            assert_eq!(status.phase, SnapshotReclaimPhase::Complete);
            assert!(status.physical_cleanup_deferred);
            retired = true;
            break;
        }
    }
    assert!(retired);
    assert_eq!(
        f.authority
            .reclaim_snapshot_uploads_step(&f.identity, snapshot, &f.uploads, &mut upload_cursor)
            .await
            .unwrap(),
        progress
    );
    assert!(matches!(
        f.uploads.abort(&f.location).await,
        Err(UploadError::Conflict)
    ));
    assert!(matches!(
        f.uploads
            .claim(&f.location, record.publication.operation, 4)
            .await,
        Err(UploadError::Conflict)
    ));
    f.uploads.retire_owner(&f.location, other_owner).await.unwrap();
    assert!(f.uploads.abort(&f.location).await.unwrap().cleanup_complete);
}
