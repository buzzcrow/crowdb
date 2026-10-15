// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use bytes::Bytes;
use crowdb_access_dataset::{composition::*, *};
use md5::{Digest, Md5};
use std::sync::Arc;

struct TestComposition {
    authority: DatasetAuthority,
    identity: DatasetIdentity,
    uploads: DatasetUploads,
    operation: OperationId,
    location: Vec<u8>,
    edit: CompositionEdit,
}
impl TestComposition {
    async fn new() -> Self {
        let (store, authority, identity, parent) = split_store::fixture().await;
        let uploads = DatasetUploads::new(store, Arc::new(upload::TestPayload::default()));
        let operation = OperationId::random();
        authority
            .begin_composition(&identity, operation, &common::input(parent))
            .await
            .unwrap();
        authority
            .manage_composition_uploads(&identity, operation)
            .await
            .unwrap();
        let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
        uploads
            .create(&location, 4, 4, Md5::digest(b"data").into(), u64::MAX)
            .await
            .unwrap();
        uploads
            .put_part(&location, 0, Bytes::from_static(b"data"), 1)
            .await
            .unwrap();
        uploads.complete(&location, 2).await.unwrap();
        let edit = common::edit(
            1,
            vec![SampleChange::Upsert(SampleRecord {
                sample_id: b"uploaded".to_vec(),
                fields: vec![FieldRecord {
                    name: "marker".into(),
                    value: FieldLocator::Chunk {
                        location: location.clone(),
                        length: 4,
                        md5: Md5::digest(b"data").into(),
                    },
                }],
            })],
        );
        authority
            .edit_composition_with_uploads(&identity, operation, &edit, &uploads)
            .await
            .unwrap();
        Self {
            authority,
            identity,
            uploads,
            operation,
            location,
            edit,
        }
    }
    async fn prepare(&self) -> SnapshotId {
        common::evaluate(&self.authority, &self.identity, self.operation, 2).await;
        common::prepare(&self.authority, &self.identity, self.operation, 2)
            .await
            .preparation
            .unwrap()
            .candidate
    }
    async fn own(&self) {
        for _ in 0..10 {
            if self
                .authority
                .prepare_composition_uploads(
                    &self.identity,
                    self.operation,
                    &self.uploads,
                    &ReadCancellation::new(),
                )
                .await
                .unwrap()
                .publication_ready
            {
                return;
            }
        }
        panic!("upload ownership did not finish");
    }
    async fn cleanup(&self) -> CompositionUploadCleanup {
        self.authority
            .cleanup_composition_uploads_step(&self.identity, self.operation, &self.uploads)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn publication_requires_candidate_owners_and_preserves_payload_after_staging_cleanup() {
    let f = TestComposition::new().await;
    assert!(matches!(
        f.uploads.abort(&f.location).await,
        Err(UploadError::Conflict)
    ));
    let snapshot = f.prepare().await;
    let attempt = common::attempt(&f.authority, &f.identity, snapshot).await;
    assert!(matches!(
        f.authority
            .publish_composition(&f.identity, f.operation, &attempt)
            .await,
        Err(CompositionError::Conflict)
    ));
    f.own().await;
    f.authority
        .publish_composition(&f.identity, f.operation, &attempt)
        .await
        .unwrap();
    let cleanup = f.cleanup().await;
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_owners, 1);
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
    // Historical edit retries use the receipt even after the staging owner is revoked.
    assert_eq!(
        f.authority
            .edit_composition_with_uploads(&f.identity, f.operation, &f.edit, &f.uploads)
            .await
            .unwrap()
            .record
            .revision,
        2
    );
    assert_eq!(
        f.authority.inspect_head(&f.identity).await.unwrap().latest,
        Some(snapshot)
    );
}

#[tokio::test]
async fn cancelling_prepared_composition_retires_both_owners_and_is_restartable() {
    let f = TestComposition::new().await;
    f.prepare().await;
    f.own().await;
    f.authority
        .abandon_composition(&f.identity, f.operation, 2)
        .await
        .unwrap();
    let cleanup = f.cleanup().await;
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_owners, 2);
    assert_eq!(f.cleanup().await, cleanup);
    assert!(f.uploads.abort(&f.location).await.unwrap().cleanup_complete);
    assert!(f
        .authority
        .prepare_composition_uploads(&f.identity, f.operation, &f.uploads, &ReadCancellation::new())
        .await
        .is_err());
}

#[tokio::test]
async fn cancelling_editing_composition_releases_staged_upload_without_candidate() {
    let f = TestComposition::new().await;
    f.authority
        .abandon_composition(&f.identity, f.operation, 2)
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
    f.own().await;
    let attempt = common::attempt(&f.authority, &f.identity, parent).await;
    f.authority
        .publish_composition(&f.identity, f.operation, &attempt)
        .await
        .unwrap();
    assert!(f.cleanup().await.complete);
    let child = OperationId::random();
    f.authority
        .begin_composition(&f.identity, child, &common::input(parent))
        .await
        .unwrap();
    f.authority
        .manage_composition_uploads(&f.identity, child)
        .await
        .unwrap();
    common::evaluate(&f.authority, &f.identity, child, 1).await;
    common::prepare(&f.authority, &f.identity, child, 1).await;
    let owned = f
        .authority
        .prepare_composition_uploads(&f.identity, child, &f.uploads, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(owned.publication_ready);
    assert_eq!(owned.staged_uploads, 0);
    assert_eq!(owned.candidate_uploads, 1);
    f.authority
        .abandon_composition(&f.identity, child, 1)
        .await
        .unwrap();
    let cleanup = f
        .authority
        .cleanup_composition_uploads_step(&f.identity, child, &f.uploads)
        .await
        .unwrap();
    assert!(cleanup.complete);
    assert_eq!(cleanup.retired_owners, 1);
    assert!(matches!(
        f.uploads.abort(&f.location).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(
        f.authority.inspect_head(&f.identity).await.unwrap().latest,
        Some(parent)
    );
}

#[tokio::test]
async fn reclaimed_snapshot_retires_its_upload_owner_after_index_cleanup() {
    let f = TestComposition::new().await;
    let snapshot = f.prepare().await;
    f.own().await;
    let attempt = common::attempt(&f.authority, &f.identity, snapshot).await;
    f.authority
        .publish_composition(&f.identity, f.operation, &attempt)
        .await
        .unwrap();
    assert!(f.cleanup().await.complete);
    let record = f.authority.get_snapshot(&f.identity, snapshot).await.unwrap();
    let other_owner = OperationId::random();
    f.uploads.claim(&f.location, other_owner, 3).await.unwrap();
    assert!(matches!(
        f.authority
            .reclaim_snapshot_uploads_step(&f.identity, snapshot, &f.uploads)
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
    f.authority
        .reclaim_snapshot_metadata(&f.identity, snapshot)
        .await
        .unwrap();
    let progress = f
        .authority
        .reclaim_snapshot_uploads_step(&f.identity, snapshot, &f.uploads)
        .await
        .unwrap();
    assert!(progress.complete);
    assert_eq!(progress.retired_references, 1);
    let mut retired = false;
    for _ in 0..100 {
        let status = f
            .authority
            .reclaim_snapshot_step(&f.identity, snapshot, &f.uploads, &ReadCancellation::new())
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
            .reclaim_snapshot_uploads_step(&f.identity, snapshot, &f.uploads)
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
