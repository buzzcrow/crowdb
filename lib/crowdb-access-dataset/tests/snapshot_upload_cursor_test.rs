// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;

use bytes::Bytes;
use crowdb_access_dataset::{composition::*, split::SplitPolicy, *};
use md5::{Digest, Md5};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

struct TestCleanup {
    store: Arc<upload::TestStore>,
    clock: Arc<AtomicU64>,
    authority: DatasetAuthority,
    uploads: DatasetUploads,
    identity: DatasetIdentity,
    snapshot: SnapshotId,
    location: Vec<u8>,
}
impl TestCleanup {
    async fn new() -> Self {
        let store = Arc::new(upload::TestStore::default());
        let clock = Arc::new(AtomicU64::new(1000));
        let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone());
        let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
        let identity = DatasetIdentity::new(NamespacePath::root(), "local-cleanup").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
        let digest = Md5::digest(b"data").into();
        uploads.create(&location, 4, 4, digest, u64::MAX).await.unwrap();
        uploads
            .put_part(&location, 0, Bytes::from_static(b"data"), 1)
            .await
            .unwrap();
        uploads.complete(&location, 2).await.unwrap();
        let snapshot = SnapshotId::random();
        let request = CompositionRequest {
            parent: None,
            source: CompositionSource::All,
            schema: Some(SchemaRecord {
                version: 1,
                fields: vec![FieldDefinition {
                    name: "data".into(),
                    required: true,
                }],
            }),
            samples: (0..51)
                .map(|id| {
                    SampleChange::Upsert(SampleRecord {
                        sample_id: vec![id],
                        fields: vec![FieldRecord {
                            name: "data".into(),
                            value: FieldLocator::Chunk {
                                location: location.clone(),
                                length: 4,
                                md5: digest,
                            },
                        }],
                    })
                })
                .collect(),
            splits: Vec::new(),
            policy: SplitPolicy::default(),
        };
        authority
            .prepare_composition_resource(
                &identity,
                snapshot,
                &request,
                Some(&uploads),
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        authority
            .mark_snapshot_reclaim(&identity, snapshot)
            .await
            .unwrap();
        clock.store(2200, Ordering::Release);
        Self {
            store,
            clock,
            authority,
            uploads,
            identity,
            snapshot,
            location,
        }
    }
}

#[tokio::test]
async fn owner_cleanup_uses_only_local_progress_and_restart_preserves_shared_owners() {
    let test = TestCleanup::new().await;
    let owner = OperationId::random();
    test.uploads.claim(&test.location, owner, 3).await.unwrap();
    let mut legacy_key = b"DS\0GUP\0".to_vec();
    legacy_key.extend(test.identity.key().unwrap());
    legacy_key.extend(test.snapshot.as_bytes());
    test.store
        .compare_exchange(&legacy_key, None, b"obsolete corrupt progress")
        .await
        .unwrap();
    let mut cursor = SnapshotUploadCleanup::default();
    let step = test
        .authority
        .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &test.uploads, &mut cursor)
        .await
        .unwrap();
    assert!(!step.complete);
    assert!(step.retired_references > 0 && step.retired_references <= 20);
    let restarted = DatasetAuthority::new(test.store.clone()).with_reclaim_clock_for_tests(test.clock);
    cursor = SnapshotUploadCleanup::default();
    for _ in 0..20 {
        if restarted
            .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &test.uploads, &mut cursor)
            .await
            .unwrap()
            .complete
        {
            break;
        }
    }
    assert!(cursor.complete);
    assert_eq!(cursor.retired_references, 51);
    assert_eq!(
        test.store.get(&legacy_key).await.unwrap().unwrap().bytes,
        b"obsolete corrupt progress"
    );
    assert!(matches!(
        test.uploads.abort(&test.location).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(
        test.uploads.read_part(&test.location, 0).await.unwrap(),
        b"data".as_slice()
    );
}

#[tokio::test]
async fn worker_cursor_survives_cancel_and_can_be_lost_between_cleanup_windows() {
    let test = TestCleanup::new().await;
    let mut cursor = SnapshotReclaimSweep::default();
    test.authority
        .reclaim_due_page(&test.uploads, &mut cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(cursor.pending_snapshots(), 1);
    let cancel = ReadCancellation::new();
    cancel.cancel();
    let writes = test.store.mutations.load(Ordering::Relaxed);
    assert!(test
        .authority
        .reclaim_due_page(&test.uploads, &mut cursor, &cancel)
        .await
        .is_err());
    assert_eq!(test.store.mutations.load(Ordering::Relaxed), writes);
    let restarted = DatasetAuthority::new(test.store.clone()).with_reclaim_clock_for_tests(test.clock);
    let mut cursors = [SnapshotReclaimSweep::default(), SnapshotReclaimSweep::default()];
    for turn in 0..100 {
        restarted
            .reclaim_due_page(&test.uploads, &mut cursors[turn % 2], &ReadCancellation::new())
            .await
            .unwrap();
        assert!(cursors.iter().all(|cursor| cursor.pending_snapshots() <= 32));
        if restarted
            .inspect_snapshot_lifecycle(&test.identity, test.snapshot, &ReadCancellation::new())
            .await
            .unwrap()
            .completed
        {
            return;
        }
    }
    panic!("cleanup did not finish after local cursor loss and concurrent workers");
}

#[tokio::test]
async fn index_and_metadata_positions_can_be_lost_between_cleanup_windows() {
    let test = TestCleanup::new().await;
    let mut cursor = SnapshotReclaimCursor::new(test.identity.clone(), test.snapshot);
    let mut lost_indexes = false;
    let mut lost_metadata = false;
    for _ in 0..100 {
        let status = test
            .authority
            .advance_snapshot_reclaim(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if status.complete {
            assert!(lost_indexes && lost_metadata);
            for prefix in [b"DS\0GUP\0".as_slice(), b"DS\0GMC\0", b"DS\0GSD\0", b"DS\0CGF\0"] {
                let mut key = prefix.to_vec();
                key.extend(test.identity.key().unwrap());
                key.extend(test.snapshot.as_bytes());
                assert!(test.store.get(&key).await.unwrap().is_none());
            }
            return;
        }
        if status.phase == SnapshotReclaimPhase::IndexFences && !lost_indexes {
            lost_indexes = true;
            cursor = SnapshotReclaimCursor::new(test.identity.clone(), test.snapshot);
        }
        if status.phase == SnapshotReclaimPhase::Metadata && !lost_metadata {
            lost_metadata = true;
            cursor = SnapshotReclaimCursor::new(test.identity.clone(), test.snapshot);
        }
    }
    panic!("lost index and metadata positions prevented cleanup completion");
}
