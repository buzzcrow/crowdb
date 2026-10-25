// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{retirement_store::*, *};

#[tokio::test]
async fn shared_upload_is_read_once_per_window_without_losing_references_or_shared_owners() {
    let test = TestCleanup::new().await;
    test.uploads.claim(&test.location, other(), 3).await.unwrap();
    let store = TestRetirementStore::new(test.store.clone());
    let uploads = store.uploads();
    let mut cursor = SnapshotUploadCleanup::default();
    test.authority
        .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &uploads, &mut cursor)
        .await
        .unwrap();
    assert_eq!(
        (cursor.sample, cursor.field, cursor.retired_references),
        (10, 0, 10)
    );
    assert!(!cursor.complete);
    assert_eq!(store.counts(), (1, 1));
    test.authority
        .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &uploads, &mut cursor)
        .await
        .unwrap();
    assert_eq!(
        (cursor.sample, cursor.field, cursor.retired_references),
        (20, 0, 20)
    );
    assert_eq!(
        store.counts(),
        (2, 1),
        "the next window must read the final owner fact again"
    );

    let restarted =
        DatasetAuthority::new(test.store.clone()).with_reclaim_clock_for_tests(test.clock.clone());
    let uploads = store.uploads();
    cursor = SnapshotUploadCleanup::default();
    for _ in 0..6 {
        restarted
            .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &uploads, &mut cursor)
            .await
            .unwrap();
    }
    assert!(cursor.complete);
    assert_eq!(cursor.retired_references, 51);
    assert_eq!(
        store.counts(),
        (8, 1),
        "each restarted window independently checks its upload"
    );
    assert!(matches!(
        test.uploads.abort(&test.location).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(
        test.uploads.read_part(&test.location, 0).await.unwrap(),
        b"data".as_slice()
    );
    let owner = OperationId::from_bytes(test.snapshot.as_bytes()).unwrap();
    assert!(matches!(
        test.uploads.claim(&test.location, owner, 3).await,
        Err(UploadError::Conflict)
    ));
    test.uploads.release(&test.location, other()).await.unwrap();
    test.uploads.abort(&test.location).await.unwrap();
}

#[tokio::test]
async fn shared_upload_saturation_fence_is_rechecked_after_each_window() {
    for (version, absent, exchanges) in [(4, false, 1), (4, true, 1), (5, true, 0)] {
        let test = TestCleanup::new().await;
        test.uploads.claim(&test.location, other(), 3).await.unwrap();
        let owner = OperationId::from_bytes(test.snapshot.as_bytes()).unwrap();
        let store = TestRetirementStore::new(test.store.clone());
        store
            .fill_history(&test.location, version, absent.then_some(owner))
            .await;
        let mut cursor = SnapshotUploadCleanup::default();
        test.authority
            .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &store.uploads(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(cursor.retired_references, 10);
        assert_eq!(store.counts(), (1, exchanges));
        let value = store.value(&test.location).await;
        assert_eq!(value["version"], 5);
        assert_eq!(value["retired_owners"].as_array().unwrap().len(), 4096);
        assert_eq!(value["owners"], serde_json::json!([other()]));
        test.authority
            .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &store.uploads(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(store.counts(), (2, exchanges));
        assert_eq!(cursor.retired_references, 20);
        assert!(matches!(
            test.uploads.claim(&test.location, owner, 3).await,
            Err(UploadError::Capacity)
        ));
        assert_eq!(
            test.uploads.read_part(&test.location, 0).await.unwrap(),
            b"data".as_slice()
        );
    }
}

#[tokio::test]
async fn lost_retirement_reply_stops_without_counting_success_and_restart_reads_final_fact() {
    for fault in [1, 2] {
        let test = TestCleanup::new().await;
        test.uploads.claim(&test.location, other(), 3).await.unwrap();
        let store = TestRetirementStore::new(test.store.clone());
        store.fault.store(fault, Ordering::Relaxed);
        let mut cursor = SnapshotUploadCleanup::default();
        assert!(matches!(
            test.authority
                .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &store.uploads(), &mut cursor)
                .await,
            Err(UploadReclaimError::Upload(UploadError::Store(
                StoreError::Rejected
            )))
        ));
        assert_eq!(
            (cursor.sample, cursor.field, cursor.retired_references),
            (0, 0, 0)
        );
        assert_eq!(
            store.counts(),
            (1, 1),
            "unknown has no confirming read or later retirement"
        );
        let restarted =
            DatasetAuthority::new(test.store.clone()).with_reclaim_clock_for_tests(test.clock.clone());
        cursor = SnapshotUploadCleanup::default();
        restarted
            .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &store.uploads(), &mut cursor)
            .await
            .unwrap();
        assert_eq!(cursor.retired_references, 10);
        assert_eq!(store.counts(), (2, if fault == 1 { 2 } else { 1 }));
        assert_eq!(
            test.uploads.read_part(&test.location, 0).await.unwrap(),
            b"data".as_slice()
        );
    }
}

#[tokio::test]
async fn rival_claim_observation_is_used_before_shared_references_are_deduplicated() {
    let test = TestCleanup::new().await;
    test.uploads.claim(&test.location, other(), 3).await.unwrap();
    let store = TestRetirementStore::new(test.store.clone());
    store.fault.store(3, Ordering::Relaxed);
    let mut cursor = SnapshotUploadCleanup::default();
    test.authority
        .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &store.uploads(), &mut cursor)
        .await
        .unwrap();
    assert_eq!(
        store.counts(),
        (1, 2),
        "the conflicting CAS supplies the only reload"
    );
    assert_eq!(cursor.retired_references, 10);
    assert_eq!(
        store.value(&test.location).await["owners"],
        serde_json::json!([other(), rival()])
    );
}

#[tokio::test]
async fn identical_upload_id_does_not_bypass_foreign_or_malformed_locator_binding() {
    let test = TestCleanup::new().await;
    let foreign = DatasetIdentity::new(NamespacePath::root(), "foreign-cleanup").unwrap();
    let id = DatasetUploads::upload_id(&test.identity, &test.location).unwrap();
    let foreign = DatasetUploads::location(&foreign, id).unwrap();
    complete_upload(&test, &foreign).await;
    let mut malformed = test.location.clone();
    malformed.push(0);
    // Retained legacy Manifests may have opaque locators that new composition requests reject.
    replace_locations(
        &test,
        vec![
            test.location.clone(),
            foreign.clone(),
            malformed.clone(),
            test.location.clone(),
        ],
    )
    .await;
    let original = test.store.get(&test.location).await.unwrap().unwrap().bytes;
    assert!(matches!(
        test.store
            .compare_exchange(&malformed, None, &original)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    let before_foreign = test.store.get(&foreign).await.unwrap().unwrap();
    let before_malformed = test.store.get(&malformed).await.unwrap().unwrap();
    let store = TestRetirementStore::new(test.store.clone());
    let mut cursor = SnapshotUploadCleanup::default();
    test.authority
        .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &store.uploads(), &mut cursor)
        .await
        .unwrap();
    assert!(cursor.complete);
    assert_eq!(
        (cursor.sample, cursor.field, cursor.retired_references),
        (4, 0, 2)
    );
    assert_eq!(store.counts(), (1, 1));
    // Existing opaque legacy locators are not current Dataset upload references.
    assert_eq!(test.store.get(&foreign).await.unwrap().unwrap(), before_foreign);
    assert_eq!(
        test.store.get(&malformed).await.unwrap().unwrap(),
        before_malformed
    );
}

#[tokio::test]
async fn distinct_uploads_for_one_publication_are_each_retired_once() {
    let test = TestCleanup::new().await;
    let second = DatasetUploads::location(&test.identity, OperationId::random()).unwrap();
    complete_upload(&test, &second).await;
    test.uploads.claim(&second, other(), 3).await.unwrap();
    replace_locations(
        &test,
        (0..6)
            .map(|index| {
                if index % 2 == 0 {
                    test.location.clone()
                } else {
                    second.clone()
                }
            })
            .collect(),
    )
    .await;
    let store = TestRetirementStore::new(test.store.clone());
    let mut cursor = SnapshotUploadCleanup::default();
    test.authority
        .reclaim_snapshot_uploads_step(&test.identity, test.snapshot, &store.uploads(), &mut cursor)
        .await
        .unwrap();
    assert!(cursor.complete);
    assert_eq!(
        (cursor.sample, cursor.field, cursor.retired_references),
        (6, 0, 6)
    );
    assert_eq!(store.counts(), (2, 2));
    assert_eq!(store.value(&test.location).await["owners"], serde_json::json!([]));
    assert_eq!(store.value(&second).await["owners"], serde_json::json!([other()]));
    assert_eq!(
        test.uploads.read_part(&second, 0).await.unwrap(),
        b"data".as_slice()
    );
}
