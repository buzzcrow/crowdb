// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/snapshot_expiry.rs"]
mod common;
#[path = "common/incomplete_expiry.rs"]
mod incomplete;
#[path = "common/publication.rs"]
mod publication;
use common::{key, TestExpiryFixture};
use crowdb_access_dataset::*;
use incomplete::{finish_reclaim, index_row, staged, uploads, wait_for_pause, TestPreparedUpload, EXPIRES};
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn interrupted_preparation_expires_without_extending_its_original_deadline() {
    let f = TestExpiryFixture::new().await;
    let snapshot = staged(&f, 2).await;
    let cb = f.store.get(&key(&f.identity, snapshot)).await.unwrap().unwrap();
    let binding: serde_json::Value = serde_json::from_slice(&cb.bytes).unwrap();
    assert_eq!(binding["version"], 3);
    assert_eq!(binding["expires_at"], EXPIRES);
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_err());
    f.clock.store(EXPIRES - 1, Ordering::Relaxed);
    assert_eq!(staged(&f, 2).await, snapshot);
    assert_eq!(
        f.store.get(&key(&f.identity, snapshot)).await.unwrap().unwrap(),
        cb
    );
    let writes = f.store.writes();
    f.expire().await.unwrap();
    assert_eq!(
        f.store.writes(),
        writes,
        "unexpired preparation must not be marked"
    );
    assert!(f
        .authority
        .snapshot_reclaim_after(&f.identity, snapshot)
        .await
        .is_err());
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .expect("expired CB3 must enter durable delayed reclamation"),
        EXPIRES + 1200
    );
}

#[tokio::test]
async fn restart_finishes_due_preparation_and_seals_a_delayed_index_retry() {
    let f = TestExpiryFixture::new().await;
    let snapshot = staged(&f, 2).await;
    f.store.gate.store(4, Ordering::Relaxed);
    let retry = tokio::spawn({
        let authority = Arc::clone(&f.authority);
        let identity = f.identity.clone();
        let parent = f.root;
        async move {
            authority
                .prepare_snapshot_with_operation(
                    &identity,
                    Some(parent),
                    publication::publication_manifest(2),
                    OperationId::from_bytes(&[2; 16]).unwrap(),
                )
                .await
        }
    });
    wait_for_pause(&f).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    f.clock.store(EXPIRES + 1199, Ordering::Relaxed);
    let uploads = uploads(&f);
    let writes = f.store.writes();
    f.authority
        .reclaim_due_page(
            &uploads,
            &mut SnapshotReclaimSweep::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(f.store.writes(), writes, "cleanup must wait for persisted grace");
    f.clock.store(EXPIRES + 1200, Ordering::Relaxed);
    let restarted = finish_reclaim(&f, &uploads, snapshot).await;
    let row = index_row(&f, snapshot);
    let fence = f.store.get(&row).await.unwrap().unwrap();
    assert_eq!(fence.bytes, b"snapshot-metadata-retired-v1");
    f.store.resume.notify_one();
    assert!(retry.await.unwrap().is_err());
    assert_eq!(f.store.get(&row).await.unwrap().unwrap(), fence);
    assert!(restarted
        .complete_snapshot_candidate(&f.identity, snapshot)
        .await
        .is_err());
    assert!(restarted.get_snapshot(&f.identity, f.root).await.is_ok());
}

#[tokio::test]
async fn finalization_and_head_winning_before_expiry_mark_remain_readable() {
    let f = TestExpiryFixture::new().await;
    let snapshot = staged(&f, 2).await;
    f.store.gate.store(2, Ordering::Relaxed);
    f.clock.store(EXPIRES, Ordering::Relaxed);
    let expiry = tokio::spawn({
        let authority = Arc::clone(&f.authority);
        async move {
            authority
                .expire_snapshot_candidates_page(
                    &mut SnapshotExpirySweep::default(),
                    &ReadCancellation::new(),
                )
                .await
        }
    });
    wait_for_pause(&f).await;
    f.authority
        .complete_snapshot_candidate(&f.identity, snapshot)
        .await
        .unwrap();
    f.authority
        .update_snapshot_head(
            &f.identity,
            &SnapshotHeadUpdate {
                snapshot,
                expected_head: f.authority.inspect_head(&f.identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    f.store.resume.notify_one();
    expiry.await.unwrap().unwrap();
    assert_eq!(f.authority.latest(&f.identity).await.unwrap(), Some(snapshot));
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_ok());
    assert!(f
        .authority
        .snapshot_reclaim_after(&f.identity, snapshot)
        .await
        .is_err());
}

#[tokio::test]
async fn completed_and_retained_preparation_survives_the_authoritative_expiry_recheck() {
    let f = TestExpiryFixture::new().await;
    let snapshot = staged(&f, 2).await;
    let preserved = f.candidate(None, 4).await;
    f.authority.retain_snapshot(&f.identity, preserved).await.unwrap();
    f.store.gate.store(2, Ordering::Relaxed);
    f.clock.store(EXPIRES, Ordering::Relaxed);
    let expiry = tokio::spawn({
        let authority = Arc::clone(&f.authority);
        async move {
            authority
                .expire_snapshot_candidates_page(
                    &mut SnapshotExpirySweep::default(),
                    &ReadCancellation::new(),
                )
                .await
        }
    });
    wait_for_pause(&f).await;
    f.authority
        .complete_snapshot_candidate(&f.identity, snapshot)
        .await
        .unwrap();
    f.authority.retain_snapshot(&f.identity, snapshot).await.unwrap();
    f.store.resume.notify_one();
    expiry.await.unwrap().unwrap();
    for snapshot in [snapshot, preserved] {
        assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_ok());
        assert!(f
            .authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .is_err());
    }
}

#[tokio::test]
async fn unknown_expiry_mark_stops_and_restart_recovers_the_incomplete_resource() {
    let f = TestExpiryFixture::new().await;
    let snapshot = staged(&f, 2).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.store.lose_mark.store(true, Ordering::Relaxed);
    assert!(matches!(
        f.expire().await,
        Err(AuthorityError::Store(StoreError::Rejected))
    ));
    assert_eq!(f.store.reads_after_unknown.load(Ordering::Relaxed), 0);
    assert_eq!(f.store.writes_after_unknown.load(Ordering::Relaxed), 0);
    f.store.unknown.store(false, Ordering::Relaxed);
    let restarted = DatasetAuthority::new(f.store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    restarted
        .expire_snapshot_candidates_page(&mut SnapshotExpirySweep::default(), &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(
        restarted
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 1200
    );
    f.clock.store(EXPIRES + 1200, Ordering::Relaxed);
    finish_reclaim(&f, &uploads(&f), snapshot).await;
}

#[tokio::test]
async fn interrupted_claimed_preparation_retires_only_its_owner() {
    let f = TestExpiryFixture::new().await;
    let prepared = TestPreparedUpload::new(&f).await;
    prepared.claim().await;
    let shared = prepared.prepare_shared_owner(&f).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    f.clock.store(EXPIRES + 1200, Ordering::Relaxed);
    let authority = finish_reclaim(&f, &prepared.uploads, prepared.snapshot).await;
    let upload = prepared.uploads.metadata(&prepared.location).await.unwrap();
    assert_eq!(
        serde_json::to_value(upload).unwrap()["owners"],
        serde_json::json!([OperationId::from_bytes(shared.as_bytes()).unwrap()])
    );
    assert!(matches!(
        prepared
            .uploads
            .claim(&prepared.location, prepared.operation, EXPIRES + 1200)
            .await,
        Err(UploadError::Conflict)
    ));
    assert!(authority.get_snapshot(&f.identity, shared).await.is_ok());
    assert_eq!(
        prepared.uploads.read_part(&prepared.location, 0).await.unwrap(),
        b"data"[..]
    );
}

#[tokio::test]
async fn delayed_claim_cannot_acquire_ownership_after_incomplete_expiry_cleanup() {
    let f = TestExpiryFixture::new().await;
    let prepared = TestPreparedUpload::new(&f).await;
    f.store.gate.store(3, Ordering::Relaxed);
    let pending = tokio::spawn({
        let uploads = prepared.uploads.clone();
        let location = prepared.location.clone();
        let operation = prepared.operation;
        async move { uploads.claim(&location, operation, 1000).await }
    });
    wait_for_pause(&f).await;
    assert_eq!(
        f.store.gate.load(Ordering::Relaxed),
        0,
        "claim pause must actually fire"
    );
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    f.clock.store(EXPIRES + 1200, Ordering::Relaxed);
    finish_reclaim(&f, &prepared.uploads, prepared.snapshot).await;
    f.store.resume.notify_one();
    assert!(matches!(pending.await.unwrap(), Err(UploadError::Conflict)));
    assert_eq!(
        serde_json::to_value(prepared.uploads.metadata(&prepared.location).await.unwrap()).unwrap()["owners"],
        serde_json::json!([])
    );
}

#[tokio::test]
async fn unknown_owner_retirement_does_not_advance_to_index_cleanup() {
    let f = TestExpiryFixture::new().await;
    let prepared = TestPreparedUpload::new(&f).await;
    prepared.claim().await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    f.clock.store(EXPIRES + 1200, Ordering::Relaxed);
    f.store.lose_owner_retirement.store(true, Ordering::Relaxed);
    assert!(f
        .authority
        .reclaim_due_page(
            &prepared.uploads,
            &mut SnapshotReclaimSweep::default(),
            &ReadCancellation::new()
        )
        .await
        .is_err());
    assert!(f.store.unknown.load(Ordering::Relaxed));
    assert!(
        !f.store.lose_owner_retirement.load(Ordering::Relaxed),
        "retirement fault must actually fire"
    );
    assert_eq!(f.store.reads_after_unknown.load(Ordering::Relaxed), 0);
    assert_eq!(f.store.writes_after_unknown.load(Ordering::Relaxed), 0);
    f.store.unknown.store(false, Ordering::Relaxed);
    let row = index_row(&f, prepared.snapshot);
    assert_ne!(
        f.store.get(&row).await.unwrap().unwrap().bytes,
        b"snapshot-metadata-retired-v1"
    );
    let upload = prepared.uploads.metadata(&prepared.location).await.unwrap();
    assert_eq!(
        serde_json::to_value(upload).unwrap()["owners"],
        serde_json::json!([]),
        "the lost retirement reply had applied"
    );
    finish_reclaim(&f, &prepared.uploads, prepared.snapshot).await;
    assert_eq!(
        f.store.get(&row).await.unwrap().unwrap().bytes,
        b"snapshot-metadata-retired-v1"
    );
}

#[tokio::test]
async fn removing_the_binding_deadline_does_not_remove_the_snapshot_deadline() {
    let f = TestExpiryFixture::new().await;
    let snapshot = staged(&f, 2).await;
    let cb_key = key(&f.identity, snapshot);
    let original = f.store.get(&cb_key).await.unwrap().unwrap();
    let mut binding: serde_json::Value = serde_json::from_slice(&original.bytes).unwrap();
    binding.as_object_mut().unwrap().remove("expires_at");
    assert!(matches!(
        f.store
            .compare_exchange(
                &cb_key,
                Some(&original.bytes),
                &serde_json::to_vec(&binding).unwrap()
            )
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    f.clock.store(EXPIRES + 86400, Ordering::Relaxed);
    f.expire().await.unwrap();
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 86400 + 1200
    );
    assert_eq!(
        f.authority
            .publication_status(&f.identity, OperationId::from_bytes(snapshot.as_bytes()).unwrap())
            .await
            .unwrap()
            .publication
            .state,
        PublicationState::Aborted
    );
}
