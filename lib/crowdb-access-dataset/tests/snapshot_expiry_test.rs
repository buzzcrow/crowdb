// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/snapshot_expiry.rs"]
mod common;
#[path = "common/publication.rs"]
mod publication;
use common::{key, TestExpiryFixture};
use crowdb_access_dataset::*;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

const EXPIRES: u64 = 1000 + 86400;

#[tokio::test]
async fn fixed_resource_deadline_survives_retries_restart_and_deletion_grace() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    let cb_key = key(&f.identity, snapshot);
    let binding = f.store.get(&cb_key).await.unwrap().unwrap();
    let value: serde_json::Value = serde_json::from_slice(&binding.bytes).unwrap();
    assert_eq!(value["expires_at"], EXPIRES);
    f.clock.store(EXPIRES - 1, Ordering::Relaxed);
    assert_eq!(f.candidate(None, 2).await, snapshot);
    assert_eq!(f.store.get(&cb_key).await.unwrap().unwrap(), binding);
    let writes = f.store.writes();
    f.expire().await.unwrap();
    assert_eq!(f.store.writes(), writes);
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_ok());
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_err());
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 300
    );
    let restarted = DatasetAuthority::new(f.store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    f.clock.store(EXPIRES + 100, Ordering::Relaxed);
    let writes = f.store.writes();
    restarted
        .expire_snapshot_candidates_page(&mut SnapshotExpirySweep::default(), &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(f.store.writes(), writes);
    assert_eq!(
        restarted
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 300
    );
    assert!(restarted.get_snapshot(&f.identity, f.root).await.is_ok());
}

#[tokio::test]
async fn heads_pins_children_and_legacy_resources_need_no_mutations_when_expiry_scans() {
    let f = TestExpiryFixture::new().await;
    let pinned = f.candidate(None, 2).await;
    f.authority.retain_snapshot(&f.identity, pinned).await.unwrap();
    let parent = f.candidate(None, 3).await;
    let legacy = f.candidate(None, 4).await;
    let cb_key = key(&f.identity, legacy);
    let old = f.store.get(&cb_key).await.unwrap().unwrap();
    let mut binding: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    binding.as_object_mut().unwrap().remove("expires_at");
    f.store
        .compare_exchange(&cb_key, Some(&old.bytes), &serde_json::to_vec(&binding).unwrap())
        .await
        .unwrap();
    f.clock.store(2000, Ordering::Relaxed);
    let child = f.candidate(Some(parent), 5).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    let writes = f.store.writes();
    f.expire().await.unwrap();
    assert_eq!(
        f.store.writes(),
        writes,
        "protected resources cannot churn the shared head"
    );
    for snapshot in [f.root, pinned, parent, child, legacy] {
        assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_ok());
    }
    f.authority.release_snapshot(&f.identity, pinned).await.unwrap();
    f.expire().await.unwrap();
    assert!(f.authority.get_snapshot(&f.identity, pinned).await.is_err());
    assert!(f.authority.get_snapshot(&f.identity, parent).await.is_ok());
}

#[tokio::test]
async fn expiry_invalidates_a_publisher_paused_at_its_head_cas() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    let update = SnapshotHeadUpdate {
        snapshot,
        expected_head: f.authority.inspect_head(&f.identity).await.unwrap().generation,
    };
    f.store.gate.store(1, Ordering::Relaxed);
    let authority = Arc::clone(&f.authority);
    let identity = f.identity.clone();
    let pending = tokio::spawn(async move { authority.update_snapshot_head(&identity, &update).await });
    tokio::time::timeout(Duration::from_secs(2), f.store.started.notified())
        .await
        .unwrap();
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    f.store.resume.notify_one();
    assert!(matches!(
        pending.await.unwrap(),
        Err(AuthorityError::SnapshotHeadConflict { .. })
    ));
    assert_eq!(
        f.authority.inspect_head(&f.identity).await.unwrap().latest,
        Some(f.root)
    );
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_err());
}

#[tokio::test]
async fn publication_winning_before_the_mark_is_preserved_by_authoritative_recheck() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    f.store.gate.store(2, Ordering::Relaxed);
    f.clock.store(EXPIRES, Ordering::Relaxed);
    let authority = Arc::clone(&f.authority);
    let pending = tokio::spawn(async move {
        authority
            .expire_snapshot_candidates_page(&mut SnapshotExpirySweep::default(), &ReadCancellation::new())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), f.store.started.notified())
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
    pending.await.unwrap().unwrap();
    assert_eq!(
        f.authority.inspect_head(&f.identity).await.unwrap().latest,
        Some(snapshot)
    );
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_ok());
    assert!(f
        .authority
        .snapshot_reclaim_after(&f.identity, snapshot)
        .await
        .is_err());
}

#[tokio::test]
async fn an_unknown_mark_returns_without_confirmation_and_the_next_sweep_retries() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.store.lose_mark.store(true, Ordering::Relaxed);
    assert!(matches!(
        f.expire().await,
        Err(AuthorityError::Store(StoreError::Rejected))
    ));
    assert_eq!(f.store.reads_after_unknown.load(Ordering::Relaxed), 0);
    f.store.unknown.store(false, Ordering::Relaxed);
    f.expire().await.unwrap();
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 300
    );
}

#[tokio::test]
async fn corrupt_rows_do_not_pin_discovery_and_cancellation_creates_no_mark() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    let bad = key(&f.identity, SnapshotId::from_bytes(&[1; 16]).unwrap());
    f.store.compare_exchange(&bad, None, b"{").await.unwrap();
    f.clock.store(EXPIRES, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    cancel.cancel();
    let writes = f.store.writes();
    assert!(f
        .authority
        .expire_snapshot_candidates_page(&mut SnapshotExpirySweep::default(), &cancel)
        .await
        .is_err());
    assert_eq!(f.store.writes(), writes);
    let mut cursor = SnapshotExpirySweep::default();
    assert!(matches!(
        f.authority
            .expire_snapshot_candidates_page(&mut cursor, &ReadCancellation::new())
            .await,
        Err(AuthorityError::Corrupt)
    ));
    assert!(cursor.has_more());
    f.authority
        .expire_snapshot_candidates_page(&mut cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_err());
}
