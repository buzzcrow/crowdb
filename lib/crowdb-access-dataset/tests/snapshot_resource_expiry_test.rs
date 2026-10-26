// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/snapshot_expiry.rs"]
mod common;
#[path = "common/incomplete_expiry.rs"]
#[allow(dead_code)]
mod incomplete;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/snapshot_resource.rs"]
#[allow(dead_code)]
mod resource;
use common::{key, TestExpiryFixture};
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

const EXPIRES: u64 = 1000 + 86400;

#[tokio::test]
async fn snapshot_created_before_its_first_manifest_write_is_discovered_after_restart() {
    let f = TestExpiryFixture::new().await;
    let snapshot = SnapshotId::from_bytes(&[2; 16]).unwrap();
    f.store.gate.store(5, Ordering::Relaxed);
    let preparing = tokio::spawn({
        let authority = Arc::clone(&f.authority);
        let identity = f.identity.clone();
        async move {
            authority
                .prepare_snapshot_candidate(
                    &identity,
                    None,
                    publication::publication_manifest(2),
                    OperationId::from_bytes(snapshot.as_bytes()).unwrap(),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), f.store.started.notified())
        .await
        .unwrap();
    assert!(f.store.get(&key(&f.identity, snapshot)).await.unwrap().is_none());
    preparing.abort();
    assert!(preparing.await.unwrap_err().is_cancelled());

    f.clock.store(EXPIRES, Ordering::Relaxed);
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
    assert!(restarted.get_snapshot(&f.identity, f.root).await.is_ok());
}

#[tokio::test]
async fn an_old_resource_inherits_its_binding_deadline_without_a_new_wait() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    resource::downgrade(f.store.as_ref(), &f.identity, snapshot).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    assert_eq!(
        resource::deadline(f.store.as_ref(), &f.identity, snapshot).await,
        EXPIRES
    );
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 1200
    );
}

#[tokio::test]
async fn an_old_resource_without_a_deadline_gets_one_fixed_background_deadline() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    resource::downgrade(f.store.as_ref(), &f.identity, snapshot).await;
    let cb_key = key(&f.identity, snapshot);
    let value = f.store.get(&cb_key).await.unwrap().unwrap();
    let mut binding: serde_json::Value = serde_json::from_slice(&value.bytes).unwrap();
    binding.as_object_mut().unwrap().remove("expires_at");
    assert!(matches!(
        f.store
            .compare_exchange(
                &cb_key,
                Some(&value.bytes),
                &serde_json::to_vec(&binding).unwrap()
            )
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    f.clock.store(EXPIRES, Ordering::Relaxed);
    let writes = f.store.writes();
    f.expire().await.unwrap();
    assert_eq!(f.store.writes(), writes + 1);
    assert_eq!(
        resource::deadline(f.store.as_ref(), &f.identity, snapshot).await,
        EXPIRES + 86400
    );
    f.clock.store(EXPIRES + 1, Ordering::Relaxed);
    let writes = f.store.writes();
    f.expire().await.unwrap();
    assert_eq!(f.store.writes(), writes);
    assert_eq!(
        resource::deadline(f.store.as_ref(), &f.identity, snapshot).await,
        EXPIRES + 86400
    );
    f.clock.store(EXPIRES + 86400, Ordering::Relaxed);
    f.expire().await.unwrap();
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 86400 + 1200
    );
}

#[tokio::test]
async fn a_late_first_manifest_write_cannot_escape_completed_retirement() {
    let f = TestExpiryFixture::new().await;
    let snapshot = SnapshotId::from_bytes(&[2; 16]).unwrap();
    f.store.gate.store(5, Ordering::Relaxed);
    let preparing = tokio::spawn({
        let authority = Arc::clone(&f.authority);
        let identity = f.identity.clone();
        async move {
            authority
                .prepare_snapshot_candidate(
                    &identity,
                    None,
                    publication::publication_manifest(2),
                    OperationId::from_bytes(snapshot.as_bytes()).unwrap(),
                )
                .await
        }
    });
    incomplete::wait_for_pause(&f).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.expire().await.unwrap();
    assert_eq!(
        f.store
            .get(&key(&f.identity, snapshot))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"retired-snapshot-content-v1"
    );
    f.clock.store(EXPIRES + 1200, Ordering::Relaxed);
    incomplete::finish_reclaim(&f, &incomplete::uploads(&f), snapshot).await;
    f.store.resume.notify_one();
    assert!(preparing.await.unwrap().is_err());
    let manifest_key = [
        b"DS\0M\0".as_slice(),
        &f.identity.key().unwrap(),
        snapshot.as_bytes(),
        &0_u32.to_be_bytes(),
    ]
    .concat();
    assert_eq!(
        f.store.get(&manifest_key).await.unwrap().unwrap().bytes,
        b"snapshot-metadata-retired-v1"
    );
    assert!(f.authority.get_snapshot(&f.identity, snapshot).await.is_err());
    assert_eq!(
        resource::deadline(f.store.as_ref(), &f.identity, snapshot).await,
        EXPIRES
    );
}

#[tokio::test]
async fn unknown_deadline_migration_stops_before_any_followup_rpc() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    resource::downgrade(f.store.as_ref(), &f.identity, snapshot).await;
    f.clock.store(EXPIRES, Ordering::Relaxed);
    f.store.lose_deadline.store(true, Ordering::Relaxed);
    assert!(matches!(
        f.expire().await,
        Err(AuthorityError::Store(StoreError::Rejected))
    ));
    assert_eq!(f.store.reads_after_unknown.load(Ordering::Relaxed), 0);
    assert_eq!(f.store.writes_after_unknown.load(Ordering::Relaxed), 0);
    f.store.unknown.store(false, Ordering::Relaxed);
    assert_eq!(
        resource::deadline(f.store.as_ref(), &f.identity, snapshot).await,
        EXPIRES
    );
    f.expire().await.unwrap();
    assert_eq!(
        f.authority
            .snapshot_reclaim_after(&f.identity, snapshot)
            .await
            .unwrap(),
        EXPIRES + 1200
    );
}

#[tokio::test]
async fn full_resource_scan_pages_large_manifests_without_writes_before_expiry() {
    let f = TestExpiryFixture::new().await;
    for marker in 2..5 {
        resource::put_large(f.store.as_ref(), &f.identity, marker, EXPIRES).await;
    }
    let writes = f.store.writes();
    let mut cursor = SnapshotExpirySweep::default();
    for _ in 0..4 {
        f.authority
            .expire_snapshot_candidates_page(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if !cursor.has_more() {
            break;
        }
    }
    assert!(!cursor.has_more());
    assert_eq!(f.store.resource_pages.load(Ordering::Relaxed), 3);
    assert!(f.store.scan_peak_bytes.load(Ordering::Relaxed) <= 9 * 1024 * 1024);
    assert!(f.store.scan_peak_items.load(Ordering::Relaxed) <= 20);
    assert_eq!(f.store.writes(), writes);
}

#[tokio::test]
async fn delayed_preparation_keeps_the_first_snapshot_deadline_in_its_binding() {
    let f = TestExpiryFixture::new().await;
    f.store.gate.store(5, Ordering::Relaxed);
    let preparing = tokio::spawn({
        let authority = Arc::clone(&f.authority);
        let identity = f.identity.clone();
        async move {
            authority
                .prepare_snapshot_candidate(
                    &identity,
                    None,
                    publication::publication_manifest(2),
                    OperationId::from_bytes(&[2; 16]).unwrap(),
                )
                .await
        }
    });
    incomplete::wait_for_pause(&f).await;
    f.clock.store(2000, Ordering::Relaxed);
    f.store.resume.notify_one();
    let snapshot = preparing.await.unwrap().unwrap();
    assert_eq!(
        resource::deadline(f.store.as_ref(), &f.identity, snapshot).await,
        EXPIRES
    );
    let binding = f.store.get(&key(&f.identity, snapshot)).await.unwrap().unwrap();
    let binding: serde_json::Value = serde_json::from_slice(&binding.bytes).unwrap();
    assert_eq!(binding["expires_at"], EXPIRES);
}

#[tokio::test]
async fn versioned_snapshot_resources_reject_truncation_trailing_bytes_and_unknown_versions() {
    let f = TestExpiryFixture::new().await;
    let snapshot = f.candidate(None, 2).await;
    let key = resource::snapshot_key(&f.identity, snapshot);
    let mut stored = f.store.get(&key).await.unwrap().unwrap();
    let original = stored.bytes.clone();
    let mut future = original.clone();
    future[..2].copy_from_slice(&3_u16.to_le_bytes());
    let mut trailing = original.clone();
    trailing.push(0);
    for bytes in [
        vec![],
        original[..2].to_vec(),
        original[..original.len() - 1].to_vec(),
        future,
        trailing,
    ] {
        assert!(matches!(
            f.store
                .compare_exchange(&key, Some(&stored.bytes), &bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
        assert!(matches!(
            f.authority.get_snapshot(&f.identity, snapshot).await,
            Err(AuthorityError::Corrupt)
        ));
        stored = f.store.get(&key).await.unwrap().unwrap();
    }
}
