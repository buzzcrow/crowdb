// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/snapshot_child_reclaim.rs"]
mod child;
#[path = "common/snapshot_expiry.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use child::{TestChildStore, CANCEL_AFTER_RETIREMENT_READ, DELETE_TIMEOUT, DELETE_UNKNOWN};
use crowdb_access_dataset::*;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[tokio::test]
async fn absent_or_malformed_retirement_and_foreign_child_binding_never_delete_an_index() {
    for kind in 0..3 {
        let (f, parent, snapshot) = child::pair().await;
        let store = TestChildStore::new(&f, parent, snapshot);
        if kind == 0 {
            store.inner.delete(&store.retirement).await.unwrap();
        } else if kind == 1 {
            assert!(matches!(
                store
                    .inner
                    .compare_exchange(&store.retirement, Some(&[1]), &[2])
                    .await
                    .unwrap(),
                CasOutcome::Applied(_)
            ));
        } else {
            let index = store.inner.get(&store.index).await.unwrap().unwrap();
            let mut summary: inspection::SnapshotSummary = serde_json::from_slice(&index.bytes).unwrap();
            summary.parent = Some(f.root);
            assert!(matches!(
                store
                    .inner
                    .compare_exchange(
                        &store.index,
                        Some(&index.bytes),
                        &serde_json::to_vec(&summary).unwrap()
                    )
                    .await
                    .unwrap(),
                CasOutcome::Applied(_)
            ));
        }
        let before = store.inner.get(&store.index).await.unwrap().unwrap();
        let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
        let result = authority.begin_reclaim(&f.identity, parent).await;
        if kind == 0 {
            assert!(matches!(result, Err(AuthorityError::SnapshotProtected)));
        } else {
            assert!(matches!(result, Err(AuthorityError::Corrupt)));
        }
        assert_eq!(store.deletes.load(Ordering::Relaxed), 0);
        assert_eq!(store.inner.get(&store.index).await.unwrap().unwrap(), before);
    }
}

#[tokio::test]
async fn live_child_remains_protected_after_a_retired_sibling_is_pruned() {
    let (f, parent, retired) = child::pair().await;
    let live = f.candidate(Some(parent), 4).await;
    let store = TestChildStore::new(&f, parent, retired);
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    assert!(matches!(
        authority.begin_reclaim(&f.identity, parent).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    assert!(retired < live);
    assert_eq!(store.deletes.load(Ordering::Relaxed), 1);
    assert!(store.inner.get(&store.index).await.unwrap().is_none());
    assert!(store
        .inner
        .get(&child::child_key(&f.identity, parent, live))
        .await
        .unwrap()
        .is_some());
    assert!(authority
        .require_committed_snapshot(&f.identity, live)
        .await
        .is_ok());
    assert!(authority.get_snapshot(&f.identity, parent).await.is_ok());
}

#[tokio::test]
async fn lost_delete_reply_stops_without_followup_and_a_fresh_authority_recovers() {
    let (f, parent, snapshot) = child::pair().await;
    let store = TestChildStore::new(&f, parent, snapshot);
    store.fault.store(DELETE_UNKNOWN, Ordering::Relaxed);
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    assert!(matches!(
        authority.begin_reclaim(&f.identity, parent).await,
        Err(AuthorityError::Store(StoreError::Response))
    ));
    assert!(store.hit.load(Ordering::Relaxed));
    assert_eq!(store.deletes.load(Ordering::Relaxed), 1);
    assert_eq!(store.after_unknown.load(Ordering::Relaxed), 0);
    store.reset_unknown();
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    restarted.begin_reclaim(&f.identity, parent).await.unwrap();
    assert_eq!(
        restarted
            .snapshot_reclaim_after(&f.identity, parent)
            .await
            .unwrap(),
        2200
    );
    assert_eq!(
        store.inner.get(&store.retirement).await.unwrap().unwrap().bytes,
        [1]
    );
    assert!(store.inner.get(&store.index).await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn delete_timeout_is_unknown_and_does_not_attempt_compensation() {
    let (f, parent, snapshot) = child::pair().await;
    let store = TestChildStore::new(&f, parent, snapshot);
    store.fault.store(DELETE_TIMEOUT, Ordering::Relaxed);
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    let result = authority.begin_reclaim(&f.identity, parent).await;
    assert!(
        matches!(result, Err(AuthorityError::Store(StoreError::Failure(message))) if message == "retired child index deletion timed out; outcome unknown")
    );
    assert!(store.hit.load(Ordering::Relaxed));
    assert_eq!(store.deletes.load(Ordering::Relaxed), 1);
    assert_eq!(store.after_unknown.load(Ordering::Relaxed), 0);
    store.reset_unknown();
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    restarted.begin_reclaim(&f.identity, parent).await.unwrap();
    assert!(store.inner.get(&store.index).await.unwrap().is_none());
}

#[tokio::test]
async fn cancellation_between_retirement_read_and_delete_does_not_mutate_the_index() {
    let (f, parent, snapshot) = child::pair().await;
    let store = TestChildStore::new(&f, parent, snapshot);
    store.fault.store(CANCEL_AFTER_RETIREMENT_READ, Ordering::Relaxed);
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    assert!(matches!(
        authority.begin_reclaim(&f.identity, parent).await,
        Err(AuthorityError::Invalid(DatasetError::ReadCancelled))
    ));
    assert!(store.hit.load(Ordering::Relaxed));
    assert_eq!(store.deletes.load(Ordering::Relaxed), 0);
    assert!(store.inner.get(&store.index).await.unwrap().is_some());
    assert!(authority.get_snapshot(&f.identity, parent).await.is_ok());
}

#[tokio::test]
async fn recreated_index_and_repeated_child_retirement_cannot_reopen_final_content() {
    let (f, parent, snapshot) = child::pair().await;
    let store = TestChildStore::new(&f, parent, snapshot);
    let original = store.inner.get(&store.index).await.unwrap().unwrap();
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(f.clock.clone());
    authority.begin_reclaim(&f.identity, parent).await.unwrap();
    assert!(matches!(
        store
            .inner
            .compare_exchange(&store.index, None, &original.bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    assert!(authority
        .require_committed_snapshot(&f.identity, snapshot)
        .await
        .is_err());
    assert!(authority
        .complete_snapshot_candidate(&f.identity, snapshot)
        .await
        .is_err());
    f.clock.store(2200, Ordering::Relaxed);
    let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
    let mut cursor = SnapshotReclaimCursor::new(f.identity.clone(), snapshot);
    let mut complete = false;
    for _ in 0..32 {
        if authority
            .advance_snapshot_reclaim(&uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            complete = true;
            break;
        }
    }
    assert!(complete);
    let index = store.inner.get(&store.index).await.unwrap().unwrap();
    let summary: inspection::SnapshotSummary = serde_json::from_slice(&index.bytes).unwrap();
    assert_eq!(
        summary.state,
        inspection::InspectionState::Unsupported {
            reason: "snapshot is retired".into()
        }
    );
    assert_eq!(
        store.inner.get(&store.retirement).await.unwrap().unwrap().bytes,
        [1]
    );
    assert!(authority
        .complete_snapshot_candidate(&f.identity, snapshot)
        .await
        .is_err());
    let page = authority
        .snapshot_children_page(
            &f.identity,
            Some(parent),
            None,
            MetadataScanBudget {
                items: 20,
                bytes: 1024 * 1024,
                timeout: Duration::from_secs(2),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(page.entries.is_empty());
    assert!(
        store.inner.get(&store.index).await.unwrap().is_some(),
        "ordinary reads do not prune indexes"
    );
}
