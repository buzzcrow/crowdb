// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::*;
use std::sync::atomic::Ordering;

fn counts(store: &split_store::TestDraftStore) -> (usize, usize, usize) {
    (
        store.unbounded_reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
        store.data_head_reads.load(Ordering::Relaxed),
    )
}
fn delta(store: &split_store::TestDraftStore, before: (usize, usize, usize)) -> (usize, usize, usize) {
    let now = counts(store);
    (now.0 - before.0, now.1 - before.1, now.2 - before.2)
}
async fn prepare(authority: &DatasetAuthority, identity: &DatasetIdentity, marker: u8) -> SnapshotId {
    authority
        .prepare_snapshot_candidate(
            identity,
            None,
            publication::publication_manifest(marker),
            OperationId::random(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn publication_writes_only_the_pointer_and_retries_current_effect_without_a_ledger() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let first = prepare(&authority, &identity, 2).await;
    let attempt = common::attempt(&authority, &identity, first).await;
    store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
    let before = counts(&store);
    let receipt = authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    assert_eq!(delta(&store, before), (3, 1, 1));
    let before = counts(&store);
    assert_eq!(
        authority
            .publish_snapshot_candidate(&identity, &attempt)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(delta(&store, before), (3, 0, 1));
    let second = prepare(&authority, &identity, 3).await;
    let next = common::attempt(&authority, &identity, second).await;
    authority
        .publish_snapshot_candidate(&identity, &next)
        .await
        .unwrap();
    let before = counts(&store);
    let error = authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap_err();
    let AuthorityError::SnapshotHeadConflict { observed } = error else {
        panic!("{error:?}")
    };
    assert_eq!(observed.latest, Some(second));
    assert_eq!(delta(&store, before), (3, 0, 1));
    assert!(authority
        .commit_status(&identity, attempt.id)
        .await
        .unwrap()
        .is_none());
    assert!(authority
        .snapshot_commit(&identity, first)
        .await
        .unwrap()
        .is_none());
    assert!(authority.get_snapshot(&identity, first).await.is_ok());
    assert!(authority.get_snapshot(&identity, parent).await.is_ok());
}

#[tokio::test]
async fn real_cas_conflicts_return_the_observation_and_unknown_outcomes_do_not_query() {
    for same in [false, true] {
        let (store, authority, identity, _) = split_store::fixture().await;
        let snapshot = prepare(&authority, &identity, 2).await;
        let attempt = common::attempt(&authority, &identity, snapshot).await;
        store.keep_data_conflict_target.store(same, Ordering::Relaxed);
        store.data_head_conflicts.store(1, Ordering::Relaxed);
        store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
        let before = counts(&store);
        let result = authority.publish_snapshot_candidate(&identity, &attempt).await;
        if same {
            assert_eq!(result.unwrap().attempt, attempt);
        } else {
            let AuthorityError::SnapshotHeadConflict { observed } = result.unwrap_err() else {
                panic!("missing CAS observation")
            };
            assert_eq!(observed.latest, None);
            assert!(observed.generation.is_some());
        }
        assert_eq!(delta(&store, before), (3, 1, 1));
    }
    let (store, authority, identity, _) = split_store::fixture().await;
    let snapshot = prepare(&authority, &identity, 2).await;
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    store.lose_data_reply.store(true, Ordering::Relaxed);
    let before = counts(&store);
    assert!(matches!(
        authority.publish_snapshot_candidate(&identity, &attempt).await,
        Err(AuthorityError::Store(StoreError::Rejected))
    ));
    assert_eq!(delta(&store, before), (3, 1, 1));
    let before = counts(&store);
    authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    assert_eq!(delta(&store, before), (3, 0, 1));
}

#[tokio::test]
async fn four_explicit_retries_use_each_returned_generation_without_a_head_query() {
    let (store, authority, identity, _) = split_store::fixture().await;
    let snapshot = prepare(&authority, &identity, 2).await;
    let mut update = SnapshotHeadUpdate {
        snapshot,
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    store.data_head_conflicts.store(4, Ordering::Relaxed);
    store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
    let before = counts(&store);
    for _ in 0..4 {
        let error = authority
            .update_snapshot_head(&identity, &update)
            .await
            .unwrap_err();
        let AuthorityError::SnapshotHeadConflict { observed } = error else {
            panic!("{error:?}")
        };
        assert_ne!(observed.generation, update.expected_head);
        update.expected_head = observed.generation;
    }
    assert_eq!(
        authority
            .update_snapshot_head(&identity, &update)
            .await
            .unwrap()
            .latest,
        Some(snapshot)
    );
    assert_eq!(delta(&store, before), (15, 5, 5));
}

#[tokio::test]
async fn composition_publication_keeps_prepared_state_and_adds_only_its_draft_read() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    common::begin_evaluated(&authority, &identity, operation, &common::input(parent)).await;
    let draft = common::prepare(&authority, &identity, operation, 1).await;
    let snapshot = draft.preparation.as_ref().unwrap().candidate;
    let attempt = common::attempt(&authority, &identity, snapshot).await;
    store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
    let before = counts(&store);
    authority
        .publish_composition(&identity, operation, &attempt)
        .await
        .unwrap();
    assert_eq!(delta(&store, before), (4, 1, 1));
    assert_eq!(
        authority.composition_status(&identity, operation).await.unwrap(),
        draft
    );
    authority
        .abandon_composition(&identity, operation, 1)
        .await
        .unwrap();
    assert!(authority.get_snapshot(&identity, snapshot).await.is_ok());
}
