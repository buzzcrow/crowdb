// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;

use crowdb_access_dataset::{split::*, *};
use std::{collections::BTreeSet, sync::atomic::Ordering};

fn counts(store: &split_store::TestDraftStore) -> (usize, usize) {
    (
        store.bounded_reads.load(Ordering::Relaxed) + store.unbounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}
fn delta(store: &split_store::TestDraftStore, before: (usize, usize)) -> (usize, usize) {
    let now = counts(store);
    (now.0 - before.0, now.1 - before.1)
}
async fn keys(store: &split_store::TestDraftStore) -> BTreeSet<Vec<u8>> {
    store
        .scan_page(
            &MetadataScan {
                prefix: b"DS\0".to_vec(),
                after: None,
                budget: split_store::budget(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

#[tokio::test]
async fn publication_uses_one_head_mutation_and_retries_current_facts_without_writes() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let first = split_store::prepared(&authority, &identity, snapshot, None).await;
    let attempt = split_store::attempt(&authority, &identity, &first).await;
    let before_keys = keys(&store).await;
    store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
    let before = counts(&store);
    let receipt = authority
        .publish_split_revision(&identity, &attempt, None)
        .await
        .unwrap();
    assert_eq!(delta(&store, before), (5, 1));
    let after_keys = keys(&store).await;
    assert_eq!(
        after_keys.difference(&before_keys).cloned().collect::<Vec<_>>(),
        vec![key::head(&identity, snapshot).unwrap()]
    );
    let before = counts(&store);
    assert_eq!(
        authority
            .publish_split_revision(&identity, &attempt, None)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(delta(&store, before), (4, 0));
    let second = split_store::prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    let next = split_store::attempt(&authority, &identity, &second).await;
    let before = counts(&store);
    authority
        .publish_split_revision(&identity, &next, Some(first.revision))
        .await
        .unwrap();
    assert_eq!(delta(&store, before), (4, 1));
    let before = counts(&store);
    assert!(matches!(
        authority.publish_split_revision(&identity, &attempt, None).await,
        Err(SplitLifecycleError::HeadConflict { .. })
    ));
    assert_eq!(delta(&store, before), (4, 0));
}

#[tokio::test]
async fn definite_cas_conflicts_use_the_observation_and_unknown_outcomes_do_not_query() {
    for same_target in [true, false] {
        let (store, authority, identity, snapshot) = split_store::fixture().await;
        let record = split_store::prepared(&authority, &identity, snapshot, None).await;
        let attempt = split_store::attempt(&authority, &identity, &record).await;
        store.split_head_conflicts.store(1, Ordering::Relaxed);
        store
            .clear_split_conflict_target
            .store(!same_target, Ordering::Relaxed);
        store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
        let before = counts(&store);
        let result = authority.publish_split_revision(&identity, &attempt, None).await;
        assert_eq!(delta(&store, before), (5, 1));
        if same_target {
            let receipt = result.unwrap();
            assert_eq!(
                receipt.committed_head,
                authority
                    .inspect_split_head(&identity, snapshot)
                    .await
                    .unwrap()
                    .generation
                    .unwrap()
            );
        } else {
            assert!(matches!(result, Err(SplitLifecycleError::HeadConflict { .. })));
        }
    }
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let record = split_store::prepared(&authority, &identity, snapshot, None).await;
    let attempt = split_store::attempt(&authority, &identity, &record).await;
    store.lose_split_reply.store(true, Ordering::Relaxed);
    store.reject_publishing_ledgers.store(true, Ordering::Relaxed);
    let before = counts(&store);
    assert!(matches!(
        authority.publish_split_revision(&identity, &attempt, None).await,
        Err(SplitLifecycleError::Store(StoreError::Rejected))
    ));
    assert_eq!(delta(&store, before), (5, 1));
    let restarted = DatasetAuthority::new(store.clone());
    let before = counts(&store);
    restarted
        .publish_split_revision(&identity, &attempt, None)
        .await
        .unwrap();
    assert_eq!(delta(&store, before), (4, 0));
}

#[tokio::test]
async fn lost_success_hidden_by_a_later_revision_is_a_conflict_without_replay() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let first = split_store::prepared(&authority, &identity, snapshot, None).await;
    let attempt = split_store::attempt(&authority, &identity, &first).await;
    store.lose_split_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .publish_split_revision(&identity, &attempt, None)
        .await
        .is_err());
    let second = split_store::prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    let next = split_store::attempt(&authority, &identity, &second).await;
    authority
        .publish_split_revision(&identity, &next, Some(first.revision))
        .await
        .unwrap();
    let head = authority.inspect_split_head(&identity, snapshot).await.unwrap();
    let before = counts(&store);
    assert!(matches!(
        authority.publish_split_revision(&identity, &attempt, None).await,
        Err(SplitLifecycleError::HeadConflict { .. })
    ));
    assert_eq!(delta(&store, before), (4, 0));
    assert_eq!(
        authority.inspect_split_head(&identity, snapshot).await.unwrap(),
        head
    );
}

#[tokio::test]
async fn reclaim_reuses_four_split_head_observations_without_reloading_or_helping() {
    for has_split in [false, true] {
        let (store, authority, identity, snapshot) = split_store::fixture().await;
        if has_split {
            let record = split_store::prepared(&authority, &identity, snapshot, None).await;
            split_store::publish_revision(&authority, &identity, &record).await;
        }
        let root = authority
            .prepare_snapshot_candidate(
                &identity,
                None,
                publication::publication_manifest(2),
                OperationId::random(),
            )
            .await
            .unwrap();
        split_store::publish_snapshot_head(&authority, &identity, root).await;
        store.split_head_conflicts.store(4, Ordering::Relaxed);
        let gets = store.split_head_reads.load(Ordering::Relaxed);
        let cas = store.split_head_mutations.load(Ordering::Relaxed);
        let candidate = store.candidate_reads.load(Ordering::Relaxed);
        authority.begin_reclaim(&identity, snapshot).await.unwrap();
        assert_eq!(store.split_head_reads.load(Ordering::Relaxed) - gets, 1);
        assert_eq!(store.split_head_mutations.load(Ordering::Relaxed) - cas, 5);
        assert_eq!(
            store.candidate_reads.load(Ordering::Relaxed) - candidate,
            usize::from(!has_split)
        );
    }
}

#[tokio::test]
async fn legacy_head_attempts_remain_readable_without_creating_receipts() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let record = split_store::prepared(&authority, &identity, snapshot, None).await;
    let attempt = split_store::attempt(&authority, &identity, &record).await;
    authority
        .publish_split_revision(&identity, &attempt, None)
        .await
        .unwrap();
    let key = key::head(&identity, snapshot).unwrap();
    let current = store.get(&key).await.unwrap().unwrap();
    let head = SplitHead::decode(&current.bytes).unwrap();
    let legacy = serde_json::to_vec(&serde_json::json!({"version":2,"head":head,"attempt":attempt})).unwrap();
    store
        .compare_exchange(&key, Some(&current.bytes), &legacy)
        .await
        .unwrap();
    let legacy_key = [
        b"DS\0CA\0".as_slice(),
        &identity.key().unwrap(),
        attempt.id.as_bytes(),
    ]
    .concat();
    store
        .compare_exchange(&legacy_key, None, &attempt.encode().unwrap())
        .await
        .unwrap();
    let before = counts(&store);
    let receipt = authority
        .commit_status(&identity, attempt.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delta(&store, before).1, 0);
    assert_eq!(receipt.attempt, attempt);
    assert_eq!(receipt.committed_head, CommitGeneration::from_head(&legacy));
    assert_eq!(
        authority
            .inspect_split_head(&identity, snapshot)
            .await
            .unwrap()
            .revision,
        Some(record.revision)
    );
}
