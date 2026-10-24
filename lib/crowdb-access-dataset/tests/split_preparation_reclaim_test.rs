// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[tokio::test]
async fn owner_reclaim_finds_complete_and_partial_resources_without_any_draft() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = authority.with_reclaim_clock_for_tests(clock.clone());
    let input = SplitPreparation {
        revision: SplitRevisionId::random(),
        policy: SplitPolicy::default(),
        splits: split_store::partition_splits(),
    };
    store.reject_draft_writes.store(true, Ordering::Relaxed);
    let complete = authority
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .unwrap();
    let mut partial = input.clone();
    partial.revision = SplitRevisionId::random();
    for split in &mut partial.splits {
        split.definition = SplitDefinition::Selection(Selection::Explicit(vec![b"sample-1".to_vec()]));
    }
    assert!(matches!(
        authority
            .prepare_split_revision(&identity, snapshot, &partial, &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::PolicyViolation)
    ));
    let mut keys = vec![key::revision(&identity, snapshot, complete.revision).unwrap()];
    for split in &complete.splits {
        for page in &split.pages {
            keys.push(
                key::membership_page(&identity, snapshot, complete.revision, &split.name, page.page).unwrap(),
            );
        }
    }
    for split in &partial.splits {
        keys.push(key::membership_page(&identity, snapshot, partial.revision, &split.name, 0).unwrap());
    }
    let mut before = Vec::new();
    for key in &keys {
        before.push(store.get(key).await.unwrap().unwrap().bytes);
    }
    store.reject_draft_writes.store(false, Ordering::Relaxed);
    let next = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot: next,
                expected_head: authority.inspect_head(&identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    clock.store(1300, Ordering::Release);
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock);
    let mut finished = false;
    let mut metadata_cursor = MetadataCleanupProgress::new(snapshot);
    for _ in 0..100 {
        if restarted
            .reclaim_snapshot_metadata_step_for_tests(
                &identity,
                snapshot,
                &mut metadata_cursor,
                &ReadCancellation::new(),
            )
            .await
            .unwrap()
            .complete
        {
            finished = true;
            break;
        }
    }
    assert!(finished);
    for (key, previous) in keys.iter().zip(before) {
        assert_eq!(store.get(key).await.unwrap().unwrap().bytes, b"split-retired-v1");
        assert!(matches!(
            store.compare_exchange(key, None, &previous).await.unwrap(),
            CasOutcome::Conflict(_)
        ));
    }
    let writes = store.mutations.load(Ordering::Relaxed);
    assert!(restarted
        .prepare_split_revision(&identity, snapshot, &input, &ReadCancellation::new())
        .await
        .is_err());
    assert!(restarted
        .prepare_split_revision(&identity, snapshot, &partial, &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    assert!(restarted.get_snapshot(&identity, next).await.is_ok());
}
