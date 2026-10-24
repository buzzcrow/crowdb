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

#[tokio::test]
async fn retired_snapshot_cleans_all_revision_pages_without_touching_other_owners() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1000));
    let authority = authority.with_reclaim_clock_for_tests(clock.clone());
    let first = split_store::prepared(&authority, &identity, snapshot, None).await;
    let first_attempt = split_store::attempt(&authority, &identity, &first).await;
    authority
        .publish_split_draft(&identity, first.operation, &first_attempt)
        .await
        .unwrap();
    let second = split_store::prepared(&authority, &identity, snapshot, Some(first.revision)).await;
    split_store::publish_revision(&authority, &identity, &second).await;
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(&identity, &common::attempt(&authority, &identity, root).await)
        .await
        .unwrap();
    let foreign = split_store::prepared(&authority, &identity, root, None).await;
    authority
        .publish_split_draft(
            &identity,
            foreign.operation,
            &split_store::attempt(&authority, &identity, &foreign).await,
        )
        .await
        .unwrap();
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    clock.store(1300, std::sync::atomic::Ordering::Release);
    let cancel = ReadCancellation::new();
    let partial = authority
        .reclaim_snapshot_metadata_step(&identity, snapshot, &cancel)
        .await
        .unwrap();
    assert!(!partial.complete);
    clock.store(1300, std::sync::atomic::Ordering::Release);
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock);
    let mut completed = false;
    for _ in 0..100 {
        if restarted
            .reclaim_snapshot_metadata_step(&identity, snapshot, &cancel)
            .await
            .unwrap()
            .complete
        {
            completed = true;
            break;
        }
    }
    assert!(completed);
    assert_eq!(
        restarted
            .reclaim_progress(&identity, snapshot)
            .await
            .unwrap()
            .state,
        ReclaimState::Completed
    );
    for revision in [&first, &second] {
        let revision_key = split::key::revision(&identity, snapshot, revision.revision).unwrap();
        assert_retired_fence(store.as_ref(), &revision_key).await;
        assert!(matches!(
            store
                .compare_exchange(&revision_key, None, &revision.encode().unwrap())
                .await
                .unwrap(),
            CasOutcome::Conflict(_)
        ));
        for split in &revision.splits {
            for page in &split.pages {
                let page_key = split::key::membership_page(
                    &identity,
                    snapshot,
                    revision.revision,
                    &split.name,
                    page.page,
                )
                .unwrap();
                assert_retired_fence(store.as_ref(), &page_key).await;
            }
        }
    }
    assert!(restarted
        .get_split_revision(&identity, root, foreign.revision, common::budget(), &cancel)
        .await
        .is_ok());
    assert!(restarted
        .commit_status(&identity, first_attempt.id)
        .await
        .unwrap()
        .is_none());
    assert!(restarted.inspect_split_head(&identity, snapshot).await.is_err());
    assert!(restarted.get_manifest(&identity, snapshot).await.is_err());
}

#[tokio::test]
async fn cleanup_completion_is_helped_after_final_status_write_failure() {
    let (store, authority, identity, snapshot) = split_store::fixture().await;
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1000));
    let authority = authority.with_reclaim_clock_for_tests(clock.clone());
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(&identity, &common::attempt(&authority, &identity, root).await)
        .await
        .unwrap();
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    clock.store(1300, std::sync::atomic::Ordering::Release);
    store
        .reject_cleanup_finish
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let mut failed = false;
    for _ in 0..20 {
        if authority
            .reclaim_snapshot_metadata_step(&identity, snapshot, &ReadCancellation::new())
            .await
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert_eq!(
        authority
            .reclaim_progress(&identity, snapshot)
            .await
            .unwrap()
            .state,
        ReclaimState::Pending
    );
    let restarted = DatasetAuthority::new(store).with_reclaim_clock_for_tests(clock);
    assert!(
        restarted
            .reclaim_snapshot_metadata_step(&identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
    );
    assert_eq!(
        restarted
            .reclaim_progress(&identity, snapshot)
            .await
            .unwrap()
            .state,
        ReclaimState::Completed
    );
}

async fn assert_retired_fence(store: &dyn DatasetStore, key: &[u8]) {
    let value = store.get(key).await.unwrap().unwrap();
    assert_eq!(value.bytes, b"split-retired-v1");
    assert!(matches!(
        store
            .compare_exchange(key, None, b"delayed payload")
            .await
            .unwrap(),
        CasOutcome::Conflict(_)
    ));
}
