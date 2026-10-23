// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{split::*, *};

#[tokio::test]
async fn retired_owner_recovers_all_draft_work_without_touching_other_snapshots() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1000));
    let authority = authority.with_reclaim_clock_for_tests(clock.clone());
    let snapshot = split_store::publish_binary_samples(&authority, &identity, parent).await;
    let record = split_store::prepared(&authority, &identity, snapshot, None).await;
    assert!(matches!(
        authority
            .cleanup_snapshot_split_drafts_step(&identity, snapshot, &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::Frozen)
    ));
    let split = record.splits.iter().find(|s| s.count > 0).unwrap();
    let key = key::membership_page(&identity, snapshot, record.revision, &split.name, 0).unwrap();
    let delayed = store.get(&key).await.unwrap().unwrap();
    store.delete(&key).await.unwrap();
    // A terminated draft worker does not block snapshot deletion.
    let root = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(&identity, &common::attempt(&authority, &identity, root).await)
        .await
        .unwrap();
    let foreign = split_store::prepared(&authority, &identity, root, None).await;
    split_store::publish_revision(&authority, &identity, &foreign).await;
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    clock.store(1300, std::sync::atomic::Ordering::Release);
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock);
    let mut complete = false;
    for _ in 0..100 {
        let progress = restarted
            .cleanup_snapshot_split_drafts_step(&identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap();
        if progress.complete {
            assert_eq!(progress.scanned_drafts, 2);
            complete = true;
            break;
        }
    }
    assert!(complete);
    restarted
        .reclaim_snapshot_metadata(&identity, snapshot)
        .await
        .unwrap();
    assert!(matches!(
        store.compare_exchange(&key, None, &delayed.bytes).await.unwrap(),
        CasOutcome::Conflict(_)
    ));
    assert_eq!(
        restarted
            .get_split_revision(
                &identity,
                root,
                foreign.revision,
                common::budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        foreign
    );
    assert!(
        restarted
            .cleanup_snapshot_split_drafts_step(&identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
    );
}
