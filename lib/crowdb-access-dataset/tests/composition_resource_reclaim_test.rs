// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{composition::*, split::*, *};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[tokio::test]
async fn interrupted_resource_is_discoverable_and_retirement_blocks_recomputation() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = authority.with_reclaim_clock_for_tests(clock.clone());
    let snapshot = SnapshotId::random();
    let request = CompositionRequest {
        parent: Some(parent),
        source: CompositionSource::All,
        schema: None,
        samples: Vec::new(),
        splits: split_store::partition_splits(),
        policy: SplitPolicy::default(),
    };
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    store.lose_member_reply.store(true, Ordering::Relaxed);
    assert!(authority
        .prepare_composition_resource(&identity, snapshot, &request, None, &ReadCancellation::new())
        .await
        .is_err());
    assert!(authority.get_snapshot(&identity, snapshot).await.is_err());
    let mut prefix = b"DS\0SM\0".to_vec();
    prefix.extend(identity.key().unwrap());
    prefix.extend(snapshot.as_bytes());
    let pages = store
        .scan_page(
            &MetadataScan {
                prefix,
                after: None,
                budget: common::budget(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(pages.entries.len(), 1);
    assert!(pages.after.is_none());
    store.reject_workflow_writes.store(false, Ordering::Relaxed);
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    clock.store(1300, Ordering::Release);
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock);
    let mut complete = false;
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
            complete = true;
            break;
        }
    }
    assert!(complete);
    for (key, previous) in pages.entries {
        assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, b"split-retired-v1");
        assert!(matches!(
            store.compare_exchange(&key, None, &previous.bytes).await.unwrap(),
            CasOutcome::Conflict(_)
        ));
    }
    let before = store.mutations.load(Ordering::Relaxed);
    assert!(restarted
        .prepare_composition_resource(&identity, snapshot, &request, None, &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
    assert!(restarted.get_snapshot(&identity, parent).await.is_ok());
}
