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

async fn prepare(authority: &DatasetAuthority, identity: &DatasetIdentity, parent: SnapshotId) -> SnapshotId {
    let snapshot = SnapshotId::random();
    authority
        .prepare_composition_resource(
            identity,
            snapshot,
            &common::request(parent),
            None,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    snapshot
}

#[tokio::test]
async fn composition_retry_observes_current_effect_without_a_workflow_record() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let snapshot = prepare(&authority, &identity, parent).await;
    let update = SnapshotHeadUpdate {
        snapshot,
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    store.lose_data_reply.store(true, Ordering::Relaxed);
    assert!(authority.update_snapshot_head(&identity, &update).await.is_err());
    let restarted = DatasetAuthority::new(store);
    assert_eq!(
        restarted
            .update_snapshot_head(&identity, &update)
            .await
            .unwrap()
            .latest,
        Some(snapshot)
    );
    assert!(restarted.get_snapshot(&identity, snapshot).await.is_ok());
}

#[tokio::test]
async fn composition_head_conflict_preserves_complete_content_for_an_explicit_retry() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let snapshot = prepare(&authority, &identity, parent).await;
    let mut update = SnapshotHeadUpdate {
        snapshot,
        expected_head: authority.inspect_head(&identity).await.unwrap().generation,
    };
    let other = prepare(&authority, &identity, parent).await;
    authority
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot: other,
                expected_head: update.expected_head,
            },
        )
        .await
        .unwrap();
    let AuthorityError::SnapshotHeadConflict { observed } = authority
        .update_snapshot_head(&identity, &update)
        .await
        .unwrap_err()
    else {
        panic!("missing conflict observation");
    };
    assert_eq!(observed.latest, Some(other));
    assert!(authority.get_manifest(&identity, snapshot).await.is_ok());
    update.expected_head = observed.generation;
    assert_eq!(
        authority
            .update_snapshot_head(&identity, &update)
            .await
            .unwrap()
            .latest,
        Some(snapshot)
    );
    assert_eq!(
        authority
            .get_snapshot(&identity, snapshot)
            .await
            .unwrap()
            .publication
            .parent,
        Some(parent)
    );
}
