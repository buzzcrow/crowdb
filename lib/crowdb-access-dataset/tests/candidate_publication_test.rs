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

async fn prepare(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    parent: Option<SnapshotId>,
    marker: u8,
) -> SnapshotId {
    authority
        .prepare_snapshot_candidate(
            identity,
            parent,
            publication::publication_manifest(marker),
            OperationId::random(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn historical_parent_and_new_roots_use_independent_head_preconditions() {
    let (_, authority, identity, root) = split_store::fixture().await;
    let first = prepare(&authority, &identity, Some(root), 2).await;
    let second = prepare(&authority, &identity, Some(root), 3).await;
    let first_attempt = common::attempt(&authority, &identity, first).await;
    let stale = common::attempt(&authority, &identity, second).await;
    authority
        .publish_snapshot_candidate(&identity, &first_attempt)
        .await
        .unwrap();
    let error = authority
        .publish_snapshot_candidate(&identity, &stale)
        .await
        .unwrap_err();
    let AuthorityError::SnapshotHeadConflict { observed } = error else {
        panic!("{error:?}")
    };
    assert_eq!(observed.latest, Some(first));
    let retry = CommitAttempt {
        expected_head: observed.generation,
        ..stale
    };
    authority
        .publish_snapshot_candidate(&identity, &retry)
        .await
        .unwrap();
    assert_eq!(
        authority
            .get_snapshot(&identity, second)
            .await
            .unwrap()
            .publication
            .parent,
        Some(root)
    );
    let new_root = prepare(&authority, &identity, None, 4).await;
    authority
        .publish_snapshot_candidate(&identity, &common::attempt(&authority, &identity, new_root).await)
        .await
        .unwrap();
    let head = authority.inspect_head(&identity).await.unwrap();
    assert!(matches!(
        authority
            .publish_snapshot_candidate(&identity, &first_attempt)
            .await,
        Err(AuthorityError::SnapshotHeadConflict { .. })
    ));
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), head);
    assert_eq!(head.latest, Some(new_root));
    assert!(authority.get_snapshot(&identity, first).await.is_ok());
    assert!(authority
        .commit_status(&identity, first_attempt.id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn complete_unpublished_results_are_parents_and_stable_targets_without_attempt_identity_state() {
    let (_, authority, identity, _) = split_store::fixture().await;
    let parent = prepare(&authority, &identity, None, 7).await;
    let child = prepare(&authority, &identity, Some(parent), 8).await;
    authority.set_stable(&identity, parent).await.unwrap();
    let attempt = common::attempt(&authority, &identity, parent).await;
    authority
        .publish_snapshot_candidate(&identity, &attempt)
        .await
        .unwrap();
    let current = authority.inspect_head(&identity).await.unwrap();
    let reviewed = CommitAttempt {
        target: CommitTarget::Snapshot { snapshot: child },
        expected_head: current.generation,
        ..attempt
    };
    authority
        .publish_snapshot_candidate(&identity, &reviewed)
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), Some(child));
    assert!(authority
        .commit_status(&identity, attempt.id)
        .await
        .unwrap()
        .is_none());
    assert!(authority
        .snapshot_commit(&identity, parent)
        .await
        .unwrap()
        .is_none());
}
