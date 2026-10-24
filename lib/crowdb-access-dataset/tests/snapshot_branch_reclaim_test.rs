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
async fn historical_branch_children_protect_parent_until_terminal_retirement() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let child = authority
        .publish_snapshot(&identity, Some(parent), publication::publication_manifest(2))
        .await
        .unwrap();
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
    assert!(matches!(
        authority.begin_reclaim(&identity, parent).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    assert_eq!(
        authority
            .begin_reclaim(&identity, child)
            .await
            .unwrap()
            .publication
            .state,
        PublicationState::Aborted
    );
    assert_eq!(
        authority
            .begin_reclaim(&identity, parent)
            .await
            .unwrap()
            .publication
            .state,
        PublicationState::Aborted
    );
    assert!(authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(4),
            OperationId::random()
        )
        .await
        .is_err());
}

#[tokio::test]
async fn temporary_child_reclaim_fence_is_not_terminal_retirement() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let child = authority
        .publish_snapshot(&identity, Some(parent), publication::publication_manifest(2))
        .await
        .unwrap();
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
    let key = [
        b"DS\0L\0".as_slice(),
        identity.key().unwrap().as_slice(),
        child.as_bytes(),
    ]
    .concat();
    let old = store.get(&key).await.unwrap();
    let fenced = bincode::serialize(&(2u16, true, 0u64, 0u64, Vec::<(OperationId, u64)>::new())).unwrap();
    store
        .compare_exchange(&key, old.as_ref().map(|s| s.bytes.as_slice()), &fenced)
        .await
        .unwrap();
    assert!(matches!(
        authority.begin_reclaim(&identity, parent).await,
        Err(AuthorityError::SnapshotProtected)
    ));
}

#[tokio::test]
async fn complete_unpublished_children_protect_parent_until_their_retirement() {
    let (_, authority, identity, parent) = split_store::fixture().await;
    let candidate = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
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
    assert!(matches!(
        authority.begin_reclaim(&identity, parent).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    authority.begin_reclaim(&identity, candidate).await.unwrap();
    authority.begin_reclaim(&identity, parent).await.unwrap();
    assert!(authority
        .publish_snapshot_candidate(
            &identity,
            &common::attempt(&authority, &identity, candidate).await
        )
        .await
        .is_err());
}
