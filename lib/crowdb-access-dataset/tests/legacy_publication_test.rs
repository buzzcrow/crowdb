// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/legacy.rs"]
mod legacy;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
use publication::publication_manifest;
use std::sync::Arc;

#[tokio::test]
async fn legacy_ancestry_remains_available_after_publishing_a_new_root() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-branch").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let operation = OperationId::random();
    let root = authority
        .publish_snapshot_with_operation(&identity, None, publication_manifest(1), operation)
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(root), publication_manifest(2))
        .await
        .unwrap();
    legacy::emulate_legacy(store.as_ref(), &authority, &identity, &[root, child], child).await;
    let new_root = authority
        .prepare_snapshot_candidate(&identity, None, publication_manifest(3), OperationId::random())
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(
            &identity,
            &CommitAttempt {
                version: 1,
                id: OperationId::random(),
                target: CommitTarget::Snapshot { snapshot: new_root },
                expected_head: authority.inspect_head(&identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    let branch = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(root),
            publication_manifest(4),
            OperationId::random(),
        )
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(
            &identity,
            &CommitAttempt {
                version: 1,
                id: OperationId::random(),
                target: CommitTarget::Snapshot { snapshot: branch },
                expected_head: authority.inspect_head(&identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        authority
            .get_snapshot(&identity, branch)
            .await
            .unwrap()
            .publication
            .parent,
        Some(root)
    );
    let head = authority.inspect_head(&identity).await.unwrap();
    assert_eq!(
        authority
            .publish_snapshot_with_operation(&identity, None, publication_manifest(1), operation)
            .await
            .unwrap(),
        root
    );
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), head);
}

#[tokio::test]
async fn unindexed_legacy_child_protects_parent_after_latest_moves_to_new_root() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-gc").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let root = authority
        .publish_snapshot(&identity, None, publication_manifest(1))
        .await
        .unwrap();
    let child = authority
        .publish_snapshot(&identity, Some(root), publication_manifest(2))
        .await
        .unwrap();
    legacy::emulate_legacy(store.as_ref(), &authority, &identity, &[root, child], child).await;
    let child_index = [
        b"DS\0IC\0".as_slice(),
        identity.key().unwrap().as_slice(),
        &[1],
        root.as_bytes(),
        child.as_bytes(),
    ]
    .concat();
    store.delete(&child_index).await.unwrap();
    let new_root = authority
        .prepare_snapshot_candidate(&identity, None, publication_manifest(3), OperationId::random())
        .await
        .unwrap();
    authority
        .publish_snapshot_candidate(
            &identity,
            &CommitAttempt {
                version: 1,
                id: OperationId::random(),
                target: CommitTarget::Snapshot { snapshot: new_root },
                expected_head: authority.inspect_head(&identity).await.unwrap().generation,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        authority.begin_reclaim(&identity, root).await,
        Err(AuthorityError::SnapshotProtected)
    ));
    authority.begin_reclaim(&identity, child).await.unwrap();
    authority.begin_reclaim(&identity, root).await.unwrap();
}

#[tokio::test]
async fn completing_an_already_current_legacy_manifest_installs_a_readable_resource_root() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-complete").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, publication_manifest(1))
        .await
        .unwrap();
    let manifest = authority.get_manifest(&identity, snapshot).await.unwrap();
    legacy::emulate_legacy(store.as_ref(), &authority, &identity, &[snapshot], snapshot).await;
    let head = authority.inspect_head(&identity).await.unwrap();
    authority
        .persist_manifest(&identity, snapshot, &manifest, 1024)
        .await
        .unwrap();
    assert_eq!(authority.inspect_head(&identity).await.unwrap(), head);
    let cb = [
        b"DS\0CB\0".as_slice(),
        &identity.key().unwrap(),
        snapshot.as_bytes(),
    ]
    .concat();
    let root: serde_json::Value =
        serde_json::from_slice(&store.get(&cb).await.unwrap().unwrap().bytes).unwrap();
    assert_eq!(root["version"], 2);
    assert!(DatasetAuthority::new(store)
        .read_manifest(&identity, snapshot)
        .await
        .is_ok());
}

#[tokio::test]
async fn a_stable_only_legacy_root_remains_readable_after_both_names_move() {
    let store = Arc::new(dataset_store::TestDatasetStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-stable").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let old = authority
        .publish_snapshot(&identity, None, publication_manifest(1))
        .await
        .unwrap();
    legacy::emulate_legacy(store.as_ref(), &authority, &identity, &[old], old).await;
    let head_key = [b"DS\0H\0".as_slice(), &identity.key().unwrap()].concat();
    let stored = store.get(&head_key).await.unwrap().unwrap();
    let head = bincode::serialize(&(
        1u16,
        HeadRecord {
            latest: None,
            stable: Some(old),
        },
    ))
    .unwrap();
    store
        .compare_exchange(&head_key, Some(&stored.bytes), &head)
        .await
        .unwrap();
    let next = authority
        .prepare_snapshot_candidate(&identity, None, publication_manifest(2), OperationId::random())
        .await
        .unwrap();
    authority
        .update_snapshot_head(
            &identity,
            &SnapshotHeadUpdate {
                snapshot: next,
                expected_head: Some(CommitGeneration::from_head(&head)),
            },
        )
        .await
        .unwrap();
    authority.set_stable(&identity, next).await.unwrap();
    assert!(DatasetAuthority::new(store)
        .get_snapshot(&identity, old)
        .await
        .is_ok());
}
