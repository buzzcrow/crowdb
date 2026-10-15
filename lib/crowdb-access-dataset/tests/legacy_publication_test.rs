// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
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
    // Emulate records written before candidate bindings and commit receipts existed.
    for snapshot in [root, child] {
        let receipt = authority
            .snapshot_commit(&identity, snapshot)
            .await
            .unwrap()
            .unwrap();
        for (prefix, suffix) in [
            (b"DS\0CB\0".as_slice(), snapshot.as_bytes()),
            (b"DS\0SC\0", snapshot.as_bytes()),
            (b"DS\0CR\0", receipt.attempt.id.as_bytes()),
        ] {
            let key = [prefix, identity.key().unwrap().as_slice(), suffix].concat();
            store.delete(&key).await.unwrap();
        }
    }
    let head_key = [b"DS\0H\0".as_slice(), identity.key().unwrap().as_slice()].concat();
    let old = store.get(&head_key).await.unwrap().unwrap();
    let bytes = bincode::serialize(&(
        1u16,
        HeadRecord {
            latest: Some(child),
            stable: None,
        },
    ))
    .unwrap();
    assert!(matches!(
        store
            .compare_exchange(&head_key, Some(&old.bytes), &bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
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
