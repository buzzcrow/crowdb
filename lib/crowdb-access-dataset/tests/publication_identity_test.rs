// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::{
    CasOutcome, DatasetAuthority, DatasetIdentity, DatasetStore, OperationId, SnapshotId,
};
use publication::publication_manifest;
use std::sync::atomic::Ordering;

fn operation_key(identity: &DatasetIdentity, operation: OperationId) -> Vec<u8> {
    [
        b"DS\0O\0".as_slice(),
        &identity.key().unwrap(),
        operation.as_bytes(),
    ]
    .concat()
}

#[tokio::test]
async fn caller_tokens_are_resource_identities_without_new_operation_mappings() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let operation = OperationId::random();
    let manifest = publication_manifest(17);
    let snapshot = authority
        .prepare_snapshot_with_operation(&identity, Some(parent), manifest.clone(), operation)
        .await
        .unwrap();
    assert_eq!(snapshot.as_bytes(), operation.as_bytes());
    assert!(store
        .get(&operation_key(&identity, operation))
        .await
        .unwrap()
        .is_none());
    let restarted = DatasetAuthority::new(store.clone());
    assert_eq!(
        restarted
            .publication_status(&identity, operation)
            .await
            .unwrap()
            .publication
            .snapshot,
        snapshot
    );
    assert_eq!(
        restarted
            .publish_snapshot_with_operation(&identity, Some(parent), manifest, operation)
            .await
            .unwrap(),
        snapshot
    );
    assert_eq!(restarted.latest(&identity).await.unwrap(), Some(snapshot));
    let opaque = OperationId::random();
    let prepared = restarted
        .publish_snapshot_with_operation(&identity, Some(snapshot), b"opaque-reference".to_vec(), opaque)
        .await
        .unwrap();
    assert_eq!(prepared.as_bytes(), opaque.as_bytes());
    assert!(store
        .get(&operation_key(&identity, opaque))
        .await
        .unwrap()
        .is_none());
    assert_eq!(restarted.latest(&identity).await.unwrap(), Some(snapshot));
}

#[tokio::test]
async fn existing_random_snapshot_mappings_are_reused_without_rewriting_the_ledger() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let operation = OperationId::random();
    let snapshot = SnapshotId::random();
    let key = operation_key(&identity, operation);
    assert!(matches!(
        store
            .compare_exchange(&key, None, snapshot.as_bytes())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    store.reject_workflow_writes.store(true, Ordering::Relaxed);
    let manifest = publication_manifest(17);
    assert_eq!(
        authority
            .prepare_snapshot_with_operation(&identity, Some(parent), manifest.clone(), operation)
            .await
            .unwrap(),
        snapshot
    );
    assert_eq!(
        authority
            .publish_snapshot_with_operation(&identity, Some(parent), manifest, operation)
            .await
            .unwrap(),
        snapshot
    );
    assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, snapshot.as_bytes());
    assert_eq!(
        DatasetAuthority::new(store)
            .publication_status(&identity, operation)
            .await
            .unwrap()
            .publication
            .snapshot,
        snapshot
    );
}
