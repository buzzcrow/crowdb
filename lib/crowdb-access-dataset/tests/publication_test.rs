// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_store.rs"]
mod dataset_store;
#[path = "common/publication.rs"]
mod publication;
use dataset_store::TestDatasetStore as MemoryStore;
use publication::publication_manifest;

use crowdb_access_dataset::{
    DatasetAuthority, DatasetIdentity, NamespacePath, OperationId, PublicationState,
};
use std::sync::Arc;

#[tokio::test]
async fn prepare_preserves_latest_and_recovers_the_same_candidate() {
    let store = Arc::new(MemoryStore::default());
    let authority = crowdb_access_dataset::DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "prepare-api").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let operation = OperationId::random();
    let manifest = publication_manifest(17);
    store.fail_next_snapshot_write();
    assert!(authority
        .prepare_snapshot_with_operation(&identity, None, manifest.clone(), operation)
        .await
        .is_err());
    let candidate = authority
        .prepare_snapshot_with_operation(&identity, None, manifest.clone(), operation)
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), None);
    assert_eq!(
        authority
            .publication_status(&identity, operation)
            .await
            .unwrap()
            .publication
            .state,
        PublicationState::Prepared
    );
    let restarted = crowdb_access_dataset::DatasetAuthority::new(store);
    assert_eq!(
        restarted
            .prepare_snapshot_with_operation(&identity, None, manifest.clone(), operation,)
            .await
            .unwrap(),
        candidate
    );
    assert!(restarted
        .prepare_snapshot_with_operation(&identity, None, publication_manifest(18), operation,)
        .await
        .is_err());
    assert_eq!(
        restarted
            .publish_snapshot_with_operation(&identity, None, manifest, operation,)
            .await
            .unwrap(),
        candidate
    );
    assert_eq!(restarted.latest(&identity).await.unwrap(), Some(candidate));
    assert_eq!(
        restarted
            .publication_status(&identity, operation)
            .await
            .unwrap()
            .publication
            .state,
        PublicationState::Published
    );
}

#[tokio::test]
async fn opened_dataset_reads_current_latest_and_stable_bindings() {
    let store = Arc::new(MemoryStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "bindings").unwrap();
    let id = authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .publish_snapshot(&identity, None, publication_manifest(91))
        .await
        .unwrap();
    let opened = authority.get_dataset(&identity).await.unwrap();
    assert_eq!(opened.id, id);
    assert_eq!(opened.latest, Some(snapshot));
    assert_eq!(opened.stable, None);
    authority.set_stable(&identity, snapshot).await.unwrap();
    let reopened = DatasetAuthority::new(store).get_dataset(&identity).await.unwrap();
    assert_eq!(reopened.latest, Some(snapshot));
    assert_eq!(reopened.stable, Some(snapshot));
}
