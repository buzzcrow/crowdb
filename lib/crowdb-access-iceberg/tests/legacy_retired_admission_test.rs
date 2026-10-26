// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    namespace::{
        authority_key, name_key, NamespaceCreator, NamespaceDropper, NamespaceJournal, NamespacePhase,
    },
    operation::PayloadStore,
    record::StorageRecord,
    table::{TableLifecyclePhase, TableLifecycles, TableRepository},
};
use std::{sync::atomic::Ordering, time::Duration};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/legacy_retired_admission.rs"]
mod legacy;
#[path = "common/namespace_store.rs"]
mod namespace_store;
use legacy::{PauseAt, TestLegacyAdmissionStore};

#[tokio::test]
async fn legacy_child_parent_get_observes_retirement_as_the_original_bad_request() {
    assert_child_admission(PauseAt::ParentGet).await;
}

#[tokio::test]
async fn legacy_child_admission_cas_observes_retirement_as_the_original_bad_request() {
    assert_child_admission(PauseAt::ParentCas).await;
}

#[tokio::test]
async fn legacy_rename_parent_get_observes_retirement_as_no_such_namespace() {
    assert_table_admission(PauseAt::ParentGet).await;
}

#[tokio::test]
async fn legacy_rename_admission_cas_observes_retirement_as_no_such_namespace() {
    assert_table_admission(PauseAt::ParentCas).await;
}

async fn assert_child_admission(pause_at: PauseAt) {
    let (fixture, parent, operation) = legacy::namespace_case(pause_at).await;
    let key = authority_key(parent.catalog, parent.namespace);
    let store = TestLegacyAdmissionStore::new(
        fixture.store.clone(),
        key.encode().unwrap(),
        operation.identity.operation,
        pause_at,
    );
    let creator = NamespaceCreator::new(store.clone());
    let context = fixture.context;
    let identity = operation.identity.operation;
    let worker = tokio::spawn(async move { creator.resume(context, identity).await });
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        NamespaceDropper::new(store.clone())
            .drop_namespace(&legacy::drop_request(&fixture, &parent))
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    let journal = NamespaceJournal::new(fixture.store.clone());
    assert_eq!(
        journal.load(context, identity).await.unwrap().unwrap().phase,
        NamespacePhase::Aborted,
        "drop must help the legacy reservation to its terminal result before retiring the parent"
    );
    let retired = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    assert!(matches!(
        StorageRecord::decode(&key, &retired.bytes).unwrap(),
        StorageRecord::RetiredNamespace { .. }
    ));
    store.release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(outcome.status, 400);
    assert_eq!(store.pauses.load(Ordering::SeqCst), 1);
    let bytes = PayloadStore::new(fixture.store.clone())
        .get(&outcome.body)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["error"]["type"],
        "BadRequestException"
    );
    let child = name_key(context.catalog, operation.parent, operation.identifier.name()).unwrap();
    assert!(fixture
        .store
        .get(&child.encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        fixture.store.get(&key.encode().unwrap()).await.unwrap(),
        Some(retired)
    );
    assert_eq!(
        NamespaceCreator::new(store)
            .resume(context, identity)
            .await
            .unwrap(),
        outcome
    );
}

async fn assert_table_admission(pause_at: PauseAt) {
    let (fixture, parent, operation) = legacy::table_case(pause_at).await;
    let key = authority_key(parent.catalog, parent.namespace);
    let store = TestLegacyAdmissionStore::new(
        fixture.store.clone(),
        key.encode().unwrap(),
        operation.identity.operation,
        pause_at,
    );
    let manager = TableLifecycles::new(store.clone());
    let context = fixture.context;
    let identity = operation.identity.operation;
    let worker = tokio::spawn(async move { manager.resume(context, identity).await });
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        NamespaceDropper::new(store.clone())
            .drop_namespace(&legacy::drop_request(&fixture, &parent))
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    assert_eq!(
        TableLifecycles::new(fixture.store.clone())
            .load(context, identity)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TableLifecyclePhase::Aborted,
        "drop must help the legacy reservation to its terminal result before retiring the parent"
    );
    let retired = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    assert!(matches!(
        StorageRecord::decode(&key, &retired.bytes).unwrap(),
        StorageRecord::RetiredNamespace { .. }
    ));
    store.release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(outcome.status, 404);
    assert_eq!(store.pauses.load(Ordering::SeqCst), 1);
    let bytes = PayloadStore::new(fixture.store.clone())
        .get(&outcome.body)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["error"]["type"],
        "NoSuchNamespaceException"
    );
    let tables = TableRepository::new(fixture.store.clone());
    let source = tables
        .select(context, operation.before.namespace, &operation.before.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.head, operation.before);
    assert!(tables
        .select(context, parent.namespace, &operation.candidate.name)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        fixture.store.get(&key.encode().unwrap()).await.unwrap(),
        Some(retired)
    );
    assert_eq!(
        TableLifecycles::new(store)
            .resume(context, identity)
            .await
            .unwrap(),
        outcome
    );
}
