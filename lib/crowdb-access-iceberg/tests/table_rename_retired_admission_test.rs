// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{sync::atomic::Ordering, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogStore},
    namespace::{authority_key, NamespaceDropper, NamespaceRepository},
    record::StorageRecord,
    table::{TableLifecycles, TableRepository},
};

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/table_rename_retired_admission.rs"]
mod retired;

#[tokio::test]
async fn retired_destination_observation_cancels_admission_and_releases_the_source() {
    let test = retired::TestRetiredAdmission::new().await;
    let tables = TableRepository::new(test.fixture.store.clone());
    let original = tables
        .select(test.fixture.context, test.source.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    let manager = TableLifecycles::new(test.store.clone());
    let request = test.request.clone();
    let worker = tokio::spawn(async move { manager.execute(&request).await });
    tokio::time::timeout(Duration::from_secs(5), test.store.entered.notified())
        .await
        .unwrap();
    let source = NamespaceRepository::new(test.fixture.store.clone())
        .load(test.fixture.context, &test.source.identifier)
        .await
        .unwrap()
        .unwrap();
    assert!(
        source.rename.is_some(),
        "source occupancy must win before target CAS pauses"
    );
    assert_eq!(
        NamespaceDropper::new(test.store.clone())
            .drop_namespace(&test.drop_target())
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    let target_key = authority_key(test.fixture.context.catalog, test.target.namespace);
    let target = test
        .fixture
        .store
        .get(&target_key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        StorageRecord::decode(&target_key, &target.bytes).unwrap(),
        StorageRecord::RetiredNamespace { .. }
    ));
    test.store.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(CatalogError::Conflict)), "{result:?}");
    assert_eq!(test.store.pauses.load(Ordering::SeqCst), 1);
    let source = NamespaceRepository::new(test.fixture.store.clone())
        .load(test.fixture.context, &test.source.identifier)
        .await
        .unwrap()
        .unwrap();
    assert!(
        source.rename.is_none(),
        "failed target admission must release source occupancy"
    );
    let remaining = tables
        .select(test.fixture.context, test.source.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remaining.head.table, original.head.table);
    assert_eq!(remaining.head.metadata_file, original.head.metadata_file);
    assert!(tables
        .select(test.fixture.context, test.target.namespace, "renamed")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        test.fixture
            .store
            .get(&target_key.encode().unwrap())
            .await
            .unwrap(),
        Some(target)
    );
}
