// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_creation.rs"]
#[allow(dead_code)]
mod creation;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace_default_unknown.rs"]
mod unknown;

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogError, StoreError},
    commit::{CommitPublicationError, TableCreator},
    key::{CatalogScope, IcebergKey},
    namespace::{NamespaceIdentifier, NamespaceRepository},
    record::StorageRecord,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use unknown::TestDefaultUnknownStore;

async fn check_unknown(after_apply: bool) {
    let mut test = creation::TestCreation::new().await;
    test.fixture
        .put(
            IcebergKey::Catalog {
                catalog: test.fixture.context.catalog,
                scope: CatalogScope::Authority,
                suffix: Vec::new(),
            },
            StorageRecord::Authority(
                CatalogAuthority::new_native(test.fixture.context.catalog, "native".into()).unwrap(),
            ),
        )
        .await;
    test.request.namespace = NamespaceIdentifier::new(vec!["default".into()]).unwrap();
    let store = Arc::new(TestDefaultUnknownStore {
        inner: test.fixture.store.clone(),
        after_apply,
        failed: AtomicBool::new(false),
        reads_after_unknown: AtomicUsize::new(0),
        writes_after_unknown: AtomicUsize::new(0),
    });
    let result = TableCreator::new(store.clone(), test.blocks.clone())
        .create(&test.request)
        .await;
    assert!(
        matches!(
            result,
            Err(CommitPublicationError::Catalog(CatalogError::Store(
                StoreError::Response
            )))
        ),
        "{result:?}"
    );
    assert_eq!(
        store.reads_after_unknown.load(Ordering::SeqCst),
        0,
        "the wrapper must not confirm an unknown namespace creation"
    );
    assert_eq!(store.writes_after_unknown.load(Ordering::SeqCst), 0);
    assert_eq!(test.blocks.writes.load(Ordering::SeqCst), 0);
    assert!(!test
        .fixture
        .store
        .values
        .load()
        .iter()
        .any(|(key, value)| matches!(
            StorageRecord::decode(
                &crowdb_access_iceberg::key::StorageKey::decode(key).unwrap(),
                &value.bytes
            )
            .unwrap(),
            StorageRecord::TableHead(_) | StorageRecord::TableMapping(_) | StorageRecord::File(_)
        )));
    // A new complete client attempt may discover or finish the existing default namespace.
    assert_eq!(test.creator().create(&test.request).await.unwrap().status, 200);
    assert!(NamespaceRepository::new(test.fixture.store.clone())
        .exists(test.fixture.context, &test.request.namespace)
        .await
        .unwrap());
}

#[tokio::test]
async fn default_namespace_unknown_before_apply_returns_without_followup_io() {
    check_unknown(false).await;
}

#[tokio::test]
async fn default_namespace_unknown_after_apply_cannot_become_a_successful_table_creation() {
    check_unknown(true).await;
}
