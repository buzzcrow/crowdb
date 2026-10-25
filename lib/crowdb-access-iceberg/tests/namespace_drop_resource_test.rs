// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace_recovery_store.rs"]
mod recovery_store;
#[path = "common/namespace_drop_resource.rs"]
mod resource;

use crowdb_access_iceberg::catalog::CatalogStore;
use crowdb_access_iceberg::key::{CatalogScope, IcebergKey};
use crowdb_access_iceberg::namespace::{
    authority_key, NamespaceCreator, NamespaceDropper, NamespaceLifecycle, NamespaceRepository,
};
use crowdb_access_iceberg::record::StorageRecord;
use resource::{creation, setup};

#[tokio::test]
async fn empty_drop_uses_only_final_resources_and_four_catalog_checks() {
    for depth in [1, 3] {
        let (fixture, store, authority, request) = setup(depth).await;
        let outcome = NamespaceDropper::new(store.clone())
            .drop_namespace(&request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.status, 204);
        let counts = store.counts();
        println!("namespace empty depth={depth}: {counts:?}");
        assert_eq!(counts, (2 * depth + 4, 2, 1, 2));
        assert!(fixture.store.values.load().keys().all(|encoded| !matches!(
            IcebergKey::decode(encoded),
            Ok(IcebergKey::Catalog {
                scope: CatalogScope::NamespaceOperation | CatalogScope::OperationPayload,
                ..
            })
        )));
        let key = authority_key(fixture.context.catalog, authority.namespace);
        let value = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
        assert!(matches!(
            StorageRecord::decode(&key, &value.bytes).unwrap(),
            StorageRecord::RetiredNamespace { catalog, namespace }
                if catalog == authority.catalog && namespace == authority.namespace
        ));
        assert!(NamespaceRepository::new(fixture.store.clone())
            .load(fixture.context, &request.identifier)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn fresh_retry_can_delete_recreated_empty_b_but_cannot_delete_nonempty_b() {
    for child in [false, true] {
        let (fixture, store, original, request) = setup(1).await;
        let dropper = NamespaceDropper::new(store.clone());
        assert_eq!(
            dropper.drop_namespace(&request).await.unwrap().unwrap().status,
            204
        );
        let creator = NamespaceCreator::new(fixture.store.clone());
        let names: Vec<_> = request
            .identifier
            .components()
            .iter()
            .map(String::as_str)
            .collect();
        creator.create(&creation(&fixture, &names)).await.unwrap();
        let reader = NamespaceRepository::new(fixture.store.clone());
        let replacement = reader
            .load(fixture.context, &request.identifier)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(replacement.namespace, original.namespace);
        if child {
            creator
                .create(&creation(&fixture, &["n0", "child"]))
                .await
                .unwrap();
        }
        store.reset_counts();
        assert_eq!(
            dropper.drop_namespace(&request).await.unwrap().unwrap().status,
            if child { 409 } else { 204 }
        );
        let current = reader.load(fixture.context, &request.identifier).await.unwrap();
        if child {
            let current = current.unwrap();
            assert_eq!(current.namespace, replacement.namespace);
            assert_eq!(current.lifecycle, NamespaceLifecycle::Ready);
            assert_eq!(current.pending_operation, None);
            assert_eq!(current.properties, replacement.properties);
            assert_eq!(store.counts(), (7, 2, 0, 1));
        } else {
            assert!(current.is_none());
            assert_eq!(store.counts(), (6, 2, 1, 2));
        }
    }
}

#[path = "namespace_drop_resource_test/races.rs"]
mod races;

#[path = "namespace_drop_resource_test/legacy.rs"]
mod legacy;
