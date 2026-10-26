// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/namespace_property_admission.rs"]
mod admission;
#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use admission::{TestInterruption, TestPropertyAdmissionStore};
use crowdb_access_iceberg::{
    catalog::{ActiveCatalogRecord, CatalogError, RootState},
    key::{CatalogId, IcebergKey, OperationId, SystemScope},
    namespace::{
        authority_key, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle,
        NamespacePropertyRequest, NamespaceRepository, PropertyChanges,
    },
    record::StorageRecord,
};
use fixture::TestNamespace;
use std::collections::BTreeMap;

async fn setup(depth: usize) -> (TestNamespace, NamespaceAuthority) {
    let fixture = TestNamespace::new().await;
    let names = ["root", "branch", "leaf"];
    let mut parent = None;
    let mut leaf = None;
    for level in 1..=depth {
        let authority = fixture.authority(parent, &names[..level]);
        fixture.publish(&authority).await;
        parent = Some(authority.namespace);
        leaf = Some(authority);
    }
    (fixture, leaf.unwrap())
}

fn request(fixture: &TestNamespace, leaf: &NamespaceAuthority) -> NamespacePropertyRequest {
    NamespacePropertyRequest {
        context: fixture.context,
        identifier: leaf.identifier.clone(),
        changes: PropertyChanges {
            removals: Vec::new(),
            updates: BTreeMap::from([("owner".into(), "value".into())]),
        },
    }
}

#[tokio::test]
async fn property_requests_check_catalog_once_at_each_request_boundary() {
    for depth in [1, 3] {
        let (fixture, leaf) = setup(depth).await;
        let store = TestPropertyAdmissionStore::new(fixture.store.clone(), None);
        let repository = NamespaceRepository::new(store.clone());
        let request = request(&fixture, &leaf);
        repository.update_properties(&request).await.unwrap().unwrap();
        assert_eq!(store.counts(), (2 * depth + 2, 2, 1));
        repository.update_properties(&request).await.unwrap().unwrap();
        assert_eq!(store.counts(), (2 * (2 * depth + 2), 4, 1));
    }
}

#[tokio::test]
async fn public_namespace_load_keeps_both_of_its_own_context_checks() {
    for depth in [1, 3] {
        let (fixture, leaf) = setup(depth).await;
        let store = TestPropertyAdmissionStore::new(fixture.store.clone(), None);
        assert_eq!(
            NamespaceRepository::new(store.clone())
                .load(fixture.context, &leaf.identifier)
                .await
                .unwrap(),
            Some(leaf)
        );
        assert_eq!(store.counts(), (2 * depth + 2, 2, 0));
    }
}

#[tokio::test]
async fn missing_property_targets_still_check_the_final_catalog_context() {
    for depth in [1, 3] {
        let (fixture, leaf) = setup(depth).await;
        let mut request = request(&fixture, &leaf);
        let mut names = request.identifier.components().to_vec();
        *names.last_mut().unwrap() = "absent".into();
        request.identifier = NamespaceIdentifier::new(names).unwrap();
        let store = TestPropertyAdmissionStore::new(fixture.store.clone(), None);
        assert!(NamespaceRepository::new(store.clone())
            .update_properties(&request)
            .await
            .unwrap()
            .is_none());
        assert_eq!(store.counts(), (2 * depth + 1, 2, 0));
        let root = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let interruption = TestInterruption::new(
            &fixture.store,
            name_key(fixture.context.catalog, leaf.parent, "absent")
                .unwrap()
                .encode()
                .unwrap(),
            &root,
            &StorageRecord::Active(ActiveCatalogRecord {
                context: fixture.context.replacement(CatalogId::random()).unwrap(),
                operation: OperationId::random(),
                state: RootState::Ready,
            }),
        );
        let store = TestPropertyAdmissionStore::new(fixture.store.clone(), Some(interruption));
        assert!(matches!(
            NamespaceRepository::new(store.clone())
                .update_properties(&request)
                .await,
            Err(CatalogError::Conflict)
        ));
        assert_eq!(store.counts(), (2 * depth + 1, 2, 0));
    }
}

#[tokio::test]
async fn catalog_clear_or_replacement_prevents_both_mutated_and_noop_success() {
    for no_op in [false, true] {
        for replacement in [false, true] {
            let (fixture, leaf) = setup(3).await;
            let mut request = request(&fixture, &leaf);
            if no_op {
                request.changes = PropertyChanges::default();
            }
            let root = ActiveCatalogRecord {
                context: if replacement {
                    fixture.context.replacement(CatalogId::random()).unwrap()
                } else {
                    fixture.context
                },
                operation: OperationId::random(),
                state: if replacement {
                    RootState::Ready
                } else {
                    RootState::Fencing
                },
            };
            let interruption = TestInterruption::new(
                &fixture.store,
                authority_key(leaf.catalog, leaf.namespace).encode().unwrap(),
                &IcebergKey::System {
                    scope: SystemScope::ActiveRoot,
                    suffix: Vec::new(),
                },
                &StorageRecord::Active(root),
            );
            let store = TestPropertyAdmissionStore::new(fixture.store.clone(), Some(interruption));
            let error = NamespaceRepository::new(store.clone())
                .update_properties(&request)
                .await
                .unwrap_err();
            assert_eq!(matches!(error, CatalogError::Conflict), replacement, "{error:?}");
            assert_ne!(matches!(error, CatalogError::Busy), replacement, "{error:?}");
            assert_eq!(store.counts(), (8, 2, usize::from(!no_op)));
        }
    }
}

#[tokio::test]
async fn a_dropping_leaf_observation_stops_the_exact_property_cas_without_a_reload() {
    for depth in [1, 3] {
        let (fixture, leaf) = setup(depth).await;
        let key = authority_key(leaf.catalog, leaf.namespace);
        let mut dropping = leaf.clone();
        dropping.lifecycle = NamespaceLifecycle::Dropping;
        dropping.pending_operation = Some(OperationId::random());
        dropping.mutation_revision += 1;
        let interruption = TestInterruption::new(
            &fixture.store,
            key.encode().unwrap(),
            &key,
            &StorageRecord::NamespaceAuthority(Box::new(dropping.clone())),
        );
        let store = TestPropertyAdmissionStore::new(fixture.store.clone(), Some(interruption));
        assert!(matches!(
            NamespaceRepository::new(store.clone())
                .update_properties(&request(&fixture, &leaf))
                .await,
            Err(CatalogError::Conflict)
        ));
        assert_eq!(store.counts(), (2 * depth + 1, 1, 1));
        let bytes = &store.inner.values.load()[&key.encode().unwrap()].bytes;
        assert_eq!(
            StorageRecord::decode(&key, bytes).unwrap(),
            StorageRecord::NamespaceAuthority(Box::new(dropping))
        );
    }
}
