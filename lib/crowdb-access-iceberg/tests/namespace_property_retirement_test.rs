// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace_property_retirement.rs"]
mod retirement;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::CatalogError,
    error::ValidationError,
    key::NamespaceId,
    namespace::{
        authority_key, name_key, NamespaceAuthority, NamespaceDropRequest, NamespaceDropper,
        NamespacePropertyRequest, NamespaceRepository, PropertyChanges,
    },
    record::StorageRecord,
};

use fixture::TestNamespace;
use retirement::TestPropertyRetirementStore;

#[path = "namespace_property_retirement/legacy.rs"]
mod legacy;

async fn setup(depth: usize) -> (TestNamespace, NamespaceAuthority) {
    let fixture = TestNamespace::new().await;
    let names = ["root", "branch", "parent"];
    let mut parent = None;
    let mut selected = None;
    for level in 1..=depth {
        let authority = fixture.authority(parent, &names[..level]);
        fixture.publish(&authority).await;
        parent = Some(authority.namespace);
        selected = Some(authority);
    }
    (fixture, selected.unwrap())
}

async fn start_property_write(
    fixture: &TestNamespace,
    authority: &NamespaceAuthority,
) -> (
    Arc<TestPropertyRetirementStore>,
    tokio::task::JoinHandle<Result<Option<crowdb_access_iceberg::namespace::PropertyUpdate>, CatalogError>>,
) {
    let store = TestPropertyRetirementStore::new(
        fixture.store.clone(),
        authority_key(authority.catalog, authority.namespace)
            .encode()
            .unwrap(),
    );
    let repository = NamespaceRepository::new(store.clone());
    let request = NamespacePropertyRequest {
        context: fixture.context,
        identifier: authority.identifier.clone(),
        changes: PropertyChanges {
            removals: Vec::new(),
            updates: BTreeMap::from([("owner".into(), "updated".into())]),
        },
    };
    let pending = tokio::spawn(async move { repository.update_properties(&request).await });
    tokio::time::timeout(Duration::from_secs(5), store.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        store.guard_hits(),
        1,
        "the actual property CAS must reach its pause"
    );
    (store, pending)
}

#[tokio::test]
async fn property_cas_observes_real_namespace_retirement_as_conflict_without_confirmation_get() {
    for depth in [1, 3] {
        let (fixture, authority) = setup(depth).await;
        let (store, pending) = start_property_write(&fixture, &authority).await;
        assert_eq!(store.counts(), (2 * depth + 1, 1, 0));
        let request = NamespaceDropRequest {
            context: fixture.context,
            identity: crowdb_access_iceberg::operation::RequestIdentity {
                operation: crowdb_access_iceberg::key::OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            identifier: authority.identifier.clone(),
        };
        assert_eq!(
            NamespaceDropper::new(fixture.store.clone())
                .drop_namespace(&request)
                .await
                .unwrap()
                .unwrap()
                .status,
            204
        );
        let key = authority_key(authority.catalog, authority.namespace);
        let encoded = key.encode().unwrap();
        let retired = fixture.store.values.load()[&encoded].clone();
        assert_eq!(
            StorageRecord::decode(&key, &retired.bytes).unwrap(),
            StorageRecord::RetiredNamespace {
                catalog: authority.catalog,
                namespace: authority.namespace
            }
        );
        store.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, Err(CatalogError::Conflict)),
            "depth={depth}: {result:?}"
        );
        assert_eq!(store.guard_hits(), 1);
        assert_eq!(
            store.retired_observations(),
            1,
            "the stale property CAS must consume Retired"
        );
        assert_eq!(store.authority_readbacks(), 0);
        assert_eq!(
            store.counts(),
            (2 * depth + 1, 1, 0),
            "no store call follows the definite retirement conflict"
        );
        assert_eq!(
            fixture.store.values.load()[&encoded],
            retired,
            "the failed writer must preserve the terminal slot exactly"
        );
        let mapping = name_key(authority.catalog, authority.parent, authority.identifier.name())
            .unwrap()
            .encode()
            .unwrap();
        assert!(!fixture.store.values.load().contains_key(&mapping));
    }
}

#[tokio::test]
async fn malformed_and_foreign_retirement_observations_keep_the_original_validation_error() {
    for foreign in [false, true] {
        let (fixture, authority) = setup(1).await;
        let (store, pending) = start_property_write(&fixture, &authority).await;
        let key = authority_key(authority.catalog, authority.namespace);
        let bytes = if foreign {
            StorageRecord::RetiredNamespace {
                catalog: authority.catalog,
                namespace: NamespaceId::random(),
            }
            .encode()
            .unwrap()
        } else {
            b"malformed namespace record".to_vec()
        };
        let expected = StorageRecord::decode(&key, &bytes).unwrap_err();
        assert_eq!(
            expected,
            if foreign {
                ValidationError::IdentityMismatch
            } else {
                ValidationError::Record
            }
        );
        fixture.bytes(key.clone(), &bytes).await;
        store.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(&result, Err(CatalogError::Invalid(error)) if error == &expected),
            "foreign={foreign}: {result:?}"
        );
        assert_eq!(store.guard_hits(), 1);
        assert_eq!(store.retired_observations(), 0);
        assert_eq!(store.counts(), (3, 1, 0));
        assert_eq!(fixture.store.values.load()[&key.encode().unwrap()].bytes, bytes);
    }
}
