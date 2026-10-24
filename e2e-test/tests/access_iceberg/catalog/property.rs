// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{atomic::AtomicU8, Arc};

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogStore, RoutedCatalogStore};
use crowdb_access_iceberg::key::{NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    authority_key, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceLifecycle, NamespaceMapping,
    NamespaceMappingState, NamespaceProperties, NamespacePropertyRequest, NamespaceRepository,
    PropertyChanges,
};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;

use super::fault::TestFaultStore;

pub async fn prepare(store: Arc<RoutedCatalogStore>, context: CatalogContext) -> NamespacePropertyRequest {
    let request = NamespacePropertyRequest {
        context,
        identifier: NamespaceIdentifier::new(vec!["property-recovery".into()]).unwrap(),
        changes: PropertyChanges {
            removals: vec!["gone".into()],
            updates: BTreeMap::from([("owner".into(), "survives-restart".into())]),
        },
    };
    let authority = NamespaceAuthority {
        catalog: context.catalog,
        namespace: NamespaceId::random(),
        parent: None,
        identifier: request.identifier.clone(),
        name_epoch: 1,
        property_revision: 1,
        admission_fence: 1,
        mutation_revision: 1,
        lifecycle: NamespaceLifecycle::Ready,
        pending_operation: None,
        rename: None,
        properties: NamespaceProperties::new(BTreeMap::from([("gone".into(), "old".into())])).unwrap(),
    };
    let mapping = NamespaceMapping {
        catalog: context.catalog,
        parent: None,
        name: request.identifier.name().into(),
        namespace: authority.namespace,
        name_epoch: 1,
        operation: OperationId::random(),
        state: NamespaceMappingState::Published,
    };
    for (key, record) in [
        (
            authority_key(context.catalog, authority.namespace),
            StorageRecord::NamespaceAuthority(Box::new(authority)),
        ),
        (
            name_key(context.catalog, None, request.identifier.name()).unwrap(),
            StorageRecord::NamespaceMapping(mapping),
        ),
    ] {
        let key = key.encode().unwrap();
        let bytes = record.encode().unwrap();
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
    }
    let before = super::scan::catalog_keys(&store, context).await;
    let fault = Arc::new(TestFaultStore {
        inner: store.clone(),
        mode: AtomicU8::new(3),
    });
    assert!(NamespaceRepository::new(fault)
        .update_properties(&request)
        .await
        .is_err());
    let applied = NamespaceRepository::new(store.clone())
        .load(context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.property_revision, 2);
    assert!(applied.pending_operation.is_none());
    assert!(applied.rename.is_none());
    assert!(!applied.properties.entries().contains_key("gone"));
    assert_eq!(before, super::scan::catalog_keys(&store, context).await);
    request
}

pub async fn verify(store: Arc<RoutedCatalogStore>, request: &NamespacePropertyRequest) {
    let repository = NamespaceRepository::new(store.clone());
    let outcome = repository.update_properties(request).await.unwrap().unwrap();
    assert!(outcome.removed.is_empty());
    assert_eq!(outcome.updated, ["owner"]);
    assert_eq!(outcome.missing, ["gone"]);
    assert_eq!(
        repository.update_properties(request).await.unwrap(),
        Some(outcome)
    );
    let authority = repository
        .load(request.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authority.property_revision, 2);
    assert_eq!(authority.name_epoch, 1);
    assert_eq!(authority.admission_fence, 1);
    assert_eq!(authority.pending_operation, None);
    assert_eq!(
        authority.properties.entries().get("owner").unwrap(),
        "survives-restart"
    );
}
