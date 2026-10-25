// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    commit::TableCreatePhase,
    namespace::{authority_key, NamespaceAuthority, NamespaceDropper, NamespaceMutation},
    operation::PayloadStore,
    record::StorageRecord,
    table::{name_key, TableMappingState},
};

use crate::legacy::TestLegacyCreation;

pub async fn retired_parent(native: bool) -> (TestLegacyCreation, NamespaceAuthority, Vec<u8>) {
    let mut test = TestLegacyCreation::with_layout(native).await;
    test.operation.phase = TableCreatePhase::Admitted;
    test.operation.stage = None;
    let parent = &test.staged.parent;
    let key = authority_key(parent.catalog, parent.namespace);
    let mut captured = parent.clone();
    captured.pending_operation = Some(test.operation.identity.operation);
    captured.mutation_revision += 1;
    let payloads = PayloadStore::new(test.staged.namespace.store.clone());
    let before = payloads
        .put(
            parent.catalog,
            test.operation.identity.operation,
            &StorageRecord::NamespaceAuthority(Box::new(parent.clone()))
                .encode()
                .unwrap(),
        )
        .await
        .unwrap();
    let after = payloads
        .put(
            parent.catalog,
            test.operation.identity.operation,
            &StorageRecord::NamespaceAuthority(Box::new(captured.clone()))
                .encode()
                .unwrap(),
        )
        .await
        .unwrap();
    test.operation.admission = Some(NamespaceMutation {
        key: key.encode().unwrap(),
        before: Some(before),
        after,
    });
    test.persist().await;
    let reserved = test.operation.mapping(TableMappingState::Reserved);
    let name = name_key(reserved.catalog, reserved.namespace, &reserved.name).unwrap();
    test.staged
        .namespace
        .put(name.clone(), StorageRecord::TableMapping(reserved))
        .await;
    assert_eq!(
        NamespaceDropper::new(test.staged.namespace.store.clone())
            .drop_namespace(&test.staged.drop_request())
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    let store = &test.staged.namespace.store;
    let bytes = store.get(&key.encode().unwrap()).await.unwrap().unwrap().bytes;
    assert_eq!(
        StorageRecord::decode(&key, &bytes).unwrap(),
        StorageRecord::RetiredNamespace {
            catalog: parent.catalog,
            namespace: parent.namespace,
        }
    );
    assert!(store.get(&name.encode().unwrap()).await.unwrap().is_none());
    assert!(matches!(test.head(), Some(StorageRecord::RetiredTable(_))));
    (test, captured, bytes)
}
