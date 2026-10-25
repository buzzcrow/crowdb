// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{fixed_namespace, identity, PauseAt};
use crate::fixture::TestNamespace;
use crowdb_access_iceberg::{
    namespace::{
        authority_key, name_key, NamespaceAction, NamespaceAuthority, NamespaceIdentifier, NamespaceJournal,
        NamespaceMapping, NamespaceMappingState, NamespaceMutation, NamespaceOperation, NamespacePhase,
    },
    operation::PayloadStore,
    record::StorageRecord,
};

pub async fn namespace_case(pause_at: PauseAt) -> (TestNamespace, NamespaceAuthority, NamespaceOperation) {
    let fixture = TestNamespace::new().await;
    let mut parent = fixture.authority(None, &["parent"]);
    parent.namespace = fixed_namespace(2);
    fixture.publish(&parent).await;
    let identity = identity();
    let payloads = PayloadStore::new(fixture.store.clone());
    let input = payloads
        .put(fixture.context.catalog, identity.operation, b"{}")
        .await
        .unwrap();
    let journal = NamespaceJournal::new(fixture.store.clone());
    let prepared = NamespaceOperation {
        context: fixture.context,
        identity,
        principal: "legacy-writer".into(),
        action: NamespaceAction::Create,
        identifier: NamespaceIdentifier::new(vec!["parent".into(), "child".into()]).unwrap(),
        namespace: fixed_namespace(3),
        parent: Some(parent.namespace),
        phase: NamespacePhase::Prepared,
        revision: 1,
        input,
        mutation: None,
        scan_after: Vec::new(),
        scan_generation: 0,
        outcome: None,
    };
    journal.begin(prepared.clone()).await.unwrap();
    let mapping = NamespaceMapping {
        catalog: fixture.context.catalog,
        parent: prepared.parent,
        name: "child".into(),
        namespace: prepared.namespace,
        name_epoch: 1,
        operation: identity.operation,
        state: NamespaceMappingState::Reserved,
    };
    fixture
        .put(
            name_key(mapping.catalog, mapping.parent, &mapping.name).unwrap(),
            StorageRecord::NamespaceMapping(mapping),
        )
        .await;
    let mut operation = prepared.clone();
    operation.phase = NamespacePhase::Reserved;
    operation.revision += 1;
    assert!(journal.advance(&prepared, &operation).await.unwrap());
    if matches!(pause_at, PauseAt::ParentCas) {
        let key = authority_key(parent.catalog, parent.namespace);
        let before = StorageRecord::NamespaceAuthority(Box::new(parent.clone()))
            .encode()
            .unwrap();
        let mut occupied = parent.clone();
        occupied.mutation_revision += 1;
        occupied.pending_operation = Some(identity.operation);
        let after = StorageRecord::NamespaceAuthority(Box::new(occupied))
            .encode()
            .unwrap();
        let mut admitting = operation.clone();
        admitting.phase = NamespacePhase::Admitting;
        admitting.revision += 1;
        admitting.mutation = Some(NamespaceMutation {
            key: key.encode().unwrap(),
            before: Some(
                payloads
                    .put(parent.catalog, identity.operation, &before)
                    .await
                    .unwrap(),
            ),
            after: payloads
                .put(parent.catalog, identity.operation, &after)
                .await
                .unwrap(),
        });
        assert!(journal.advance(&operation, &admitting).await.unwrap());
        operation = admitting;
    }
    (fixture, parent, operation)
}
