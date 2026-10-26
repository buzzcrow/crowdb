// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{fixed_namespace, identity, PauseAt};
use crate::{blocks::TestBlocks, fixture::TestNamespace};
use crowdb_access_iceberg::{
    catalog::CatalogStore,
    commit::{TableCreationRequest, TableCreator},
    namespace::{authority_key, NamespaceAuthority, NamespaceMutation},
    operation::PayloadStore,
    record::StorageRecord,
    table::{
        name_key, TableLifecycleOperation, TableLifecyclePhase, TableMapping, TableMappingState,
        TableRepository,
    },
};
use serde_json::json;
use std::sync::Arc;

pub async fn table_case(pause_at: PauseAt) -> (TestNamespace, NamespaceAuthority, TableLifecycleOperation) {
    let fixture = TestNamespace::new().await;
    let mut source = fixture.authority(None, &["source"]);
    source.namespace = fixed_namespace(1);
    fixture.publish(&source).await;
    let mut target = fixture.authority(None, &["destination"]);
    target.namespace = fixed_namespace(2);
    fixture.publish(&target).await;
    TableCreator::new(fixture.store.clone(), Arc::new(TestBlocks::default()))
        .create(&TableCreationRequest {
            context: fixture.context,
            identity: identity(),
            principal: "writer".into(),
            namespace: source.identifier.clone(),
            timestamp_ms: 1000,
            body: serde_json::to_vec(&json!({"name":"events","schema":{"type":"struct",
                "schema-id":0,"fields":[{"id":91,"name":"id","type":"long","required":true}]}}))
            .unwrap(),
        })
        .await
        .unwrap();
    let before = TableRepository::new(fixture.store.clone())
        .select(fixture.context, source.namespace, "events")
        .await
        .unwrap()
        .unwrap()
        .head;
    let key = name_key(before.catalog, before.namespace, &before.name).unwrap();
    let value = fixture.store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::TableMapping(source) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
        panic!()
    };
    let identity = identity();
    let payloads = PayloadStore::new(fixture.store.clone());
    let input = serde_json::to_vec(&json!({"namespace":["source"], "name":"events",
        "action":{"rename":{"namespace":["destination"],"name":"renamed"}}}))
    .unwrap();
    let input = payloads
        .put(fixture.context.catalog, identity.operation, &input)
        .await
        .unwrap();
    let mut candidate = before.clone();
    candidate.namespace = target.namespace;
    candidate.name = "renamed".into();
    candidate.name_epoch += 1;
    candidate.operation_fence += 1;
    candidate.pending_operation = Some(identity.operation);
    let mapping = TableMapping {
        storage_namespace: source.storage_namespace,
        catalog: candidate.catalog,
        namespace: target.namespace,
        name: candidate.name.clone(),
        table: candidate.table,
        name_epoch: candidate.name_epoch,
        operation: identity.operation,
        state: TableMappingState::Reserved,
    };
    fixture
        .put(
            name_key(mapping.catalog, mapping.namespace, &mapping.name).unwrap(),
            StorageRecord::TableMapping(mapping),
        )
        .await;
    let mut operation = TableLifecycleOperation {
        context: fixture.context,
        identity,
        principal: "legacy-writer".into(),
        revision: 2,
        phase: TableLifecyclePhase::Reserved,
        input,
        source,
        before,
        candidate,
        destination_namespace: Some(target.identifier.clone()),
        purge_requested: false,
        admission: None,
        outcome: None,
    };
    if matches!(pause_at, PauseAt::ParentCas) {
        capture_admission(&payloads, &target, &mut operation).await;
    }
    fixture
        .put(
            operation.key(),
            StorageRecord::TableLifecycleOperation(Box::new(operation.clone())),
        )
        .await;
    (fixture, target, operation)
}

async fn capture_admission(
    payloads: &PayloadStore,
    target: &NamespaceAuthority,
    operation: &mut TableLifecycleOperation,
) {
    let key = authority_key(target.catalog, target.namespace);
    let before = StorageRecord::NamespaceAuthority(Box::new(target.clone()))
        .encode()
        .unwrap();
    let mut occupied = target.clone();
    occupied.mutation_revision += 1;
    occupied.pending_operation = Some(operation.identity.operation);
    let after = StorageRecord::NamespaceAuthority(Box::new(occupied))
        .encode()
        .unwrap();
    operation.phase = TableLifecyclePhase::Admitting;
    operation.revision += 1;
    operation.admission = Some(NamespaceMutation {
        key: key.encode().unwrap(),
        before: Some(
            payloads
                .put(target.catalog, operation.identity.operation, &before)
                .await
                .unwrap(),
        ),
        after: payloads
            .put(target.catalog, operation.identity.operation, &after)
            .await
            .unwrap(),
    });
}
