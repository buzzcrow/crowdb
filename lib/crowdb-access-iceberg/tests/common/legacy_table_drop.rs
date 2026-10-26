// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::TestStore;
use crowdb_access_iceberg::{
    catalog::CatalogStore,
    operation::{mutation_identity, PayloadStore},
    record::StorageRecord,
    table::{
        name_key, storage_head_key, TableHead, TableLifecycle, TableLifecycleAction, TableLifecycleOperation,
        TableLifecyclePhase, TableLifecycleRequest,
    },
};
use std::sync::Arc;

pub async fn install(
    store: Arc<TestStore>,
    request: &TableLifecycleRequest,
    before: TableHead,
    phase: TableLifecyclePhase,
) -> TableLifecycleOperation {
    let TableLifecycleAction::Drop { purge_requested } = request.action else {
        panic!("drop");
    };
    let key = name_key(before.catalog, before.namespace, &before.name).unwrap();
    let source = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::TableMapping(source) = StorageRecord::decode(&key, &source.bytes).unwrap() else {
        panic!("mapping");
    };
    let mut candidate = before.clone();
    candidate.lifecycle = TableLifecycle::Tombstone;
    candidate.operation_fence += 1;
    candidate.pending_operation = Some(request.identity.operation);
    let input = serde_json::to_vec(&serde_json::json!({"namespace":request.namespace.components(),
        "name":request.name,"action":{"drop":purge_requested}}))
    .unwrap();
    let input = PayloadStore::new(store.clone())
        .put(request.context.catalog, request.identity.operation, &input)
        .await
        .unwrap();
    let operation = TableLifecycleOperation {
        context: request.context,
        identity: request.identity,
        principal: request.principal.clone(),
        revision: if phase == TableLifecyclePhase::Published {
            3
        } else {
            1
        },
        phase,
        input,
        source,
        before,
        candidate,
        destination_namespace: None,
        purge_requested,
        admission: None,
        outcome: None,
    };
    let key = operation.key().encode().unwrap();
    let bytes = StorageRecord::TableLifecycleOperation(Box::new(operation.clone()))
        .encode()
        .unwrap();
    store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    if phase == TableLifecyclePhase::Published {
        let key = storage_head_key(operation.before.storage_location())
            .encode()
            .unwrap();
        let before = StorageRecord::TableHead(Box::new(operation.before.clone()))
            .encode()
            .unwrap();
        let bytes = StorageRecord::TableHead(Box::new(operation.candidate.clone()))
            .encode()
            .unwrap();
        store
            .compare_exchange(
                &key,
                Some(&before),
                &bytes,
                mutation_identity(&key, Some(&before), &bytes),
            )
            .await
            .unwrap();
    }
    operation
}
