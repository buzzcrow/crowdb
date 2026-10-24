// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogStore, RoutedCatalogStore};
use crowdb_access_iceberg::key::{NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    NamespaceAction, NamespaceIdentifier, NamespaceJournal, NamespaceOperation, NamespacePhase,
};
use crowdb_access_iceberg::operation::{
    mutation_identity, PayloadStore, RequestIdentity, RetryRecord, RetryResult,
};

use super::{now_ms, TestNativeStorage};
use crowdb_access_iceberg::key::SystemScope;
use crowdb_access_iceberg::record::StorageRecord;
use std::sync::Arc;

pub async fn verify_recovery(stack: &mut TestNativeStorage, context: CatalogContext) {
    let store = stack.store().await;
    let property_request = super::property::prepare(store.clone(), context).await;
    let creation_request = super::creation::prepare(store.clone(), context).await;
    let drop_request = super::dropping::prepare(store.clone(), context).await;
    let identity = fresh_identity(store.as_ref()).await;
    let body = vec![23; 70 * 1024];
    let input = PayloadStore::new(store.clone())
        .put(context.catalog, identity.operation, b"{}")
        .await
        .unwrap();
    let mut operation = NamespaceOperation {
        context,
        identity,
        principal: "writer".into(),
        action: NamespaceAction::Create,
        identifier: NamespaceIdentifier::new(vec!["durable".into()]).unwrap(),
        namespace: NamespaceId::random(),
        parent: None,
        phase: NamespacePhase::Prepared,
        revision: 1,
        input,
        mutation: None,
        scan_after: Vec::new(),
        scan_generation: 0,
        outcome: None,
    };
    let journal = NamespaceJournal::new(store.clone());
    journal.begin(operation.clone()).await.unwrap();
    let previous = operation.clone();
    operation.phase = NamespacePhase::Reserved;
    operation.revision += 1;
    assert!(journal.advance(&previous, &operation).await.unwrap());
    let request = RetryRecord {
        identity,
        principal: "writer".into(),
        route: "POST /namespaces".into(),
        digest: operation.input.digest,
        context,
        retained_until_ms: now_ms() + 300_000,
        status: 200,
        body: Vec::new(),
    };
    let legacy = seed_legacy_result(store, &request, &body).await;
    stack.restart_catalog().await;
    let recovered_store = stack.store().await;
    super::property::verify(recovered_store.clone(), &property_request).await;
    super::creation::verify(recovered_store.clone(), &creation_request).await;
    super::dropping::verify(recovered_store.clone(), &drop_request).await;
    let recovered_journal = NamespaceJournal::new(recovered_store.clone());
    assert_eq!(
        recovered_journal
            .load(context, identity.operation)
            .await
            .unwrap()
            .unwrap(),
        operation
    );
    let key = request.result_key();
    let value = recovered_store
        .get(&key.encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(StorageRecord::decode(&key, &value.bytes).unwrap(), legacy);
    let StorageRecord::RetryResult(result) = legacy else {
        panic!("legacy paged response")
    };
    assert_eq!(result.binding, request);
    assert_eq!(
        PayloadStore::new(recovered_store)
            .get(&result.body)
            .await
            .unwrap(),
        body
    );
}

async fn fresh_identity(store: &dyn CatalogStore) -> RequestIdentity {
    for _ in 0..32 {
        let operation = OperationId::random();
        let key = crowdb_access_iceberg::operation::ledger_key(
            crowdb_access_iceberg::key::SystemScope::RetryBinding,
            operation,
        )
        .unwrap()
        .encode()
        .unwrap();
        if store.get(&key).await.unwrap().is_none() {
            return RequestIdentity {
                operation,
                issued_ms: now_ms(),
            };
        }
    }
    panic!("no free retry slot for fixture");
}

async fn seed_legacy_result(
    store: Arc<RoutedCatalogStore>,
    request: &RetryRecord,
    body: &[u8],
) -> StorageRecord {
    let response = PayloadStore::new(store.clone())
        .put(request.context.catalog, request.identity.operation, body)
        .await
        .unwrap();
    let legacy = StorageRecord::RetryResult(Box::new(RetryResult {
        binding: request.clone(),
        body: response,
    }));
    for (key, record) in [
        (
            crowdb_access_iceberg::operation::ledger_key(
                SystemScope::RetryBinding,
                request.identity.operation,
            )
            .unwrap(),
            StorageRecord::Retry(Box::new(request.clone())),
        ),
        (request.result_key(), legacy.clone()),
    ] {
        let key = key.encode().unwrap();
        let value = record.encode().unwrap();
        assert!(matches!(
            store
                .compare_exchange(&key, None, &value, mutation_identity(&key, None, &value))
                .await
                .unwrap(),
            crowdb_access_iceberg::catalog::CasOutcome::Applied(_)
        ));
    }
    legacy
}
