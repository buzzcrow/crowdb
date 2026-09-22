use crowdb_access_iceberg::catalog::{CatalogContext, CatalogStore};
use crowdb_access_iceberg::key::{NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    NamespaceAction, NamespaceIdentifier, NamespaceJournal, NamespaceOperation, NamespacePhase,
};
use crowdb_access_iceberg::operation::{
    PayloadStore, RequestIdentity, RetryAdmission, RetryLedger, RetryRecord,
};

use super::common::{now_ms, TestIcebergStack};

pub async fn verify_recovery(stack: &mut TestIcebergStack, context: CatalogContext) {
    let store = stack.store().await;
    let identity = fresh_identity(store.as_ref()).await;
    let body = vec![23; 70 * 1024];
    let input = PayloadStore::new(store.clone())
        .put(context.catalog, identity.operation, &body)
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
        retained_until_ms: 0,
        status: 0,
        body: Vec::new(),
    };
    let ledger = RetryLedger::new(store);
    ledger.begin(request.clone(), now_ms()).await.unwrap();
    ledger
        .finish(request.clone(), 200, body.clone(), now_ms())
        .await
        .unwrap();
    stack.chunk_kv.restart().await;
    let recovered_store = stack.store().await;
    let recovered_journal = NamespaceJournal::new(recovered_store.clone());
    assert_eq!(
        recovered_journal
            .load(context, identity.operation)
            .await
            .unwrap()
            .unwrap(),
        operation
    );
    let RetryAdmission::Replay(result) = RetryLedger::new(recovered_store)
        .begin(request, now_ms())
        .await
        .unwrap()
    else {
        panic!("large response replay")
    };
    assert_eq!(result.body, body);
    assert_eq!(result.principal, "writer");
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
