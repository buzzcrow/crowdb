use std::sync::Arc;
use std::time::Duration;

use crowdb_access_iceberg::catalog::{CatalogContext, RoutedCatalogStore};
use crowdb_access_iceberg::key::{NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    NamespaceAction, NamespaceIdentifier, NamespaceJournal, NamespaceOperation, NamespacePhase,
    NamespaceRepository,
};
use crowdb_access_iceberg::operation::{PayloadStore, RequestIdentity};

use super::common::now_ms;

pub async fn verify(store: Arc<RoutedCatalogStore>, context: CatalogContext) {
    let identity = RequestIdentity {
        operation: OperationId::random(),
        issued_ms: now_ms(),
    };
    let input = PayloadStore::new(store.clone())
        .put(context.catalog, identity.operation, b"{}")
        .await
        .unwrap();
    let operation = NamespaceOperation {
        context,
        identity,
        principal: "writer".into(),
        action: NamespaceAction::Create,
        identifier: NamespaceIdentifier::new(vec!["background-recovered".into()]).unwrap(),
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
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = journal.load(context, identity.operation).await.unwrap().unwrap();
            if current.phase == NamespacePhase::Complete {
                assert_eq!(current.outcome.unwrap().status, 200);
                let authority = NamespaceRepository::new(store.clone())
                    .load(context, &operation.identifier)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(authority.namespace, operation.namespace);
                if authority.pending_operation.is_none() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}
