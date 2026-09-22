use std::collections::BTreeMap;
use std::sync::{atomic::AtomicU8, Arc};

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogStore};
use crowdb_access_iceberg::key::{NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    authority_key, name_key, NamespaceAuthority, NamespaceIdentifier, NamespaceJournal, NamespaceLifecycle,
    NamespaceMapping, NamespaceMappingState, NamespacePhase, NamespaceProperties, NamespacePropertyRequest,
    NamespaceRepository, PropertyChanges,
};
use crowdb_access_iceberg::operation::{mutation_identity, PayloadStore, RequestIdentity};
use crowdb_access_iceberg::record::StorageRecord;

use super::{common::now_ms, fault::TestFaultStore};

pub async fn prepare(store: Arc<dyn CatalogStore>, context: CatalogContext) -> NamespacePropertyRequest {
    let request = NamespacePropertyRequest {
        context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: now_ms(),
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["property-recovery".into()]).unwrap(),
        changes: PropertyChanges {
            removals: Vec::new(),
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
        properties: NamespaceProperties::default(),
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
    let fault = Arc::new(TestFaultStore {
        inner: store.clone(),
        mode: AtomicU8::new(3),
    });
    assert!(NamespaceRepository::new(fault)
        .update_properties(&request)
        .await
        .is_err());
    assert_eq!(
        NamespaceJournal::new(store)
            .load(context, request.identity.operation)
            .await
            .unwrap()
            .unwrap()
            .phase,
        NamespacePhase::Publishing
    );
    request
}

pub async fn verify(store: Arc<dyn CatalogStore>, request: &NamespacePropertyRequest) {
    let repository = NamespaceRepository::new(store.clone());
    let outcome = repository.update_properties(request).await.unwrap().unwrap();
    assert_eq!(outcome.status, 200);
    let body = PayloadStore::new(store).get(&outcome.body).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "removed": [], "updated": ["owner"], "missing": [],
        })
    );
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
