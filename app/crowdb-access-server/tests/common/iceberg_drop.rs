use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{
    CasOutcome, CatalogContext, CatalogStore, RoutedCatalogStore, StoreError, StoredValue,
};
use crowdb_access_iceberg::key::{IcebergKey, NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    authority_key, ChildScan, NamespaceCreateRequest, NamespaceCreator, NamespaceDropRequest,
    NamespaceDropper, NamespaceIdentifier, NamespaceJournal, NamespaceLifecycle, NamespacePhase,
    NamespaceProperties, NamespaceRepository, NamespaceStore,
};
use crowdb_access_iceberg::operation::RequestIdentity;
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use super::common::now_ms;

pub struct TestDropRecovery {
    request: NamespaceDropRequest,
    namespace: NamespaceId,
}

pub async fn prepare(store: Arc<RoutedCatalogStore>, context: CatalogContext) -> TestDropRecovery {
    let request = NamespaceDropRequest {
        context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: now_ms(),
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["drop-recovery".into()]).unwrap(),
    };
    recreate(store.clone(), &request).await;
    let fault = Arc::new(TestDropFaultStore {
        inner: store.clone(),
        armed: AtomicBool::new(true),
    });
    assert!(NamespaceDropper::new(fault)
        .drop_namespace(&request)
        .await
        .is_err());
    let operation = NamespaceJournal::new(store.clone())
        .load(context, request.identity.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.phase, NamespacePhase::Tombstoning);
    assert!(!NamespaceRepository::new(store)
        .exists(context, &request.identifier)
        .await
        .unwrap());
    TestDropRecovery {
        request,
        namespace: operation.namespace,
    }
}

pub async fn verify(store: Arc<RoutedCatalogStore>, recovery: &TestDropRecovery) {
    let dropper = NamespaceDropper::new(store.clone());
    let outcome = dropper.drop_namespace(&recovery.request).await.unwrap().unwrap();
    assert_eq!(outcome.status, 204);
    assert_eq!(
        dropper.drop_namespace(&recovery.request).await.unwrap(),
        Some(outcome.clone())
    );
    let key = authority_key(recovery.request.context.catalog, recovery.namespace);
    let value = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &value.bytes).unwrap()
    else {
        panic!("expected retained namespace tombstone");
    };
    assert_eq!(authority.lifecycle, NamespaceLifecycle::Tombstone);
    recreate(store.clone(), &recovery.request).await;
    let reader = NamespaceRepository::new(store);
    let replacement = reader
        .load(recovery.request.context, &recovery.request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(replacement.namespace, recovery.namespace);
    assert_eq!(
        dropper.drop_namespace(&recovery.request).await.unwrap(),
        Some(outcome)
    );
    assert_eq!(
        reader
            .load(recovery.request.context, &recovery.request.identifier)
            .await
            .unwrap(),
        Some(replacement)
    );
}

async fn recreate(store: Arc<RoutedCatalogStore>, request: &NamespaceDropRequest) {
    let creation = NamespaceCreateRequest {
        context: request.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: now_ms(),
        },
        principal: request.principal.clone(),
        identifier: request.identifier.clone(),
        properties: NamespaceProperties::default(),
    };
    assert_eq!(
        NamespaceCreator::new(store)
            .create(&creation)
            .await
            .unwrap()
            .status,
        200
    );
}

struct TestDropFaultStore {
    inner: Arc<RoutedCatalogStore>,
    armed: AtomicBool,
}

#[async_trait]
impl CatalogStore for TestDropFaultStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let record = StorageRecord::decode(&IcebergKey::decode(key)?, value)?;
        let intercept = matches!(record, StorageRecord::NamespaceAuthority(authority)
            if authority.lifecycle == NamespaceLifecycle::Tombstone);
        let result = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        if intercept && self.armed.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Response);
        }
        Ok(result)
    }
}

#[async_trait]
impl NamespaceStore for TestDropFaultStore {
    async fn scan_children(&self, request: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(request).await
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete_mapping(key, expected, identity).await
    }
}
