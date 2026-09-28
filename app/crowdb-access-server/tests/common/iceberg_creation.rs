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
    ChildScan, NamespaceCreateRequest, NamespaceCreator, NamespaceIdentifier, NamespaceJournal,
    NamespaceMappingState, NamespacePhase, NamespaceProperties, NamespaceRepository, NamespaceStore,
};
use crowdb_access_iceberg::operation::RequestIdentity;
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use super::common::now_ms;

pub struct TestCreateRecovery {
    request: NamespaceCreateRequest,
    namespace: NamespaceId,
}

pub async fn prepare(store: Arc<RoutedCatalogStore>, context: CatalogContext) -> TestCreateRecovery {
    let mut request = NamespaceCreateRequest {
        context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: now_ms(),
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["native-created".into()]).unwrap(),
        properties: NamespaceProperties::default(),
    };
    assert_eq!(
        NamespaceCreator::new(store.clone())
            .create(&request)
            .await
            .unwrap()
            .status,
        200
    );
    request.identity.operation = OperationId::random();
    request.identifier = NamespaceIdentifier::new(vec!["native-created".into(), "child".into()]).unwrap();
    let fault = Arc::new(TestCreateFaultStore {
        inner: store.clone(),
        armed: AtomicBool::new(true),
    });
    assert!(NamespaceCreator::new(fault).create(&request).await.is_err());
    let operation = NamespaceJournal::new(store)
        .load(context, request.identity.operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.phase, NamespacePhase::Publishing);
    TestCreateRecovery {
        request,
        namespace: operation.namespace,
    }
}

pub async fn verify(store: Arc<RoutedCatalogStore>, recovery: &TestCreateRecovery) {
    let creator = NamespaceCreator::new(store.clone());
    let outcome = creator.create(&recovery.request).await.unwrap();
    assert_eq!(outcome.status, 200);
    assert_eq!(creator.create(&recovery.request).await.unwrap(), outcome);
    let reader = NamespaceRepository::new(store);
    let selected = reader
        .load(recovery.request.context, &recovery.request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.namespace, recovery.namespace);
    assert_eq!(selected.pending_operation, None);
    let parent = reader
        .load(
            recovery.request.context,
            &recovery.request.identifier.parent().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.pending_operation, None);
    assert_eq!(parent.admission_fence, 1);
    assert_eq!(parent.property_revision, 1);
}

struct TestCreateFaultStore {
    inner: Arc<RoutedCatalogStore>,
    armed: AtomicBool,
}

#[async_trait]
impl CatalogStore for TestCreateFaultStore {
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
        let intercept = expected.is_some()
            && matches!(record, StorageRecord::NamespaceMapping(mapping) if mapping.state == NamespaceMappingState::Published);
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
impl NamespaceStore for TestCreateFaultStore {
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
