// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{
    CasOutcome, CatalogContext, CatalogStore, RoutedCatalogStore, StoreError, StoredValue,
};
use crowdb_access_iceberg::key::{IcebergKey, NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    authority_key, ChildScan, NamespaceCreateRequest, NamespaceCreator, NamespaceDropRequest,
    NamespaceDropper, NamespaceIdentifier, NamespaceJournal, NamespaceProperties, NamespaceRepository,
    NamespaceStore,
};
use crowdb_access_iceberg::operation::RequestIdentity;
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use super::now_ms;

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
    let namespace = recreate(store.clone(), &request).await;
    let fault = Arc::new(TestDropFaultStore {
        inner: store.clone(),
        namespace,
        armed: AtomicBool::new(true),
        calls_after_fault: AtomicUsize::new(0),
    });
    assert!(NamespaceDropper::new(fault.clone())
        .drop_namespace(&request)
        .await
        .is_err());
    assert!(!fault.armed.load(Ordering::SeqCst), "terminal fault must fire");
    assert_eq!(fault.calls_after_fault.load(Ordering::SeqCst), 0);
    assert!(NamespaceJournal::new(store.clone())
        .load(context, request.identity.operation)
        .await
        .unwrap()
        .is_none());
    assert_retired(store.as_ref(), context, namespace).await;
    assert!(!NamespaceRepository::new(store)
        .exists(context, &request.identifier)
        .await
        .unwrap());
    TestDropRecovery { request, namespace }
}

pub async fn verify(store: Arc<RoutedCatalogStore>, recovery: &TestDropRecovery) {
    let dropper = NamespaceDropper::new(store.clone());
    let outcome = dropper.drop_namespace(&recovery.request).await.unwrap().unwrap();
    assert_eq!(outcome.status, 204);
    assert!(dropper.drop_namespace(&recovery.request).await.unwrap().is_none());
    assert_retired(store.as_ref(), recovery.request.context, recovery.namespace).await;
    let replacement = recreate(store.clone(), &recovery.request).await;
    assert_ne!(replacement, recovery.namespace);
    assert_eq!(
        dropper
            .drop_namespace(&recovery.request)
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    assert_retired(store.as_ref(), recovery.request.context, replacement).await;
    verify_nonempty_replacement(store, &recovery.request).await;
}

async fn assert_retired(store: &RoutedCatalogStore, context: CatalogContext, namespace: NamespaceId) {
    let key = authority_key(context.catalog, namespace);
    let value = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    assert_eq!(
        StorageRecord::decode(&key, &value.bytes).unwrap(),
        StorageRecord::RetiredNamespace {
            catalog: context.catalog,
            namespace
        }
    );
}

async fn verify_nonempty_replacement(store: Arc<RoutedCatalogStore>, request: &NamespaceDropRequest) {
    let replacement = recreate(store.clone(), request).await;
    let mut components = request.identifier.components().to_vec();
    components.push("child".into());
    let child = NamespaceDropRequest {
        identifier: NamespaceIdentifier::new(components).unwrap(),
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: now_ms(),
        },
        ..request.clone()
    };
    recreate(store.clone(), &child).await;
    let dropper = NamespaceDropper::new(store.clone());
    assert_eq!(
        dropper.drop_namespace(request).await.unwrap().unwrap().status,
        409
    );
    let current = NamespaceRepository::new(store.clone())
        .load(request.context, &request.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.namespace, replacement);
    assert!(NamespaceRepository::new(store)
        .exists(child.context, &child.identifier)
        .await
        .unwrap());
    assert_eq!(dropper.drop_namespace(&child).await.unwrap().unwrap().status, 204);
    assert_eq!(
        dropper.drop_namespace(request).await.unwrap().unwrap().status,
        204
    );
}

async fn recreate(store: Arc<RoutedCatalogStore>, request: &NamespaceDropRequest) -> NamespaceId {
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
        NamespaceCreator::new(store.clone())
            .create(&creation)
            .await
            .unwrap()
            .status,
        200
    );
    NamespaceRepository::new(store)
        .load(request.context, &request.identifier)
        .await
        .unwrap()
        .unwrap()
        .namespace
}

struct TestDropFaultStore {
    inner: Arc<RoutedCatalogStore>,
    namespace: NamespaceId,
    armed: AtomicBool,
    calls_after_fault: AtomicUsize,
}

impl TestDropFaultStore {
    fn entered_call(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            self.calls_after_fault.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl CatalogStore for TestDropFaultStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.entered_call();
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.entered_call();
        let record = StorageRecord::decode(&IcebergKey::decode(key)?, value)?;
        let intercept = matches!(record, StorageRecord::RetiredNamespace { namespace, .. }
            if namespace == self.namespace)
            && expected.is_some();
        let result = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        if intercept && matches!(&result, CasOutcome::Applied(_)) && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(StoreError::Response);
        }
        Ok(result)
    }
}

#[async_trait]
impl NamespaceStore for TestDropFaultStore {
    async fn scan_children(&self, request: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.entered_call();
        self.inner.scan_children(request).await
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.entered_call();
        self.inner.delete_mapping(key, expected, identity).await
    }
}
