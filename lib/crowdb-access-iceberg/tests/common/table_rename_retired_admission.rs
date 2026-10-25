// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    commit::{TableCreationRequest, TableCreator},
    key::{IcebergKey, NamespaceId, OperationId},
    namespace::{ChildScan, NamespaceAuthority, NamespaceDropRequest, NamespaceStore},
    operation::RequestIdentity,
    record::StorageRecord,
    table::{TableLifecycleAction, TableLifecycleRequest},
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use serde_json::json;
use tokio::sync::Notify;

use crate::{blocks::TestBlocks, common::TestStore, fixture::TestNamespace};

pub struct TestRetiredAdmission {
    pub fixture: TestNamespace,
    pub source: NamespaceAuthority,
    pub target: NamespaceAuthority,
    pub request: TableLifecycleRequest,
    pub store: Arc<TestPausedAdmissionStore>,
}

impl TestRetiredAdmission {
    pub async fn new() -> Self {
        let fixture = TestNamespace::new().await;
        let mut source = fixture.authority(None, &["source"]);
        source.namespace = NamespaceId::from_bytes(&[1; 16]).unwrap();
        fixture.publish(&source).await;
        let mut target = fixture.authority(None, &["destination"]);
        target.namespace = NamespaceId::from_bytes(&[2; 16]).unwrap();
        fixture.publish(&target).await;
        assert!(source.namespace.as_bytes() < target.namespace.as_bytes());
        let create = TableCreationRequest {
            context: fixture.context,
            identity: identity(),
            principal: "writer".into(),
            namespace: source.identifier.clone(),
            timestamp_ms: 1000,
            body: serde_json::to_vec(&json!({"name":"events","schema":{"type":"struct",
                "schema-id":0,"fields":[{"id":91,"name":"id","type":"long","required":true}]}}))
            .unwrap(),
        };
        TableCreator::new(fixture.store.clone(), Arc::new(TestBlocks::default()))
            .create(&create)
            .await
            .unwrap();
        let request = TableLifecycleRequest {
            context: fixture.context,
            identity: identity(),
            principal: "writer".into(),
            namespace: source.identifier.clone(),
            name: "events".into(),
            action: TableLifecycleAction::Rename {
                namespace: target.identifier.clone(),
                name: "renamed".into(),
            },
        };
        let store = Arc::new(TestPausedAdmissionStore {
            inner: fixture.store.clone(),
            target: target.namespace,
            armed: AtomicBool::new(true),
            pauses: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        Self {
            fixture,
            source,
            target,
            request,
            store,
        }
    }

    pub fn drop_target(&self) -> NamespaceDropRequest {
        NamespaceDropRequest {
            context: self.fixture.context,
            identity: identity(),
            principal: "writer".into(),
            identifier: self.target.identifier.clone(),
        }
    }
}

fn identity() -> RequestIdentity {
    RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    }
}

pub struct TestPausedAdmissionStore {
    inner: Arc<TestStore>,
    target: NamespaceId,
    armed: AtomicBool,
    pub pauses: AtomicUsize,
    pub entered: Notify,
    pub release: Notify,
}

#[async_trait]
impl CatalogStore for TestPausedAdmissionStore {
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
        let admission = matches!(IcebergKey::decode(key)
            .and_then(|key| StorageRecord::decode(&key, value)),
            Ok(StorageRecord::NamespaceAuthority(authority))
                if authority.namespace == self.target && authority.rename.is_some());
        if expected.is_some() && admission && self.armed.swap(false, Ordering::SeqCst) {
            self.pauses.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl NamespaceStore for TestPausedAdmissionStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(scan).await
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
