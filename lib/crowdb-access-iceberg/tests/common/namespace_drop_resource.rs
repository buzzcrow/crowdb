// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{CasOutcome, CatalogStore, StoreError, StoredValue};
use crowdb_access_iceberg::key::{IcebergKey, OperationId};
use crowdb_access_iceberg::namespace::{
    ChildScan, NamespaceAuthority, NamespaceCreateRequest, NamespaceDropRequest, NamespaceIdentifier,
    NamespaceProperties, NamespaceRecoveryScan, NamespaceRecoveryStore, NamespaceStore,
};
use crowdb_access_iceberg::operation::RequestIdentity;
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use tokio::sync::Notify;

use crate::{common::TestStore, fixture::TestNamespace};

#[derive(Default)]
pub struct TestDropStore {
    pub inner: Arc<TestStore>,
    reads: AtomicUsize,
    cas: AtomicUsize,
    deletes: AtomicUsize,
    scans: AtomicUsize,
    mutations: AtomicUsize,
    pub fail_at: AtomicUsize,
    pub fail_before: AtomicBool,
    pub faulted: AtomicBool,
    pub post_fault: AtomicUsize,
    pub fault_hits: AtomicUsize,
    pub terminal_pause: AtomicBool,
    pub delete_pause: AtomicBool,
    pub entered: Notify,
    pub release: Notify,
}

impl TestDropStore {
    pub fn counts(&self) -> (usize, usize, usize, usize) {
        let [gets, cas, deletes, scans] = [&self.reads, &self.cas, &self.deletes, &self.scans]
            .map(|counter| counter.load(Ordering::SeqCst));
        (gets, cas, deletes, scans)
    }

    pub fn reset_counts(&self) {
        for counter in [
            &self.reads,
            &self.cas,
            &self.deletes,
            &self.scans,
            &self.mutations,
        ] {
            counter.store(0, Ordering::SeqCst);
        }
    }

    fn entered_call(&self) {
        if self.faulted.load(Ordering::SeqCst) {
            self.post_fault.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn fault(&self) -> StoreError {
        self.fault_hits.fetch_add(1, Ordering::SeqCst);
        self.faulted.store(true, Ordering::SeqCst);
        StoreError::Response
    }

    fn fail_mutation(&self) -> bool {
        self.mutations.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl CatalogStore for TestDropStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.entered_call();
        self.reads.fetch_add(1, Ordering::SeqCst);
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
        self.cas.fetch_add(1, Ordering::SeqCst);
        let fail = self.fail_mutation();
        if fail && self.fail_before.load(Ordering::SeqCst) {
            return Err(self.fault());
        }
        let terminal = IcebergKey::decode(key)
            .and_then(|key| StorageRecord::decode(&key, value))
            .is_ok_and(|record| match record {
                StorageRecord::RetiredNamespace { .. } => true,
                StorageRecord::NamespaceAuthority(authority) => {
                    authority.lifecycle == crowdb_access_iceberg::namespace::NamespaceLifecycle::Tombstone
                }
                _ => false,
            });
        if terminal && self.terminal_pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let result = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        if fail {
            return Err(self.fault());
        }
        Ok(result)
    }
}

#[async_trait]
impl NamespaceStore for TestDropStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.entered_call();
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_children(scan).await
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.entered_call();
        self.deletes.fetch_add(1, Ordering::SeqCst);
        let fail = self.fail_mutation();
        if fail && self.fail_before.load(Ordering::SeqCst) {
            return Err(self.fault());
        }
        if self.delete_pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let result = self.inner.delete_mapping(key, expected, identity).await?;
        if fail {
            return Err(self.fault());
        }
        Ok(result)
    }
}

#[async_trait]
impl NamespaceRecoveryStore for TestDropStore {
    async fn scan_namespace_mappings(
        &self,
        scan: NamespaceRecoveryScan,
    ) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_namespace_mappings(scan).await
    }
    async fn scan_namespace_operations(
        &self,
        scan: NamespaceRecoveryScan,
    ) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_namespace_operations(scan).await
    }
}

pub fn creation(fixture: &TestNamespace, names: &[&str]) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(names.iter().map(|name| (*name).into()).collect()).unwrap(),
        properties: NamespaceProperties::default(),
    }
}

pub async fn setup(
    depth: usize,
) -> (
    TestNamespace,
    Arc<TestDropStore>,
    NamespaceAuthority,
    NamespaceDropRequest,
) {
    let fixture = TestNamespace::new().await;
    let mut parent = None;
    let mut names = Vec::new();
    let mut selected = None;
    for index in 0..depth {
        names.push(format!("n{index}"));
        let authority = fixture.authority(parent, &names.iter().map(String::as_str).collect::<Vec<_>>());
        fixture.publish(&authority).await;
        parent = Some(authority.namespace);
        selected = Some(authority);
    }
    let selected = selected.unwrap();
    let request = NamespaceDropRequest {
        context: fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        identifier: selected.identifier.clone(),
    };
    let store = Arc::new(TestDropStore {
        inner: fixture.store.clone(),
        ..Default::default()
    });
    (fixture, store, selected, request)
}
