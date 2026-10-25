// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::TestStore;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    key::{IcebergKey, NamespaceId, OperationId},
    namespace::{ChildScan, NamespaceDropRequest, NamespaceStore},
    operation::RequestIdentity,
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

#[path = "legacy_retired_admission/namespace.rs"]
mod namespace;
#[path = "legacy_retired_admission/table.rs"]
mod table;
pub use namespace::namespace_case;
pub use table::table_case;

#[derive(Clone, Copy)]
pub enum PauseAt {
    ParentGet,
    ParentCas,
}

pub struct TestLegacyAdmissionStore {
    inner: Arc<TestStore>,
    parent_key: Vec<u8>,
    operation: OperationId,
    pause_at: PauseAt,
    armed: AtomicBool,
    pub pauses: AtomicUsize,
    pub entered: Notify,
    pub release: Notify,
}

impl TestLegacyAdmissionStore {
    pub fn new(
        inner: Arc<TestStore>,
        parent_key: Vec<u8>,
        operation: OperationId,
        pause_at: PauseAt,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            parent_key,
            operation,
            pause_at,
            armed: AtomicBool::new(true),
            pauses: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        })
    }

    async fn pause(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.pauses.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

#[async_trait]
impl CatalogStore for TestLegacyAdmissionStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        if matches!(self.pause_at, PauseAt::ParentGet) && key == self.parent_key {
            self.pause().await;
        }
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        if matches!(self.pause_at, PauseAt::ParentCas)
            && key == self.parent_key
            && expected.is_some()
            && matches!(IcebergKey::decode(key)
                .and_then(|key| StorageRecord::decode(&key, value)),
                Ok(StorageRecord::NamespaceAuthority(authority))
                    if authority.pending_operation == Some(self.operation))
        {
            self.pause().await;
        }
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl NamespaceStore for TestLegacyAdmissionStore {
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

pub fn identity() -> RequestIdentity {
    RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    }
}

pub fn drop_request(
    fixture: &crate::fixture::TestNamespace,
    parent: &crowdb_access_iceberg::namespace::NamespaceAuthority,
) -> NamespaceDropRequest {
    NamespaceDropRequest {
        context: fixture.context,
        identity: identity(),
        principal: "writer".into(),
        identifier: parent.identifier.clone(),
    }
}

pub fn fixed_namespace(value: u8) -> NamespaceId {
    NamespaceId::from_bytes(&[value; 16]).unwrap()
}
