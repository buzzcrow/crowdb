// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    key::IcebergKey,
    namespace::{ChildScan, NamespaceMappingState, NamespaceStore},
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub struct TestDefaultUnknownStore {
    pub inner: Arc<crate::common::TestStore>,
    pub after_apply: bool,
    pub failed: AtomicBool,
    pub reads_after_unknown: AtomicUsize,
    pub writes_after_unknown: AtomicUsize,
}

#[async_trait]
impl CatalogStore for TestDefaultUnknownStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        if self.failed.load(Ordering::SeqCst) {
            self.reads_after_unknown.fetch_add(1, Ordering::SeqCst);
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
        if self.failed.load(Ordering::SeqCst) {
            self.writes_after_unknown.fetch_add(1, Ordering::SeqCst);
        } else if matches!(StorageRecord::decode(&IcebergKey::decode(key)?, value)?, StorageRecord::NamespaceMapping(mapping) if mapping.name == "default" && mapping.state == NamespaceMappingState::Published)
        {
            if self.after_apply {
                self.inner
                    .compare_exchange(key, expected, value, identity)
                    .await?;
            }
            self.failed.store(true, Ordering::SeqCst);
            return Err(StoreError::Response);
        }
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl NamespaceStore for TestDefaultUnknownStore {
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
