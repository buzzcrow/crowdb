// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    key::{CatalogScope, IcebergKey, StorageKey, SystemScope, TableStorageScope},
    namespace::{ChildScan, NamespaceStore},
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub struct TestDropReads {
    pub inner: Arc<crate::common::TestStore>,
    counts: [AtomicUsize; 6],
    pub deletes: AtomicUsize,
}

impl TestDropReads {
    pub fn new(inner: Arc<crate::common::TestStore>) -> Self {
        Self {
            inner,
            counts: std::array::from_fn(|_| AtomicUsize::new(0)),
            deletes: AtomicUsize::new(0),
        }
    }
    pub fn counts(&self) -> [usize; 6] {
        std::array::from_fn(|index| self.counts[index].load(Ordering::SeqCst))
    }
}

#[async_trait]
impl CatalogStore for TestDropReads {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let index = match StorageKey::decode(key)? {
            StorageKey::Legacy(IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                ..
            }) => 0,
            StorageKey::Legacy(IcebergKey::Catalog { scope, .. }) => match scope {
                CatalogScope::NamespaceName => 1,
                CatalogScope::NamespaceAuthority => 2,
                CatalogScope::TableName => 3,
                CatalogScope::TableHead => 4,
                CatalogScope::FileLocation | CatalogScope::File => 5,
                other => panic!("unexpected foreground GET: {other:?}"),
            },
            StorageKey::NativeTable(key) => match key.scope {
                TableStorageScope::Head => 4,
                TableStorageScope::File => 5,
                other => panic!("unexpected native GET: {other:?}"),
            },
            other @ StorageKey::Legacy(_) => panic!("unexpected foreground GET: {other:?}"),
        };
        self.counts[index].fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        bytes: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.compare_exchange(key, expected, bytes, identity).await
    }
}

#[async_trait]
impl NamespaceStore for TestDropReads {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(scan).await
    }
    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_mapping(key, expected, identity).await
    }
}
