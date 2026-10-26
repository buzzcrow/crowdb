// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::{common::TestStore, fixture::TestTable, metadata};
use crowdb_access_iceberg::{
    catalog::{
        ActiveCatalogRecord, CasOutcome, CatalogContext, CatalogStore, RootState, StoreError, StoredValue,
    },
    key::{CatalogId, IcebergKey, OperationId, SystemScope},
    namespace::{ChildScan, NamespaceStore},
    operation::mutation_identity,
    record::StorageRecord,
    table::TableLoader,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub fn root_key() -> IcebergKey {
    IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: vec![],
    }
}
#[derive(Clone, Copy, Debug)]
pub enum TestReset {
    Fencing,
    Replacement,
    ReusedCatalog,
}
pub struct TestContextStore {
    pub inner: Arc<TestStore>,
    pub context: CatalogContext,
    pub reads: AtomicUsize,
    pub roots: AtomicUsize,
    pub fail_at: AtomicUsize,
    pub inject_after: usize,
    pub reset: TestReset,
}
impl TestContextStore {
    pub fn new(table: &TestTable, inject_after: usize, reset: TestReset) -> Arc<Self> {
        Arc::new(Self {
            inner: table.fixture.store.clone(),
            context: table.fixture.context,
            reads: AtomicUsize::new(0),
            roots: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(0),
            inject_after,
            reset,
        })
    }
    pub fn loader(self: &Arc<Self>, table: &TestTable) -> TableLoader {
        TableLoader::new(self.clone(), table.blocks.clone(), metadata::limits())
    }
    pub async fn replace_root(&self) {
        let key = root_key().encode().unwrap();
        let old = self.inner.get(&key).await.unwrap().unwrap();
        let context = match self.reset {
            TestReset::Fencing => self.context,
            TestReset::Replacement => self.context.replacement(CatalogId::random()).unwrap(),
            TestReset::ReusedCatalog => self
                .context
                .replacement(CatalogId::random())
                .unwrap()
                .replacement(self.context.catalog)
                .unwrap(),
        };
        let bytes = StorageRecord::Active(ActiveCatalogRecord {
            context,
            operation: OperationId::random(),
            state: if matches!(self.reset, TestReset::Fencing) {
                RootState::Fencing
            } else {
                RootState::Ready
            },
        })
        .encode()
        .unwrap();
        assert!(matches!(
            self.inner
                .compare_exchange(
                    &key,
                    Some(&old.bytes),
                    &bytes,
                    mutation_identity(&key, Some(&old.bytes), &bytes),
                )
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
}
#[async_trait::async_trait]
impl CatalogStore for TestContextStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let ordinal = self.reads.fetch_add(1, Ordering::Relaxed) + 1;
        if key == root_key().encode()?.as_slice() {
            self.roots.fetch_add(1, Ordering::Relaxed);
        }
        if ordinal == self.fail_at.load(Ordering::Relaxed) {
            return Err(StoreError::Response);
        }
        let value = self.inner.get(key).await?;
        if ordinal == self.inject_after {
            self.replace_root().await;
        }
        Ok(value)
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}
#[async_trait::async_trait]
impl NamespaceStore for TestContextStore {
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
