use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, RoutedCatalogStore, StoreError, StoredValue},
    gc::{GcScan, GcStore, GcSystemScan},
    key::{CatalogScope, IcebergKey},
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub struct TestGcWorkspace {
    inner: Arc<RoutedCatalogStore>,
    denied: AtomicBool,
}

impl TestGcWorkspace {
    pub fn new(inner: Arc<RoutedCatalogStore>) -> Self {
        Self {
            inner,
            denied: AtomicBool::new(false),
        }
    }

    pub fn deny(&self, denied: bool) {
        self.denied.store(denied, Ordering::SeqCst);
    }
}

#[async_trait]
impl CatalogStore for TestGcWorkspace {
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
        if self.denied.load(Ordering::SeqCst)
            && matches!(
                IcebergKey::decode(key),
                Ok(IcebergKey::Catalog {
                    scope: CatalogScope::GcClaim | CatalogScope::GcCandidate,
                    ..
                })
            )
        {
            return Err(StoreError::Budget);
        }
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl GcStore for TestGcWorkspace {
    async fn scan_gc(&self, request: GcScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc(request).await
    }

    async fn scan_gc_system(&self, request: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc_system(request).await
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete_gc_record(key, expected, identity).await
    }
}
