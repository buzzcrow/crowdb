use async_trait::async_trait;
use crowdb_access_dataset::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub struct TestSnapshotCleanupStall {
    pub inner: Arc<crate::access_store::TestAccessStore>,
    pub stall: AtomicBool,
    pub permanent: bool,
    pub target: Option<Vec<u8>>,
}

#[async_trait]
impl DatasetStore for TestSnapshotCleanupStall {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let target = self.target.as_ref().map_or(true, |target| key == target);
        if key.starts_with(b"DS\0S\0") && target {
            let stalled = if self.permanent {
                self.stall.load(Ordering::SeqCst)
            } else {
                self.stall.swap(false, Ordering::SeqCst)
            };
            if stalled {
                futures::future::pending::<()>().await;
            }
        }
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        scan: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.inner.scan_page(scan, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
}
