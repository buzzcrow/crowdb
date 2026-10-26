use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    gc::{GcScan, GcStore, GcSystemScan},
    key::StorageKey,
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub struct TestRetirementConflict {
    pub inner: Arc<crate::common::TestStore>,
    pub key: StorageKey,
    pub expected: ArcSwap<Vec<Vec<u8>>>,
    pub replacement: Vec<u8>,
    first: AtomicBool,
    reads: AtomicUsize,
}

impl TestRetirementConflict {
    pub fn new(inner: Arc<crate::common::TestStore>, key: StorageKey, replacement: Vec<u8>) -> Self {
        Self {
            inner,
            key,
            replacement,
            expected: ArcSwap::from_pointee(Vec::new()),
            first: AtomicBool::new(true),
            reads: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl CatalogStore for TestRetirementConflict {
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
        if key == self.key.encode().unwrap()
            && matches!(
                StorageRecord::decode(&self.key, value),
                Ok(StorageRecord::RetiredTable(_))
            )
        {
            let before = expected.unwrap().to_vec();
            self.expected.rcu(|values| {
                let mut next = (**values).clone();
                next.push(before.clone());
                next
            });
            if self.first.swap(false, Ordering::SeqCst) {
                self.reads
                    .store(self.inner.reads.load(Ordering::SeqCst), Ordering::SeqCst);
                assert!(matches!(
                    self.inner
                        .compare_exchange(
                            key,
                            expected,
                            &self.replacement,
                            mutation_identity(key, expected, &self.replacement)
                        )
                        .await?,
                    CasOutcome::Applied(_)
                ));
            } else {
                assert_eq!(
                    self.inner.reads.load(Ordering::SeqCst),
                    self.reads.load(Ordering::SeqCst),
                    "CAS observation must replace the old base without another GET"
                );
            }
        }
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl GcStore for TestRetirementConflict {
    async fn scan_gc(&self, scan: GcScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc(scan).await
    }
    async fn scan_gc_system(&self, scan: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_gc_system(scan).await
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
