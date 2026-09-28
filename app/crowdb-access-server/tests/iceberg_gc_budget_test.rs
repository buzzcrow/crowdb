use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError};
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    gc::{GcScan, GcStore, GcSystemScan},
    record::MAX_RECORD_BYTES,
};
use crowdb_access_server::iceberg::{BudgetedGcBlocks, BudgetedGcStore, GcIoBudget};
use crowdb_chunk_client::ReclaimOutcome;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use crowdb_protocol::common::ChunkId;

#[derive(Default)]
struct TestBlocks {
    reads: AtomicUsize,
    deletes: AtomicUsize,
}

#[derive(Default)]
struct TestStore {
    gets: AtomicUsize,
}

#[async_trait]
impl CatalogStore for TestStore {
    async fn get(&self, _key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    async fn compare_exchange(
        &self,
        _key: &[u8],
        _expected: Option<&[u8]>,
        _value: &[u8],
        _identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        Ok(CasOutcome::Conflict(None))
    }
}

#[async_trait]
impl GcStore for TestStore {
    async fn scan_gc(&self, _request: GcScan) -> Result<MultiScanPage, StoreError> {
        Ok(MultiScanPage {
            items: Vec::new(),
            continuation: None,
            terminal_failure: None,
        })
    }

    async fn scan_gc_system(&self, _request: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        Ok(MultiScanPage {
            items: Vec::new(),
            continuation: None,
            terminal_failure: None,
        })
    }

    async fn delete_gc_record(
        &self,
        _key: &[u8],
        _expected: &[u8],
        _identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        Ok(CasOutcome::Conflict(None))
    }
}

#[async_trait]
impl FileBlockStore for TestBlocks {
    async fn put(&self, _owner: FileIdentity, _height: u8, _bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        Err(FileIoError::Bounds)
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(vec![7; usize::try_from(root.logical_length).unwrap()])
    }

    async fn reclaim(&self, _root: &ChunkRoot) -> Result<ReclaimOutcome, FileIoError> {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        Ok(ReclaimOutcome::Reclaimed)
    }
}

#[tokio::test]
async fn chunk_io_budget_rejects_work_before_dispatch_and_resets_per_step() {
    let budget = Arc::new(GcIoBudget::for_tests(1024, 8, 24, 2));
    let inner = Arc::new(TestBlocks::default());
    let blocks = BudgetedGcBlocks::new(inner.clone(), budget.clone());
    let root = ChunkRoot {
        chunk: ChunkId { high: 1, low: 1 },
        offset: 0,
        physical_length: 16,
        logical_offset: 0,
        logical_length: 16,
        height: 0,
        digest: [7; 32],
    };
    assert_eq!(blocks.read(&root).await.unwrap().len(), 16);
    assert!(matches!(blocks.read(&root).await, Err(FileIoError::Bounds)));
    assert_eq!(inner.reads.load(Ordering::Relaxed), 1);
    assert!(matches!(blocks.reclaim(&root).await, Err(FileIoError::Bounds)));
    assert_eq!(inner.deletes.load(Ordering::Relaxed), 0);
    budget.reset();
    assert_eq!(blocks.reclaim(&root).await.unwrap(), ReclaimOutcome::Reclaimed);
    assert_eq!(inner.deletes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn kv_budget_rejects_work_before_dispatch_and_resets_per_step() {
    let budget = Arc::new(GcIoBudget::for_tests(
        u64::try_from(MAX_RECORD_BYTES).unwrap() + 1,
        1,
        24,
        2,
    ));
    let inner = Arc::new(TestStore::default());
    let store = BudgetedGcStore::new(inner.clone(), budget.clone());
    assert!(store.get(b"x").await.unwrap().is_none());
    assert!(matches!(store.get(b"x").await, Err(StoreError::Budget)));
    assert_eq!(inner.gets.load(Ordering::Relaxed), 1);
    assert!(inner.get(b"x").await.unwrap().is_none());
    assert_eq!(inner.gets.load(Ordering::Relaxed), 2);
    budget.reset();
    assert!(store.get(b"x").await.unwrap().is_none());
    assert_eq!(inner.gets.load(Ordering::Relaxed), 3);
}
