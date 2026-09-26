use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError},
    gc::{GcScan, GcStore, GcSystemScan},
    key::{CatalogScope, IcebergKey},
    record::MAX_RECORD_BYTES,
};
use crowdb_chunk_client::ReclaimOutcome;
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

use super::GcRuntimeConfig;

pub struct GcIoBudget {
    kv_bytes: AtomicU64,
    kv_requests: AtomicU32,
    chunk_bytes: AtomicU64,
    chunk_requests: AtomicU32,
    recovery_bytes: AtomicU64,
    recovery_requests: AtomicU32,
    max_kv_bytes: u64,
    max_kv_requests: u32,
    max_chunk_bytes: u64,
    max_chunk_requests: u32,
}

impl GcIoBudget {
    pub(super) fn new(config: &GcRuntimeConfig) -> Self {
        Self {
            kv_bytes: AtomicU64::new(0),
            kv_requests: AtomicU32::new(0),
            chunk_bytes: AtomicU64::new(0),
            chunk_requests: AtomicU32::new(0),
            recovery_bytes: AtomicU64::new(0),
            recovery_requests: AtomicU32::new(0),
            max_kv_bytes: config.kv_bytes,
            max_kv_requests: config.kv_requests,
            max_chunk_bytes: config.chunk_bytes,
            max_chunk_requests: config.chunk_requests,
        }
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn for_tests(kv_bytes: u64, kv_requests: u32, chunk_bytes: u64, chunk_requests: u32) -> Self {
        Self {
            kv_bytes: AtomicU64::new(0),
            kv_requests: AtomicU32::new(0),
            chunk_bytes: AtomicU64::new(0),
            chunk_requests: AtomicU32::new(0),
            recovery_bytes: AtomicU64::new(0),
            recovery_requests: AtomicU32::new(0),
            max_kv_bytes: kv_bytes,
            max_kv_requests: kv_requests,
            max_chunk_bytes: chunk_bytes,
            max_chunk_requests: chunk_requests,
        }
    }

    pub fn reset(&self) {
        self.kv_bytes.store(0, Ordering::Release);
        self.kv_requests.store(0, Ordering::Release);
        self.chunk_bytes.store(0, Ordering::Release);
        self.chunk_requests.store(0, Ordering::Release);
        self.recovery_bytes.store(0, Ordering::Release);
        self.recovery_requests.store(0, Ordering::Release);
    }

    fn reserve_kv(&self, bytes: usize) -> Result<(), StoreError> {
        reserve(&self.kv_requests, 1, self.max_kv_requests).map_err(|()| StoreError::Budget)?;
        reserve(&self.kv_bytes, bytes as u64, self.max_kv_bytes).map_err(|()| StoreError::Budget)
    }

    fn reserve_kv_key(&self, key: &[u8], bytes: usize) -> Result<(), StoreError> {
        match self.reserve_kv(bytes) {
            Ok(()) => Ok(()),
            Err(error) if is_task_key(key) => {
                reserve(&self.recovery_requests, 1, 16).map_err(|()| error)?;
                reserve(&self.recovery_bytes, bytes as u64, 2 * 1024 * 1024).map_err(|()| StoreError::Budget)
            }
            Err(error) => Err(error),
        }
    }

    fn reserve_chunk(&self, bytes: u64) -> Result<(), FileIoError> {
        reserve(&self.chunk_requests, 1, self.max_chunk_requests).map_err(|()| FileIoError::Bounds)?;
        reserve(&self.chunk_bytes, bytes, self.max_chunk_bytes).map_err(|()| FileIoError::Bounds)
    }
}

fn is_task_key(key: &[u8]) -> bool {
    matches!(
        IcebergKey::decode(key),
        Ok(IcebergKey::Catalog {
            scope: CatalogScope::GcTask,
            ..
        })
    )
}

fn reserve<Counter>(counter: &Counter, amount: Counter::Value, maximum: Counter::Value) -> Result<(), ()>
where
    Counter: BudgetCounter,
{
    counter.reserve(amount, maximum)
}

trait BudgetCounter {
    type Value: Copy;
    fn reserve(&self, amount: Self::Value, maximum: Self::Value) -> Result<(), ()>;
}

impl BudgetCounter for AtomicU32 {
    type Value = u32;

    fn reserve(&self, amount: u32, maximum: u32) -> Result<(), ()> {
        self.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(amount).filter(|next| *next <= maximum)
        })
        .map(|_| ())
        .map_err(|_| ())
    }
}

impl BudgetCounter for AtomicU64 {
    type Value = u64;

    fn reserve(&self, amount: u64, maximum: u64) -> Result<(), ()> {
        self.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(amount).filter(|next| *next <= maximum)
        })
        .map(|_| ())
        .map_err(|_| ())
    }
}

pub struct BudgetedGcStore {
    inner: Arc<dyn GcStore>,
    budget: Arc<GcIoBudget>,
}

impl BudgetedGcStore {
    pub fn new(inner: Arc<dyn GcStore>, budget: Arc<GcIoBudget>) -> Self {
        Self { inner, budget }
    }
}

#[async_trait]
impl CatalogStore for BudgetedGcStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.budget.reserve_kv_key(key, key.len() + MAX_RECORD_BYTES)?;
        self.inner.get(key).await
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.budget.reserve_kv_key(
            key,
            key.len() + expected.map_or(0, <[u8]>::len) + value.len() + MAX_RECORD_BYTES,
        )?;
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl GcStore for BudgetedGcStore {
    async fn scan_gc(&self, request: GcScan) -> Result<MultiScanPage, StoreError> {
        self.budget.reserve_kv(request.bytes)?;
        self.inner.scan_gc(request).await
    }

    async fn scan_gc_system(&self, request: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        self.budget.reserve_kv(request.bytes)?;
        self.inner.scan_gc_system(request).await
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.budget
            .reserve_kv(key.len() + expected.len() + MAX_RECORD_BYTES)?;
        self.inner.delete_gc_record(key, expected, identity).await
    }
}

pub struct BudgetedGcBlocks {
    inner: Arc<dyn FileBlockStore>,
    budget: Arc<GcIoBudget>,
}

impl BudgetedGcBlocks {
    pub fn new(inner: Arc<dyn FileBlockStore>, budget: Arc<GcIoBudget>) -> Self {
        Self { inner, budget }
    }
}

#[async_trait]
impl FileBlockStore for BudgetedGcBlocks {
    async fn put(&self, owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        self.budget.reserve_chunk(bytes.len() as u64)?;
        self.inner.put(owner, height, bytes).await
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.budget.reserve_chunk(root.logical_length)?;
        self.inner.read(root).await
    }

    async fn reclaim(&self, root: &ChunkRoot) -> Result<ReclaimOutcome, FileIoError> {
        self.budget.reserve_chunk(root.physical_length)?;
        self.inner.reclaim(root).await
    }
}
