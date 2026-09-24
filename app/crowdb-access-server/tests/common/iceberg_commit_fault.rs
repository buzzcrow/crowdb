use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, RoutedCatalogStore, StoreError, StoredValue},
    file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError},
    key::IcebergKey,
    namespace::{ChildScan, NamespaceStore},
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub struct TestBoundary {
    count: AtomicUsize,
    target: usize,
    after: bool,
    marker: PathBuf,
}

impl TestBoundary {
    pub fn new(target: usize, after: bool, marker: PathBuf) -> Self {
        Self {
            count: AtomicUsize::new(0),
            target,
            after,
            marker,
        }
    }

    pub async fn before(&self, label: &str) -> usize {
        let index = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        self.observe(index, label, false).await;
        index
    }

    pub async fn observe(&self, index: usize, label: &str, after: bool) {
        let paused = index == self.target && after == self.after;
        let state = serde_json::json!({"index":index,"label":label,"after":after,"paused":paused});
        std::fs::write(&self.marker, serde_json::to_vec(&state).unwrap()).unwrap();
        if paused {
            std::future::pending::<()>().await;
        }
    }
}

pub struct TestCommitStore {
    pub inner: Arc<RoutedCatalogStore>,
    pub boundary: Arc<TestBoundary>,
}

#[async_trait]
impl NamespaceStore for TestCommitStore {
    async fn scan_children(&self, request: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(request).await
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let index = self.boundary.before("mapping-delete").await;
        let result = self.inner.delete_mapping(key, expected, identity).await;
        self.boundary.observe(index, "mapping-delete", true).await;
        result
    }
}

#[async_trait]
impl CatalogStore for TestCommitStore {
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
        let label = match StorageRecord::decode(&IcebergKey::decode(key)?, value)? {
            StorageRecord::TableCreateOperation(operation) => format!("create-{:?}", operation.phase),
            StorageRecord::TableCommitOperation(operation) => format!("commit-{:?}", operation.phase),
            StorageRecord::TableHead(head) => format!("head-{}", head.generation),
            StorageRecord::File(_) => "file-record".into(),
            StorageRecord::FileMapping(_) => "file-mapping".into(),
            _ => "journal-or-fence".into(),
        };
        let index = self.boundary.before(&label).await;
        let result = self.inner.compare_exchange(key, expected, value, identity).await;
        self.boundary.observe(index, &label, true).await;
        result
    }
}

pub struct TestCommitBlocks {
    pub inner: Arc<dyn FileBlockStore>,
    pub boundary: Arc<TestBoundary>,
}

#[async_trait]
impl FileBlockStore for TestCommitBlocks {
    async fn put(&self, owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        let index = self.boundary.before("file-block").await;
        let result = self.inner.put(owner, height, bytes).await;
        self.boundary.observe(index, "file-block", true).await;
        result
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.inner.read(root).await
    }
}
