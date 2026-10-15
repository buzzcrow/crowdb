use async_trait::async_trait;

use crate::DatasetError;

mod scan;
pub use scan::{MetadataPage, MetadataScan, MetadataScanBudget};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredValue {
    pub bytes: Vec<u8>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CasOutcome {
    Applied(u64),
    Conflict(Option<StoredValue>),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Invalid(#[from] DatasetError),
    #[error("dataset store rejected the operation")]
    Rejected,
    #[error("Chunk-KV client failed: {0}")]
    Client(#[from] crowdb_chunk_kv_client::ClientError),
    #[error("Chunk-KV returned a failure: {0}")]
    Failure(String),
    #[error("dataset store returned a malformed response")]
    Response,
}

#[async_trait]
pub trait DatasetStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError>;

    /// Reads at most one key without materializing an oversized value.
    /// Implementations must not emulate this by checking an unbounded get result.
    async fn get_bounded(
        &self,
        _key: &[u8],
        _budget: MetadataScanBudget,
        _cancel: &crate::ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        Err(StoreError::Rejected)
    }

    /// Returns a strictly ordered prefix page, with explicit continuation.
    async fn scan_page(
        &self,
        _request: &MetadataScan,
        _cancel: &crate::ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        Err(StoreError::Rejected)
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError>;

    async fn delete(&self, _key: &[u8]) -> Result<(), StoreError> {
        Err(StoreError::Rejected)
    }
}
