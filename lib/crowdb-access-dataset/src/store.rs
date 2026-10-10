use async_trait::async_trait;

use crate::DatasetError;

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
