use std::ops::Range;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_chunk_kv_client::ChunkKvClient;
use crowdb_protocol::chunk_kv::{ChunkKvResponse, OperationResult, RpcCompareCondition, RpcValue};

use crate::store::{CasOutcome, DatasetStore, StoreError, StoredValue};

/// A cancellation handle shared by bounded payload reads.
#[derive(Clone, Default)]
pub struct ReadCancellation(Arc<AtomicBool>);

impl ReadCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum ChunkReadError {
    #[error("chunk was not found")]
    NotFound,
    #[error("chunk read was cancelled")]
    Cancelled,
    #[error("chunk read failed transiently: {0}")]
    Transient(String),
    #[error("chunk payload was truncated")]
    Truncated,
    #[error("chunk read failed: {0}")]
    Failed(String),
}

/// Dataset-owned payload reader. Locations are opaque to Dataset code.
#[async_trait]
pub trait ChunkReader: Send + Sync {
    async fn read(
        &self,
        location: &[u8],
        range: Option<Range<u64>>,
        cancel: &ReadCancellation,
    ) -> Result<Vec<u8>, ChunkReadError>;

    /// Reads a payload in bounded pieces, applying backpressure at the caller's window.
    async fn read_stream(
        &self,
        location: &[u8],
        chunk_size: usize,
        cancel: &ReadCancellation,
    ) -> Result<Vec<Vec<u8>>, ChunkReadError> {
        if chunk_size == 0 {
            return Err(ChunkReadError::Failed("zero chunk size".into()));
        }
        let payload = self.read(location, None, cancel).await?;
        if cancel.is_cancelled() {
            return Err(ChunkReadError::Cancelled);
        }
        Ok(payload.chunks(chunk_size).map(ToOwned::to_owned).collect())
    }
}

struct UnavailableChunkReader;
#[async_trait]
impl ChunkReader for UnavailableChunkReader {
    async fn read(
        &self,
        _location: &[u8],
        _range: Option<Range<u64>>,
        cancel: &ReadCancellation,
    ) -> Result<Vec<u8>, ChunkReadError> {
        if cancel.is_cancelled() {
            Err(ChunkReadError::Cancelled)
        } else {
            Err(ChunkReadError::NotFound)
        }
    }
}

#[must_use]
pub fn unavailable_chunk_reader() -> Arc<dyn ChunkReader> {
    Arc::new(UnavailableChunkReader)
}

/// Production Dataset metadata store backed by routed Chunk-KV point operations.
pub struct ChunkKvDatasetStore {
    client: Arc<ChunkKvClient>,
}
impl ChunkKvDatasetStore {
    #[must_use]
    pub fn new(client: Arc<ChunkKvClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DatasetStore for ChunkKvDatasetStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let response = self.client.get(key.to_vec(), None).await?;
        match response.result.map_err(|error| failure(&error))? {
            OperationResult::Value(value) => Ok(value.map(stored_value)),
            _ => Err(StoreError::Response),
        }
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        let response = match expected {
            None => self.client.put_if_absent(key.to_vec(), value.to_vec()).await?,
            Some(expected) => {
                self.client
                    .compare_exchange(
                        key.to_vec(),
                        RpcCompareCondition::Value(expected.to_vec()),
                        value.to_vec(),
                    )
                    .await?
            }
        };
        mutation_outcome(response)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.client.delete(key.to_vec()).await.map_err(StoreError::from)?;
        Ok(())
    }
}
fn stored_value(value: RpcValue) -> StoredValue {
    StoredValue {
        bytes: value.value,
        revision: value.revision,
    }
}
fn mutation_outcome(response: ChunkKvResponse) -> Result<CasOutcome, StoreError> {
    match response.result.map_err(|error| failure(&error))? {
        OperationResult::Mutation {
            applied: true,
            revision: Some(revision),
            ..
        } => Ok(CasOutcome::Applied(revision)),
        OperationResult::Mutation {
            applied: false,
            observed,
            ..
        } => Ok(CasOutcome::Conflict(observed.map(stored_value))),
        _ => Err(StoreError::Response),
    }
}
fn failure(error: &crowdb_protocol::chunk_kv::RpcFailure) -> StoreError {
    StoreError::Failure(format!("{:?}: {}", error.code, error.message))
}
