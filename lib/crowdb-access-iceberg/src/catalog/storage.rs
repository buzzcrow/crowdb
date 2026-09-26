use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv_client::{ChunkKvClient, ClientError, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::{
    ClientRequestId, OperationResult, PointOperation, RpcCompareCondition, RpcFailure, RpcValue,
    ScanDirection,
};

use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, MAX_KEY_BYTES};
use crate::record::MAX_RECORD_BYTES;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("Chunk-KV rejected Iceberg operation: {0:?}")]
    Rejected(RpcFailure),
    #[error("invalid Chunk-KV response")]
    Response,
}

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

#[async_trait]
pub trait CatalogStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError>;
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError>;
}

pub struct RoutedCatalogStore {
    client: Arc<ChunkKvClient>,
}

impl RoutedCatalogStore {
    #[must_use]
    pub fn new(client: Arc<ChunkKvClient>) -> Self {
        Self { client }
    }

    pub(crate) async fn delete_mapping_if(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        if !matches!(
            IcebergKey::decode(key)?,
            IcebergKey::Catalog {
                scope: CatalogScope::NamespaceName | CatalogScope::TableName,
                ..
            }
        ) {
            return Err(ValidationError::Key.into());
        }
        self.delete_gc_record_if(key, expected, identity).await
    }

    pub(crate) async fn delete_gc_record_if(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        if matches!(
            IcebergKey::decode(key)?,
            IcebergKey::System {
                scope: crate::key::SystemScope::ActiveRoot,
                ..
            }
        ) {
            return Err(ValidationError::Key.into());
        }
        validate_value(expected)?;
        let response = self
            .client
            .execute_with_identity(
                PointOperation::ConditionalDelete {
                    key: key.to_vec(),
                    condition: RpcCompareCondition::Value(expected.to_vec()),
                },
                None,
                identity,
            )
            .await?;
        match response.result.map_err(StoreError::Rejected)? {
            OperationResult::Mutation {
                applied: true,
                revision: Some(revision),
                ..
            } if revision != 0 => Ok(CasOutcome::Applied(revision)),
            OperationResult::Mutation {
                applied: false,
                observed,
                ..
            } => Ok(CasOutcome::Conflict(
                observed.map(|value| stored(key, value)).transpose()?,
            )),
            _ => Err(StoreError::Response),
        }
    }

    /// # Errors
    /// Returns invalid bounds, storage failures, or malformed scan responses.
    pub async fn scan(&self, request: MultiScanRequest) -> Result<MultiScanPage, StoreError> {
        let (Some(start), Some(end)) = (&request.start, &request.end) else {
            return Err(ValidationError::Key.into());
        };
        if start.as_slice() < b"ICE\0\x01".as_slice()
            || end.as_slice() > b"ICE\0\x02".as_slice()
            || start >= end
            || start.len() > MAX_KEY_BYTES
            || end.len() > MAX_KEY_BYTES
        {
            return Err(ValidationError::Key.into());
        }
        if request.max_items == 0
            || request.max_items > 256
            || request.max_bytes == 0
            || request.max_bytes > MAX_RECORD_BYTES * 256
            || request.direction != ScanDirection::Forward
        {
            return Err(StoreError::Response);
        }
        let page = self.client.scan(request).await?;
        if let Some(failure) = page.terminal_failure {
            return Err(StoreError::Rejected(failure));
        }
        for value in &page.items {
            if value.revision == 0 {
                return Err(StoreError::Response);
            }
            IcebergKey::decode(&value.key)?;
            validate_value(&value.value)?;
        }
        Ok(page)
    }
}

#[async_trait]
impl CatalogStore for RoutedCatalogStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        IcebergKey::decode(key)?;
        let response = self.client.get(key.to_vec(), None).await?;
        match response.result.map_err(StoreError::Rejected)? {
            OperationResult::Value(value) => value.map(|value| stored(key, value)).transpose(),
            _ => Err(StoreError::Response),
        }
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        IcebergKey::decode(key)?;
        validate_value(value)?;
        let operation = match expected {
            Some(expected) => {
                validate_value(expected)?;
                PointOperation::CompareExchange {
                    key: key.to_vec(),
                    condition: RpcCompareCondition::Value(expected.to_vec()),
                    value: value.to_vec(),
                }
            }
            None => PointOperation::PutIfAbsent {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        };
        let response = self
            .client
            .execute_with_identity(operation, None, identity)
            .await?;
        match response.result.map_err(StoreError::Rejected)? {
            OperationResult::Mutation {
                applied: true,
                revision: Some(revision),
                ..
            } if revision != 0 => Ok(CasOutcome::Applied(revision)),
            OperationResult::Mutation {
                applied: false,
                observed,
                ..
            } => Ok(CasOutcome::Conflict(
                observed.map(|value| stored(key, value)).transpose()?,
            )),
            _ => Err(StoreError::Response),
        }
    }
}

fn stored(key: &[u8], value: RpcValue) -> Result<StoredValue, StoreError> {
    if value.key != key || value.revision == 0 {
        return Err(StoreError::Response);
    }
    validate_value(&value.value)?;
    Ok(StoredValue {
        bytes: value.value,
        revision: value.revision,
    })
}

fn validate_value(value: &[u8]) -> Result<(), StoreError> {
    if value.is_empty() {
        return Err(ValidationError::Record.into());
    }
    if value.len() > MAX_RECORD_BYTES {
        return Err(ValidationError::RecordTooLarge.into());
    }
    Ok(())
}
