use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

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
    #[error("background Chunk-KV admission budget exhausted")]
    Budget,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct CatalogStoreOperationCounts {
    pub get: u64,
    pub compare_exchange: u64,
    pub scan: u64,
    pub conditional_delete: u64,
}

#[derive(Default)]
struct OperationCounters {
    get: AtomicU64,
    compare_exchange: AtomicU64,
    scan: AtomicU64,
    conditional_delete: AtomicU64,
}

#[derive(Clone, Default)]
pub struct CatalogStoreOperationMeter(Arc<OperationCounters>);

impl CatalogStoreOperationMeter {
    #[must_use]
    pub fn snapshot(&self) -> CatalogStoreOperationCounts {
        self.0.snapshot()
    }

    pub async fn observe<F: std::future::Future>(&self, future: F) -> F::Output {
        REQUEST_OPERATIONS.scope(self.clone(), future).await
    }
}

tokio::task_local! {
    static REQUEST_OPERATIONS: CatalogStoreOperationMeter;
}

fn count_request(operation: fn(&OperationCounters) -> &AtomicU64) {
    let _ = REQUEST_OPERATIONS.try_with(|meter| {
        operation(&meter.0).fetch_add(1, Ordering::Relaxed);
    });
}

impl OperationCounters {
    fn snapshot(&self) -> CatalogStoreOperationCounts {
        CatalogStoreOperationCounts {
            get: self.get.load(Ordering::Relaxed),
            compare_exchange: self.compare_exchange.load(Ordering::Relaxed),
            scan: self.scan.load(Ordering::Relaxed),
            conditional_delete: self.conditional_delete.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
pub trait CatalogStore: Send + Sync {
    fn operation_counts(&self) -> Option<CatalogStoreOperationCounts> {
        None
    }

    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError>;
    /// Scans only a bounded native file-location interval.
    /// # Errors
    /// Stores without listing support reject the request rather than return an empty page.
    async fn scan_file_locations(
        &self,
        _scan: crate::file::FileLocationScan,
    ) -> Result<MultiScanPage, StoreError> {
        Err(StoreError::Budget)
    }
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
    counters: OperationCounters,
}

impl RoutedCatalogStore {
    #[must_use]
    pub fn new(client: Arc<ChunkKvClient>) -> Self {
        Self {
            client,
            counters: OperationCounters::default(),
        }
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
        self.counters.conditional_delete.fetch_add(1, Ordering::Relaxed);
        count_request(|counters| &counters.conditional_delete);
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
        self.counters.scan.fetch_add(1, Ordering::Relaxed);
        count_request(|counters| &counters.scan);
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
    async fn scan_file_locations(
        &self,
        scan: crate::file::FileLocationScan,
    ) -> Result<MultiScanPage, StoreError> {
        self.scan(scan.request()?).await
    }
    fn operation_counts(&self) -> Option<CatalogStoreOperationCounts> {
        Some(self.counters.snapshot())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        IcebergKey::decode(key)?;
        self.counters.get.fetch_add(1, Ordering::Relaxed);
        count_request(|counters| &counters.get);
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
        self.counters.compare_exchange.fetch_add(1, Ordering::Relaxed);
        count_request(|counters| &counters.compare_exchange);
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
