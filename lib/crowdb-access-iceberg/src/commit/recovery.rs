use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::ScanDirection;

use super::{
    recover_table_commit, CommitProofLimits, CommitPublicationError, StagedCommitLimits, TableCreatePhase,
    TableCreator,
};
use crate::{
    catalog::{check_context, CatalogContext, CatalogError, CatalogStore, RoutedCatalogStore, StoreError},
    error::ValidationError,
    file::FileBlockStore,
    key::{CatalogId, CatalogScope, IcebergKey, OperationId},
    namespace::NamespaceStore,
    record::{StorageRecord, MAX_RECORD_BYTES},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableRecoveryKind {
    Create,
    Update,
    Lifecycle,
}

#[derive(Clone, Debug)]
pub struct TableRecoveryScan {
    pub catalog: CatalogId,
    pub kind: TableRecoveryKind,
    pub continuation: Option<MultiScanContinuation>,
}

impl TableRecoveryScan {
    /// # Errors
    /// Rejects foreign or backward scan continuations.
    pub fn request(&self) -> Result<MultiScanRequest, ValidationError> {
        let scope = match self.kind {
            TableRecoveryKind::Create => CatalogScope::TableCreateOperation,
            TableRecoveryKind::Update => CatalogScope::TableCommitOperation,
            TableRecoveryKind::Lifecycle => CatalogScope::TableLifecycleOperation,
        };
        let mut start = IcebergKey::catalog_range(self.catalog).start;
        let mut end = start.clone();
        start.push(scope as u8);
        end.push(scope as u8 + 1);
        if self.continuation.as_ref().is_some_and(|cursor| {
            cursor.original_start.as_ref() != Some(&start)
                || cursor.original_end.as_ref() != Some(&end)
                || cursor.direction != ScanDirection::Forward
                || cursor.catalog_generation == 0
                || cursor.last_key < start
                || cursor.last_key >= end
        }) {
            return Err(ValidationError::Key);
        }
        Ok(MultiScanRequest {
            start: Some(start),
            end: Some(end),
            direction: ScanDirection::Forward,
            max_items: 4,
            max_bytes: 4 * MAX_RECORD_BYTES,
            continuation: self.continuation.clone(),
        })
    }
}

#[async_trait]
pub trait TableRecoveryStore: NamespaceStore {
    async fn scan_table_operations(&self, scan: TableRecoveryScan) -> Result<MultiScanPage, StoreError>;
}

#[async_trait]
impl TableRecoveryStore for RoutedCatalogStore {
    async fn scan_table_operations(&self, scan: TableRecoveryScan) -> Result<MultiScanPage, StoreError> {
        self.scan(scan.request()?).await
    }
}

pub struct TableRecovery {
    store: Arc<dyn CatalogStore>,
    scanner: Arc<dyn TableRecoveryStore>,
    creator: TableCreator,
    blocks: Arc<dyn FileBlockStore>,
    limits: CommitProofLimits,
    lifecycles: crate::table::TableLifecycles,
}

pub struct TableRecoveryPage {
    pub continuation: Option<MultiScanContinuation>,
    pub progressed: usize,
    pub retained: usize,
    pub failures: Vec<(OperationId, CommitPublicationError)>,
}

impl TableRecovery {
    #[must_use]
    pub fn new<Store: TableRecoveryStore + 'static>(
        store: Arc<Store>,
        blocks: Arc<dyn FileBlockStore>,
        limits: CommitProofLimits,
    ) -> Self {
        Self {
            lifecycles: crate::table::TableLifecycles::new(store.clone()),
            store: store.clone(),
            scanner: store.clone(),
            creator: TableCreator::new(store, blocks.clone()).with_staged_limits(StagedCommitLimits {
                evaluation: limits.preparation.evaluation,
                snapshots: limits.snapshots,
                auxiliary: limits.auxiliary,
            }),
            blocks,
            limits,
        }
    }

    /// # Errors
    /// Rejects retired contexts, malformed pages and invalid expiry clocks before helping.
    pub async fn recover_page(
        &self,
        context: CatalogContext,
        kind: TableRecoveryKind,
        continuation: Option<MultiScanContinuation>,
        now_ms: i64,
    ) -> Result<TableRecoveryPage, CatalogError> {
        if now_ms < 0 {
            return Err(ValidationError::Deadline.into());
        }
        check_context(self.store.as_ref(), context).await?;
        let scan = TableRecoveryScan {
            catalog: context.catalog,
            kind,
            continuation,
        };
        let request = scan.request()?;
        let page = self.scanner.scan_table_operations(scan.clone()).await?;
        if let Some(failure) = page.terminal_failure {
            return Err(StoreError::Rejected(failure).into());
        }
        if page.items.len() > request.max_items {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let start = request.start.as_ref().ok_or(ValidationError::Key)?;
        let end = request.end.as_ref().ok_or(ValidationError::Key)?;
        let mut last = request
            .continuation
            .as_ref()
            .map_or(start, |cursor| &cursor.last_key)
            .clone();
        let mut operations = Vec::with_capacity(page.items.len());
        for item in page.items {
            if item.key <= last || item.key >= *end || item.revision == 0 {
                return Err(ValidationError::Key.into());
            }
            last.clone_from(&item.key);
            let record = StorageRecord::decode(&IcebergKey::decode(&item.key)?, &item.value)?;
            match (&record, kind) {
                (StorageRecord::TableCreateOperation(operation), TableRecoveryKind::Create)
                    if operation.context == context => {}
                (StorageRecord::TableCommitOperation(operation), TableRecoveryKind::Update)
                    if operation.context == context => {}
                (StorageRecord::TableLifecycleOperation(operation), TableRecoveryKind::Lifecycle)
                    if operation.context == context => {}
                _ => return Err(ValidationError::IdentityMismatch.into()),
            }
            operations.push(record);
        }
        if let Some(cursor) = &page.continuation {
            TableRecoveryScan {
                continuation: Some(cursor.clone()),
                ..scan
            }
            .request()?;
            if operations.is_empty() || cursor.last_key != last {
                return Err(ValidationError::Key.into());
            }
        }
        let mut report = TableRecoveryPage {
            continuation: page.continuation,
            progressed: 0,
            retained: 0,
            failures: Vec::new(),
        };
        for operation in operations {
            let (identity, result) = self.resume_operation(context, operation, now_ms).await?;
            match result {
                Ok(true) => report.progressed += 1,
                Ok(false) => report.retained += 1,
                Err(error) => report.failures.push((identity, error)),
            }
        }
        check_context(self.store.as_ref(), context).await?;
        Ok(report)
    }

    async fn resume_operation(
        &self,
        context: CatalogContext,
        operation: StorageRecord,
        now_ms: i64,
    ) -> Result<(OperationId, Result<bool, CommitPublicationError>), CatalogError> {
        let result = match operation {
            StorageRecord::TableLifecycleOperation(operation) => {
                let identity = operation.identity.operation;
                (
                    identity,
                    self.lifecycles
                        .resume(context, identity)
                        .await
                        .map(|_| true)
                        .map_err(CommitPublicationError::from),
                )
            }
            StorageRecord::TableCreateOperation(operation) => {
                let identity = operation.identity.operation;
                let result = if operation.phase == TableCreatePhase::Staged {
                    self.creator
                        .expire_stage(context, operation.candidate.table, now_ms)
                        .await
                } else {
                    self.creator.resume(context, identity).await.map(|_| true)
                };
                (identity, result)
            }
            StorageRecord::TableCommitOperation(operation) => {
                let identity = operation.identity.operation;
                (
                    identity,
                    recover_table_commit(
                        self.store.clone(),
                        self.blocks.clone(),
                        context,
                        identity,
                        self.limits,
                    )
                    .await
                    .map(|_| true),
                )
            }
            _ => return Err(ValidationError::Record.into()),
        };
        Ok(result)
    }
}
