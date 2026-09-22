use std::sync::Arc;

use crowdb_chunk_kv_client::MultiScanContinuation;

use crate::catalog::{CatalogContext, CatalogError, StoreError};
use crate::error::ValidationError;
use crate::key::{IcebergKey, OperationId};
use crate::record::StorageRecord;

use super::{
    NamespaceAction, NamespaceCreator, NamespaceDropper, NamespaceOperation, NamespaceOutcome,
    NamespaceRecoveryScan, NamespaceRecoveryStore,
};

pub struct NamespaceRecovery {
    creator: NamespaceCreator,
    store: Arc<dyn NamespaceRecoveryStore>,
}

#[derive(Debug)]
pub struct NamespaceRecoveryPage {
    pub continuation: Option<MultiScanContinuation>,
    pub completed: usize,
    pub deferred: usize,
    pub failures: Vec<(OperationId, CatalogError)>,
}

impl NamespaceRecovery {
    #[must_use]
    pub fn new<Store: NamespaceRecoveryStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            creator: NamespaceCreator::new(store.clone()),
            store,
        }
    }

    /// # Errors
    /// Rejects retired contexts, invalid scan responses and unavailable storage.
    pub async fn recover_page(
        &self,
        context: CatalogContext,
        continuation: Option<MultiScanContinuation>,
    ) -> Result<NamespaceRecoveryPage, CatalogError> {
        self.creator.repository.check_context(context).await?;
        let scan = NamespaceRecoveryScan {
            catalog: context.catalog,
            continuation,
        };
        let request = scan.request()?;
        let page = self.store.scan_namespace_operations(scan.clone()).await?;
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
            if item.key <= last || item.key >= *end {
                return Err(ValidationError::Key.into());
            }
            last.clone_from(&item.key);
            let key = IcebergKey::decode(&item.key)?;
            let StorageRecord::NamespaceOperation(operation) = StorageRecord::decode(&key, &item.value)?
            else {
                return Err(ValidationError::Record.into());
            };
            if operation.context != context {
                return Err(ValidationError::IdentityMismatch.into());
            }
            operations.push(operation);
        }
        if let Some(cursor) = &page.continuation {
            NamespaceRecoveryScan {
                continuation: Some(cursor.clone()),
                ..scan
            }
            .request()?;
            if cursor.last_key < last || operations.is_empty() {
                return Err(ValidationError::Key.into());
            }
        }
        let mut report = NamespaceRecoveryPage {
            continuation: page.continuation,
            completed: 0,
            deferred: 0,
            failures: Vec::new(),
        };
        for operation in operations {
            let identity = operation.identity.operation;
            match self.resume_operation(&operation).await {
                Ok(_) => report.completed += 1,
                Err(CatalogError::Busy) => report.deferred += 1,
                Err(error) => report.failures.push((identity, error)),
            }
        }
        self.creator.repository.check_context(context).await?;
        Ok(report)
    }

    async fn resume_operation(
        &self,
        operation: &NamespaceOperation,
    ) -> Result<NamespaceOutcome, CatalogError> {
        let mut budget = 16;
        let context = operation.context;
        let identity = operation.identity.operation;
        match operation.action {
            NamespaceAction::Create => {
                self.creator
                    .resume_with_budget(context, identity, &mut budget)
                    .await
            }
            NamespaceAction::Update => {
                self.creator
                    .repository
                    .resume_property_with_budget(context, identity, &mut budget)
                    .await
            }
            NamespaceAction::Drop => {
                NamespaceDropper {
                    creator: self.creator.clone(),
                }
                .resume_with_budget(context, identity, &mut budget)
                .await
            }
        }
    }
}
