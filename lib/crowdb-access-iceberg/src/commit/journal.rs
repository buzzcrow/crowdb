use std::sync::Arc;

use super::{TableCommitOperation, TableCommitPhase};
use crate::{
    catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId},
    operation::{mutation_identity, PayloadStore},
    record::StorageRecord,
    table::head_key,
};

/// CAS journal for update intents; it neither validates files nor mutates table heads.
pub struct TableCommitJournal {
    store: Arc<dyn CatalogStore>,
    payloads: PayloadStore,
}

impl TableCommitJournal {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self {
            payloads: PayloadStore::new(store.clone()),
            store,
        }
    }

    /// # Errors
    /// Rejects invalid initial records, unavailable request payloads and identity reuse.
    pub async fn begin(&self, operation: TableCommitOperation) -> Result<TableCommitOperation, CatalogError> {
        operation.validate()?;
        if operation.phase != TableCommitPhase::Prepared || operation.revision != 1 {
            return Err(ValidationError::Record.into());
        }
        if let Some(existing) = self.load(operation.context, operation.identity.operation).await? {
            return matching_request(&operation, existing);
        }
        self.payloads.get(&operation.input).await?;
        self.check_context(operation.context).await?;
        let key = operation.key().encode()?;
        let bytes = StorageRecord::TableCommitOperation(Box::new(operation.clone())).encode()?;
        let result = match self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?
        {
            CasOutcome::Applied(_) => operation,
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::TableCommitOperation(existing) =
                    StorageRecord::decode(&operation.key(), &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                matching_request(&operation, *existing)?
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        };
        self.check_context(result.context).await?;
        Ok(result)
    }

    /// # Errors
    /// Rejects retired contexts, invalid records and foreign activation epochs.
    pub async fn load(
        &self,
        context: CatalogContext,
        operation: OperationId,
    ) -> Result<Option<TableCommitOperation>, CatalogError> {
        self.check_context(context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::TableCommitOperation,
            suffix: operation.as_bytes().to_vec(),
        };
        let Some(value) = self.store.get(&key.encode()?).await? else {
            self.check_context(context).await?;
            return Ok(None);
        };
        let StorageRecord::TableCommitOperation(operation) = StorageRecord::decode(&key, &value.bytes)?
        else {
            return Err(ValidationError::Record.into());
        };
        if operation.context != context {
            return Err(ValidationError::IdentityMismatch.into());
        }
        self.check_context(context).await?;
        Ok(Some(*operation))
    }

    /// # Errors
    /// Rejects rebasing, candidate replacement, phase skips and unproven publication outcomes.
    pub async fn advance(
        &self,
        previous: &TableCommitOperation,
        next: &TableCommitOperation,
    ) -> Result<bool, CatalogError> {
        previous.validate()?;
        next.validate()?;
        if !previous.same_request(next)
            || previous.revision.checked_add(1) != Some(next.revision)
            || !previous.phase.permits(next.phase)
            || (previous.candidate != next.candidate
                && !(previous.phase == TableCommitPhase::Prepared
                    && next.phase == TableCommitPhase::Validated))
        {
            return Err(ValidationError::Record.into());
        }
        self.check_context(previous.context).await?;
        if let Some(outcome) = &next.outcome {
            self.payloads.get(&outcome.body).await?;
        }
        if previous.phase == TableCommitPhase::Publishing {
            self.publication_outcome(previous, next.phase).await?;
        }
        let key = previous.key().encode()?;
        let before = StorageRecord::TableCommitOperation(Box::new(previous.clone())).encode()?;
        let after = StorageRecord::TableCommitOperation(Box::new(next.clone())).encode()?;
        let applied = matches!(
            self.store
                .compare_exchange(
                    &key,
                    Some(&before),
                    &after,
                    mutation_identity(&key, Some(&before), &after)
                )
                .await?,
            CasOutcome::Applied(_)
        );
        self.check_context(previous.context).await?;
        Ok(applied)
    }

    async fn publication_outcome(
        &self,
        operation: &TableCommitOperation,
        next: TableCommitPhase,
    ) -> Result<(), CatalogError> {
        let key = head_key(operation.before.catalog, operation.before.table);
        let value = self.store.get(&key.encode()?).await?.ok_or(CatalogError::Busy)?;
        let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let candidate = operation.candidate.as_ref().ok_or(ValidationError::Record)?;
        let valid = match next {
            TableCommitPhase::Published => head.as_ref() == candidate,
            TableCommitPhase::Rejected => {
                head.as_ref() != &operation.before
                    && head.as_ref() != candidate
                    && head.generation >= operation.before.generation
                    && head.operation_fence >= operation.before.operation_fence
                    && (head.generation > operation.before.generation
                        || head.operation_fence > operation.before.operation_fence)
                    && head.pending_operation != Some(operation.identity.operation)
            }
            _ => false,
        };
        if !valid {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }

    async fn check_context(&self, context: CatalogContext) -> Result<(), CatalogError> {
        check_context(self.store.as_ref(), context).await
    }
}

fn matching_request(
    request: &TableCommitOperation,
    existing: TableCommitOperation,
) -> Result<TableCommitOperation, CatalogError> {
    if request.context != existing.context
        || request.identity != existing.identity
        || request.principal != existing.principal
        || request.input != existing.input
        || request.before.table != existing.before.table
        || request.before.namespace != existing.before.namespace
        || request.before.name != existing.before.name
        || request.before.name_epoch != existing.before.name_epoch
    {
        return Err(CatalogError::Conflict);
    }
    Ok(existing)
}
