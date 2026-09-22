use std::sync::Arc;

use crate::catalog::{CasOutcome, CatalogContext, CatalogError, CatalogStore, RootState};
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, OperationId, SystemScope};
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;

use super::{NamespaceOperation, NamespacePhase};

pub struct NamespaceJournal {
    store: Arc<dyn CatalogStore>,
    payloads: PayloadStore,
}

impl NamespaceJournal {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self {
            payloads: PayloadStore::new(store.clone()),
            store,
        }
    }

    /// # Errors
    /// Rejects foreign domains, noninitial phases and changed request identity.
    pub async fn begin(&self, operation: NamespaceOperation) -> Result<NamespaceOperation, CatalogError> {
        operation.validate()?;
        if operation.phase != NamespacePhase::Prepared
            || operation.revision != 1
            || operation.mutation.is_some()
            || !operation.scan_after.is_empty()
            || operation.scan_generation != 0
        {
            return Err(ValidationError::Record.into());
        }
        self.check_context(operation.context).await?;
        if let Some(existing) = self.load(operation.context, operation.identity.operation).await? {
            return Self::matching_request(&operation, existing);
        }
        self.payloads.get(&operation.input).await?;
        let key = operation.key().encode()?;
        let bytes = StorageRecord::NamespaceOperation(Box::new(operation.clone())).encode()?;
        match self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?
        {
            CasOutcome::Applied(_) => Ok(operation),
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::NamespaceOperation(existing) =
                    StorageRecord::decode(&operation.key(), &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                Self::matching_request(&operation, *existing)
            }
            CasOutcome::Conflict(None) => Err(CatalogError::Busy),
        }
    }

    /// # Errors
    /// Rejects retired contexts and malformed durable operations.
    pub async fn load(
        &self,
        context: CatalogContext,
        operation: OperationId,
    ) -> Result<Option<NamespaceOperation>, CatalogError> {
        self.check_context(context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::NamespaceOperation,
            suffix: operation.as_bytes().to_vec(),
        };
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::NamespaceOperation(operation) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if operation.context != context {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(Some(*operation))
    }

    /// # Errors
    /// Rejects illegal transitions, missing payloads, changed targets and retired domains.
    pub async fn advance(
        &self,
        previous: &NamespaceOperation,
        next: &NamespaceOperation,
    ) -> Result<bool, CatalogError> {
        previous.validate()?;
        next.validate()?;
        if !previous.same_request(next)
            || previous.namespace != next.namespace
            || previous.parent != next.parent
            || previous.revision.checked_add(1) != Some(next.revision)
            || !previous.phase.permits(next.phase, previous.action)
        {
            return Err(ValidationError::Record.into());
        }
        if previous.mutation != next.mutation
            && !matches!(
                next.phase,
                NamespacePhase::Admitting
                    | NamespacePhase::Publishing
                    | NamespacePhase::Fencing
                    | NamespacePhase::Restoring
                    | NamespacePhase::Tombstoning
            )
        {
            return Err(ValidationError::Record.into());
        }
        self.check_context(previous.context).await?;
        if matches!(
            next.phase,
            NamespacePhase::ProbingNamespaces | NamespacePhase::ProbingTables
        ) {
            if next.phase == previous.phase && next.scan_after <= previous.scan_after {
                return Err(ValidationError::Key.into());
            }
            if next.phase != previous.phase && (!next.scan_after.is_empty() || next.scan_generation != 0) {
                return Err(ValidationError::Key.into());
            }
        }
        if next.mutation != previous.mutation {
            if let Some(mutation) = &next.mutation {
                for reference in mutation.before.iter().chain(std::iter::once(&mutation.after)) {
                    let bytes = self.payloads.get(reference).await?;
                    StorageRecord::decode(&IcebergKey::decode(&mutation.key)?, &bytes)?;
                }
            }
        }
        if let Some(outcome) = &next.outcome {
            self.payloads.get(&outcome.body).await?;
        }
        let key = previous.key().encode()?;
        let expected = StorageRecord::NamespaceOperation(Box::new(previous.clone())).encode()?;
        let value = StorageRecord::NamespaceOperation(Box::new(next.clone())).encode()?;
        Ok(matches!(
            self.store
                .compare_exchange(
                    &key,
                    Some(&expected),
                    &value,
                    mutation_identity(&key, Some(&expected), &value)
                )
                .await?,
            CasOutcome::Applied(_)
        ))
    }

    fn matching_request(
        request: &NamespaceOperation,
        existing: NamespaceOperation,
    ) -> Result<NamespaceOperation, CatalogError> {
        if !request.same_request(&existing) {
            return Err(CatalogError::Conflict);
        }
        Ok(existing)
    }

    async fn check_context(&self, context: CatalogContext) -> Result<(), CatalogError> {
        context.validate()?;
        let key = IcebergKey::System {
            scope: SystemScope::ActiveRoot,
            suffix: Vec::new(),
        };
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Uninitialized)?;
        let StorageRecord::Active(root) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if root.context != context || root.state != RootState::Ready {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }
}
