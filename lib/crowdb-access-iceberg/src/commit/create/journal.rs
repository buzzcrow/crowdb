use std::sync::Arc;

use super::{TableCreateOperation, TableCreatePhase as Phase};
use crate::{
    catalog::{check_context, CasOutcome, CatalogContext, CatalogError, CatalogStore},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId},
    operation::{mutation_identity, PayloadStore},
    record::StorageRecord,
};

pub struct TableCreateJournal {
    store: Arc<dyn CatalogStore>,
}

impl TableCreateJournal {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// # Errors
    /// Rejects retired contexts, corrupt records and foreign activation epochs.
    pub async fn load(
        &self,
        context: CatalogContext,
        identity: OperationId,
    ) -> Result<Option<TableCreateOperation>, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::TableCreateOperation,
            suffix: identity.as_bytes().to_vec(),
        };
        let value = self.store.get(&key.encode()?).await?;
        let result = value
            .map(|value| {
                let StorageRecord::TableCreateOperation(operation) =
                    StorageRecord::decode(&key, &value.bytes)?
                else {
                    return Err(ValidationError::Record);
                };
                if operation.context != context {
                    return Err(ValidationError::IdentityMismatch);
                }
                Ok(*operation)
            })
            .transpose()?;
        check_context(self.store.as_ref(), context).await?;
        Ok(result)
    }

    pub(super) async fn begin(
        &self,
        operation: TableCreateOperation,
    ) -> Result<TableCreateOperation, CatalogError> {
        operation.validate()?;
        if !matches!(operation.phase, Phase::Prepared | Phase::Staged) || operation.revision != 1 {
            return Err(ValidationError::Record.into());
        }
        if let Some(existing) = self.load(operation.context, operation.identity.operation).await? {
            return matching(&operation, existing);
        }
        let payloads = PayloadStore::new(self.store.clone());
        for payload in [&operation.input, &operation.document, &operation.response] {
            payloads.get(payload).await?;
        }
        check_context(self.store.as_ref(), operation.context).await?;
        let key = operation.key();
        let encoded = key.encode()?;
        let bytes = StorageRecord::TableCreateOperation(Box::new(operation.clone())).encode()?;
        let result = match self
            .store
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await?
        {
            CasOutcome::Applied(_) => operation,
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::TableCreateOperation(existing) =
                    StorageRecord::decode(&key, &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                matching(&operation, *existing)?
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        };
        check_context(self.store.as_ref(), result.context).await?;
        Ok(result)
    }

    pub(super) async fn advance(
        &self,
        before: &TableCreateOperation,
        after: &TableCreateOperation,
    ) -> Result<(), CatalogError> {
        before.validate()?;
        after.validate()?;
        let mut unchanged = after.clone();
        if before.binding_transition(after)? {
            unchanged.stage = before.stage.clone();
            unchanged.candidate = before.candidate.clone();
            unchanged.document = before.document.clone();
            unchanged.response = before.response.clone();
            unchanged.timestamp_ms = before.timestamp_ms;
            let payloads = PayloadStore::new(self.store.clone());
            let stage = after.stage.as_ref().ok_or(ValidationError::Record)?;
            let binding = stage.binding.as_ref().ok_or(ValidationError::Record)?;
            for payload in [&binding.input, &after.document, &after.response] {
                payloads.get(payload).await?;
            }
        }
        unchanged.phase = before.phase;
        unchanged.revision = before.revision;
        unchanged.admission = before.admission.clone();
        unchanged.outcome = before.outcome.clone();
        if &unchanged != before
            || (before.outcome.is_some() && before.outcome != after.outcome)
            || before.revision.checked_add(1) != Some(after.revision)
            || !before.phase.permits(after.phase)
            || (before.admission != after.admission
                && !matches!(
                    (before.phase, after.phase),
                    (Phase::FilesReady, Phase::Admitting) | (Phase::Admitting, Phase::FilesReady)
                ))
        {
            return Err(ValidationError::Record.into());
        }
        if let Some(outcome) = &after.outcome {
            PayloadStore::new(self.store.clone()).get(&outcome.body).await?;
        }
        check_context(self.store.as_ref(), before.context).await?;
        let key = before.key().encode()?;
        let expected = StorageRecord::TableCreateOperation(Box::new(before.clone())).encode()?;
        let next = StorageRecord::TableCreateOperation(Box::new(after.clone())).encode()?;
        let result = self
            .store
            .compare_exchange(
                &key,
                Some(&expected),
                &next,
                mutation_identity(&key, Some(&expected), &next),
            )
            .await?;
        check_context(self.store.as_ref(), before.context).await?;
        if !matches!(result, CasOutcome::Applied(_)) {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }
}

fn matching(
    request: &TableCreateOperation,
    existing: TableCreateOperation,
) -> Result<TableCreateOperation, CatalogError> {
    if request.context != existing.context
        || request.identity != existing.identity
        || request.principal != existing.principal
        || request.namespace != existing.namespace
        || request.input != existing.input
    {
        return Err(CatalogError::Conflict);
    }
    Ok(existing)
}
