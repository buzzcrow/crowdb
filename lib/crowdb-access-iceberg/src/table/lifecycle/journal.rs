use super::{Phase, TableLifecycleOperation, TableLifecycleRequest, TableLifecycles};
use crate::{
    catalog::{check_context, CasOutcome, CatalogContext, CatalogError},
    error::ValidationError,
    key::{CatalogScope, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
};

impl TableLifecycles {
    /// # Errors
    /// Rejects corrupt journals, foreign activation epochs and retired contexts.
    pub async fn load(
        &self,
        context: CatalogContext,
        identity: OperationId,
    ) -> Result<Option<TableLifecycleOperation>, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::TableLifecycleOperation,
            suffix: identity.as_bytes().to_vec(),
        };
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::TableLifecycleOperation(operation) = StorageRecord::decode(&key, &value.bytes)?
        else {
            return Err(ValidationError::Record.into());
        };
        if operation.context != context {
            return Err(ValidationError::IdentityMismatch.into());
        }
        check_context(self.store.as_ref(), context).await?;
        Ok(Some(*operation))
    }

    pub(super) async fn match_request(
        &self,
        request: &TableLifecycleRequest,
        input: &[u8],
        operation: &TableLifecycleOperation,
    ) -> Result<(), CatalogError> {
        if operation.context != request.context
            || operation.identity != request.identity
            || operation.principal != request.principal
            || self.payloads.get(&operation.input).await? != input
        {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }

    pub(super) async fn begin(
        &self,
        request: &TableLifecycleRequest,
        input: &[u8],
        operation: TableLifecycleOperation,
    ) -> Result<(), CatalogError> {
        operation.validate()?;
        check_context(self.store.as_ref(), operation.context).await?;
        let key = operation.key().encode()?;
        let bytes = StorageRecord::TableLifecycleOperation(Box::new(operation.clone())).encode()?;
        match self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?
        {
            CasOutcome::Applied(_) => (),
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::TableLifecycleOperation(existing) =
                    StorageRecord::decode(&operation.key(), &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                self.match_request(request, input, &existing).await?;
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        }
        check_context(self.store.as_ref(), operation.context).await
    }

    pub(super) async fn current(&self, operation: &TableLifecycleOperation) -> Result<(), CatalogError> {
        if self
            .load(operation.context, operation.identity.operation)
            .await?
            .as_ref()
            != Some(operation)
        {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }

    pub(super) async fn advance(
        &self,
        previous: &TableLifecycleOperation,
        next: &TableLifecycleOperation,
    ) -> Result<(), CatalogError> {
        previous.validate()?;
        next.validate()?;
        let permitted = matches!(
            (previous.phase, next.phase),
            (
                Phase::Prepared | Phase::Admitting,
                Phase::Reserved | Phase::Publishing | Phase::Aborting
            ) | (Phase::Reserved, Phase::Admitting | Phase::Aborting)
                | (Phase::Publishing, Phase::Published | Phase::Aborting)
                | (Phase::Published, Phase::Complete)
                | (Phase::Aborting, Phase::Aborted)
        );
        let mut expected = previous.next(next.phase)?;
        if matches!(
            (previous.phase, next.phase),
            (Phase::Reserved, Phase::Admitting) | (Phase::Admitting, Phase::Reserved)
        ) {
            expected.admission.clone_from(&next.admission);
        }
        if matches!(next.phase, Phase::Complete | Phase::Aborting) {
            expected.outcome.clone_from(&next.outcome);
        }
        if !permitted || expected != *next {
            return Err(ValidationError::Record.into());
        }
        check_context(self.store.as_ref(), previous.context).await?;
        let key = previous.key().encode()?;
        let before = StorageRecord::TableLifecycleOperation(Box::new(previous.clone())).encode()?;
        let after = StorageRecord::TableLifecycleOperation(Box::new(next.clone())).encode()?;
        self.store
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await?;
        check_context(self.store.as_ref(), previous.context).await
    }
}
