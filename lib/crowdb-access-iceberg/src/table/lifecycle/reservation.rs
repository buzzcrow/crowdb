use super::{Phase, TableLifecycleOperation, TableLifecycles};
use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError},
    error::ValidationError,
    key::{NamespaceId, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
    table::{head_key, name_key, TableMapping, TableMappingState},
};

impl TableLifecycles {
    pub(super) async fn reserve(
        &self,
        operation: &TableLifecycleOperation,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        self.current(operation).await?;
        let mapping = operation.destination(TableMappingState::Reserved);
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?;
        let encoded = key.encode()?;
        let bytes = StorageRecord::TableMapping(mapping).encode()?;
        let result = self
            .names
            .compare_exchange(
                &encoded,
                None,
                &bytes,
                mutation_identity(&operation.key().encode()?, None, &bytes),
            )
            .await?;
        if matches!(result, CasOutcome::Applied(_))
            || matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == bytes)
        {
            return self.advance(operation, &operation.next(Phase::Reserved)?).await;
        }
        let CasOutcome::Conflict(Some(value)) = result else {
            return Err(CatalogError::Busy);
        };
        let StorageRecord::TableMapping(existing) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if existing.state == TableMappingState::Reserved {
            Box::pin(crate::commit::TableCreator::help_reservation(
                self.store.clone(),
                self.names.clone(),
                operation.context,
                &existing,
                budget,
            ))
            .await?;
            return Ok(());
        }
        let head_key = head_key(existing.catalog, existing.table);
        let head = self
            .store
            .get(&head_key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::TableHead(head) = StorageRecord::decode(&head_key, &head.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if existing.resolves(&head) {
            return self.abort(operation, 409, "AlreadyExistsException").await;
        }
        self.names
            .delete_mapping(
                &encoded,
                &value.bytes,
                mutation_identity(&encoded, Some(&value.bytes), &[]),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn help_reservation(
        &self,
        context: CatalogContext,
        mapping: &TableMapping,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let operation = self
            .load(context, mapping.operation)
            .await?
            .ok_or(ValidationError::Record)?;
        if !operation.is_rename() || operation.destination(TableMappingState::Reserved) != *mapping {
            return Err(ValidationError::IdentityMismatch.into());
        }
        self.resume_with_budget(context, mapping.operation, budget)
            .await?;
        Ok(())
    }

    pub(crate) async fn help_admission(
        &self,
        context: CatalogContext,
        holder: NamespaceId,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let operation = self
            .load(context, identity)
            .await?
            .ok_or(ValidationError::Record)?;
        if !operation.is_rename() || operation.candidate.namespace != holder || operation.admission.is_none()
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        self.resume_with_budget(context, identity, budget).await?;
        Ok(())
    }
}
