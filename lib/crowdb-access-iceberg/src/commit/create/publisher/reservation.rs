use super::{Error, Phase, TableCreateOperation, TableCreator};
use crate::{
    catalog::{CasOutcome, CatalogError},
    commit::TableCommitOutcome,
    error::ValidationError,
    operation::mutation_identity,
    record::StorageRecord,
    table::{head_key, name_key, TableMappingState},
};

impl TableCreator {
    pub(super) async fn reserve(
        &self,
        operation: &TableCreateOperation,
        budget: &mut usize,
    ) -> Result<(), Error> {
        self.current(operation).await?;
        let mapping = operation.mapping(TableMappingState::Reserved);
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?;
        let encoded = key.encode()?;
        let bytes = StorageRecord::TableMapping(mapping.clone()).encode()?;
        let result = self
            .names
            .compare_exchange(
                &encoded,
                None,
                &bytes,
                mutation_identity(&operation.key().encode()?, None, &bytes),
            )
            .await
            .map_err(CatalogError::from)?;
        if matches!(result, CasOutcome::Applied(_))
            || matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == bytes)
        {
            self.journal()
                .advance(operation, &operation.next(Phase::Reserved)?)
                .await?;
            return Ok(());
        }
        let CasOutcome::Conflict(Some(value)) = result else {
            return Err(CatalogError::Busy.into());
        };
        let StorageRecord::TableMapping(existing) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if existing.state == TableMappingState::Reserved {
            let owner = self.journal().load(operation.context, existing.operation).await?;
            let Some(owner) = owner else {
                Box::pin(
                    crate::table::TableLifecycles::from_parts(self.store.clone(), self.names.clone())
                        .help_reservation(operation.context, &existing, budget),
                )
                .await?;
                return Ok(());
            };
            if owner.mapping(TableMappingState::Reserved) != existing {
                return Err(ValidationError::IdentityMismatch.into());
            }
            Box::pin(self.resume_with_budget(operation.context, existing.operation, budget)).await?;
            return Ok(());
        }
        let head_key = head_key(existing.catalog, existing.table);
        let value_head = self
            .store
            .get(&head_key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .ok_or(ValidationError::Record)?;
        let StorageRecord::TableHead(head) = StorageRecord::decode(&head_key, &value_head.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if existing.resolves(&head) {
            return self.abort(operation, 409).await;
        }
        self.names
            .delete_mapping(
                &encoded,
                &value.bytes,
                mutation_identity(&encoded, Some(&value.bytes), &[]),
            )
            .await
            .map_err(CatalogError::from)?;
        Ok(())
    }

    pub(super) async fn abort(&self, operation: &TableCreateOperation, status: u16) -> Result<(), Error> {
        let (kind, message) = match status {
            400 => (
                "BadRequestException",
                "Initial table files fail selected-use validation",
            ),
            404 => (
                "NoSuchNamespaceException",
                "Namespace is not available for table admission",
            ),
            409 => ("AlreadyExistsException", "Table already exists"),
            _ => return Err(ValidationError::Record.into()),
        };
        let mut next = operation.next(Phase::Aborting)?;
        let bytes =
            serde_json::to_vec(&serde_json::json!({"error":{"code":status,"type":kind,"message":message}}))
                .map_err(|_| ValidationError::Record)?;
        let body = self
            .payloads()
            .put(operation.context.catalog, operation.identity.operation, &bytes)
            .await?;
        next.outcome = Some(TableCommitOutcome { status, body });
        self.journal().advance(operation, &next).await?;
        Ok(())
    }
}
