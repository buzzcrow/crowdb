use crate::catalog::{CasOutcome, CatalogError};
use crate::error::ValidationError;
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;

use super::update_recovery::next_phase;
use super::{
    authority_key, name_key, NamespaceCreator, NamespaceJournal, NamespaceMappingState, NamespaceMutation,
    NamespaceOperation, NamespaceOutcome, NamespacePhase,
};

impl NamespaceCreator {
    pub(super) async fn prepare_publication(
        &self,
        operation: &NamespaceOperation,
    ) -> Result<(), CatalogError> {
        self.release_admission(operation).await?;
        let properties = self.properties(operation).await?;
        let bytes = Self::initial_authority(operation, properties.clone()).encode()?;
        let payloads = PayloadStore::new(self.repository.store.clone());
        let after = payloads
            .put(operation.context.catalog, operation.identity.operation, &bytes)
            .await?;
        payloads
            .put(
                operation.context.catalog,
                operation.identity.operation,
                &Self::response(operation, &properties)?,
            )
            .await?;
        let mut next = next_phase(operation, NamespacePhase::Publishing)?;
        next.mutation = Some(NamespaceMutation {
            key: authority_key(operation.context.catalog, operation.namespace).encode()?,
            before: None,
            after,
        });
        NamespaceJournal::new(self.repository.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    pub(super) async fn publish(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let bytes = Self::initial_authority(operation, self.properties(operation).await?).encode()?;
        if mutation.before.is_some()
            || mutation.key != authority_key(operation.context.catalog, operation.namespace).encode()?
            || PayloadStore::new(self.repository.store.clone())
                .get(&mutation.after)
                .await?
                != bytes
        {
            return Err(ValidationError::Record.into());
        }
        self.repository.check_context(operation.context).await?;
        let result = self
            .names
            .compare_exchange(
                &mutation.key,
                None,
                &bytes,
                mutation_identity(&mutation.key, None, &bytes),
            )
            .await?;
        if !matches!(&result, CasOutcome::Applied(_))
            && !matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == bytes)
        {
            return self.changed_publication(operation).await;
        }
        let key = name_key(
            operation.context.catalog,
            operation.parent,
            operation.identifier.name(),
        )?
        .encode()?;
        let before =
            StorageRecord::NamespaceMapping(Self::mapping(operation, NamespaceMappingState::Reserved))
                .encode()?;
        let after =
            StorageRecord::NamespaceMapping(Self::mapping(operation, NamespaceMappingState::Published))
                .encode()?;
        let result = self
            .names
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await?;
        if !matches!(&result, CasOutcome::Applied(_))
            && !matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            return self.changed_publication(operation).await;
        }
        NamespaceJournal::new(self.repository.store.clone())
            .advance(operation, &next_phase(operation, NamespacePhase::Published)?)
            .await?;
        Ok(())
    }

    async fn changed_publication(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let current = NamespaceJournal::new(self.repository.store.clone())
            .load(operation.context, operation.identity.operation)
            .await?
            .ok_or(ValidationError::Record)?;
        if matches!(
            current.phase,
            NamespacePhase::Published | NamespacePhase::Complete
        ) {
            return Ok(());
        }
        Err(ValidationError::Record.into())
    }

    pub(super) async fn complete(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let response = Self::response(operation, &self.properties(operation).await?)?;
        let body = PayloadStore::new(self.repository.store.clone())
            .put(operation.context.catalog, operation.identity.operation, &response)
            .await?;
        let mut next = next_phase(operation, NamespacePhase::Complete)?;
        next.outcome = Some(NamespaceOutcome { status: 200, body });
        NamespaceJournal::new(self.repository.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    pub(super) async fn release_created_authority(
        &self,
        operation: &NamespaceOperation,
    ) -> Result<(), CatalogError> {
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let before = Self::initial_authority(operation, self.properties(operation).await?).encode()?;
        let key = authority_key(operation.context.catalog, operation.namespace);
        if mutation.key != key.encode()? {
            return Err(ValidationError::Record.into());
        }
        let StorageRecord::NamespaceAuthority(mut authority) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        authority.pending_operation = None;
        authority.mutation_revision += 1;
        let after = StorageRecord::NamespaceAuthority(authority).encode()?;
        self.names
            .compare_exchange(
                &mutation.key,
                Some(&before),
                &after,
                mutation_identity(&mutation.key, Some(&before), &after),
            )
            .await?;
        Ok(())
    }
}
