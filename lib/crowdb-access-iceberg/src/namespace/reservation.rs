use crate::catalog::{CasOutcome, CatalogError};
use crate::error::ValidationError;
use crate::operation::mutation_identity;
use crate::record::StorageRecord;

use super::update_recovery::next_phase;
use super::{
    name_key, NamespaceAction, NamespaceCreator, NamespaceJournal, NamespaceMapping, NamespaceMappingState,
    NamespaceOperation, NamespacePhase,
};

impl NamespaceCreator {
    pub(super) fn mapping(operation: &NamespaceOperation, state: NamespaceMappingState) -> NamespaceMapping {
        NamespaceMapping {
            catalog: operation.context.catalog,
            parent: operation.parent,
            name: operation.identifier.name().into(),
            namespace: operation.namespace,
            name_epoch: 1,
            operation: operation.identity.operation,
            state,
        }
    }

    pub(super) async fn reserve_name(
        &self,
        operation: &NamespaceOperation,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let key = name_key(
            operation.context.catalog,
            operation.parent,
            operation.identifier.name(),
        )?;
        let mapping = Self::mapping(operation, NamespaceMappingState::Reserved);
        let bytes = StorageRecord::NamespaceMapping(mapping.clone()).encode()?;
        let encoded = key.encode()?;
        let result = self
            .names
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await?;
        match result {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => {}
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::NamespaceMapping(existing) = StorageRecord::decode(&key, &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                if existing.state == NamespaceMappingState::Published {
                    if self
                        .repository
                        .load(operation.context, &operation.identifier)
                        .await?
                        .is_some()
                    {
                        return self.abort(operation, 409).await;
                    }
                    self.repair_published_mapping(operation, &existing, &value.bytes, budget)
                        .await?;
                    return Ok(());
                }
                let journal = NamespaceJournal::new(self.repository.store.clone());
                let owner = journal
                    .load(operation.context, existing.operation)
                    .await?
                    .ok_or(ValidationError::Record)?;
                if owner.action != NamespaceAction::Create
                    || owner.namespace != existing.namespace
                    || owner.parent != existing.parent
                    || owner.identifier != operation.identifier
                {
                    return Err(ValidationError::Record.into());
                }
                Box::pin(self.resume_with_budget(operation.context, existing.operation, budget)).await?;
                return Ok(());
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        }
        NamespaceJournal::new(self.repository.store.clone())
            .advance(operation, &next_phase(operation, NamespacePhase::Reserved)?)
            .await?;
        Ok(())
    }

    pub(super) async fn remove_reservation(
        &self,
        operation: &NamespaceOperation,
    ) -> Result<(), CatalogError> {
        if !matches!(
            operation.phase,
            NamespacePhase::Aborting | NamespacePhase::Aborted
        ) || operation.outcome.is_none()
        {
            return Err(ValidationError::Record.into());
        }
        let key = name_key(
            operation.context.catalog,
            operation.parent,
            operation.identifier.name(),
        )?
        .encode()?;
        let bytes =
            StorageRecord::NamespaceMapping(Self::mapping(operation, NamespaceMappingState::Reserved))
                .encode()?;
        self.repository.check_context(operation.context).await?;
        if self
            .names
            .get(&key)
            .await?
            .as_ref()
            .map(|value| value.bytes.as_slice())
            != Some(bytes.as_slice())
        {
            return Ok(());
        }
        self.names
            .delete_mapping(&key, &bytes, mutation_identity(&key, Some(&bytes), &[]))
            .await?;
        Ok(())
    }

    async fn repair_published_mapping(
        &self,
        operation: &NamespaceOperation,
        mapping: &NamespaceMapping,
        bytes: &[u8],
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let key = super::authority_key(operation.context.catalog, mapping.namespace);
        if let Some(value) = self.names.get(&key.encode()?).await? {
            let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &value.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            if mapping.resolves(&authority) {
                return Err(CatalogError::Busy);
            }
            if authority.lifecycle == super::NamespaceLifecycle::Tombstone {
                self.help_marker(
                    operation.context,
                    authority.namespace,
                    authority.pending_operation.ok_or(ValidationError::Record)?,
                    budget,
                )
                .await?;
            }
        }
        let key = name_key(mapping.catalog, mapping.parent, &mapping.name)?.encode()?;
        self.names
            .delete_mapping(&key, bytes, mutation_identity(&key, Some(bytes), &[]))
            .await?;
        Ok(())
    }
}
