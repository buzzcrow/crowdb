use crate::catalog::{CasOutcome, CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::operation::{PayloadStore, RequestIdentity};
use crate::record::StorageRecord;

use super::{
    NamespaceAuthority, NamespaceIdentifier, NamespaceJournal, NamespaceLifecycle, NamespaceMutation,
    NamespaceOperation, NamespacePhase, NamespaceRepository, PropertyChanges,
};

#[derive(Clone, Debug)]
pub struct NamespacePropertyRequest {
    pub context: CatalogContext,
    pub identifier: NamespaceIdentifier,
    pub changes: PropertyChanges,
}

impl NamespaceRepository {
    /// Applies one independent set of property changes to the current authority.
    /// Repeating a removal reports its current missing state rather than replaying
    /// an earlier response. No operation or response record is persisted.
    /// # Errors
    /// Rejects invalid changes, retired contexts, deletion races and storage failures.
    pub async fn update_properties(
        &self,
        request: &NamespacePropertyRequest,
    ) -> Result<Option<super::PropertyUpdate>, CatalogError> {
        request.changes.validate()?;
        let Some(mut authority) = self
            .settle_property_target(request.context, &request.identifier)
            .await?
        else {
            return Ok(None);
        };
        let key = super::authority_key(authority.catalog, authority.namespace);
        let key_bytes = key.encode()?;
        for _ in 0..16 {
            if authority.lifecycle != NamespaceLifecycle::Ready {
                return Err(CatalogError::Conflict);
            }
            if authority.pending_operation.is_some() {
                return Err(CatalogError::Busy);
            }
            let update = authority.properties.apply(&request.changes)?;
            if update.properties == authority.properties {
                self.check_context(request.context).await?;
                return Ok(Some(update));
            }
            let expected = StorageRecord::NamespaceAuthority(Box::new(authority.clone())).encode()?;
            let mut next = authority.clone();
            next.properties = update.properties.clone();
            next.property_revision = next
                .property_revision
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
            next.mutation_revision = next
                .mutation_revision
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
            let value = StorageRecord::NamespaceAuthority(Box::new(next)).encode()?;
            match self
                .store
                .compare_exchange(
                    &key_bytes,
                    Some(&expected),
                    &value,
                    crate::operation::mutation_identity(&key_bytes, Some(&expected), &value),
                )
                .await?
            {
                CasOutcome::Applied(_) => {
                    self.check_context(request.context).await?;
                    return Ok(Some(update));
                }
                CasOutcome::Conflict(Some(observed)) => {
                    let StorageRecord::NamespaceAuthority(observed) =
                        StorageRecord::decode(&key, &observed.bytes)?
                    else {
                        return Err(ValidationError::Record.into());
                    };
                    if observed.identifier != authority.identifier
                        || observed.name_epoch != authority.name_epoch
                        || observed.parent != authority.parent
                    {
                        return Err(CatalogError::Conflict);
                    }
                    authority = *observed;
                }
                CasOutcome::Conflict(None) => return Ok(None),
            }
        }
        Err(CatalogError::Busy)
    }

    pub(super) fn prepare_property_bytes(
        authority: &NamespaceAuthority,
        changes: &PropertyChanges,
        identity: RequestIdentity,
    ) -> Result<(Vec<u8>, Vec<u8>), CatalogError> {
        if authority.lifecycle != NamespaceLifecycle::Ready || authority.pending_operation.is_some() {
            return Err(CatalogError::Busy);
        }
        authority
            .mutation_revision
            .checked_add(2)
            .ok_or(ValidationError::GenerationExhausted)?;
        let update = authority.properties.apply(changes)?;
        let response = serde_json::to_vec(&serde_json::json!({
            "removed": update.removed,
            "updated": update.updated,
            "missing": update.missing,
        }))
        .map_err(|_| ValidationError::Record)?;
        if response.len() > crate::operation::MAX_PAYLOAD_BYTES {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let mut next = authority.clone();
        next.properties = update.properties;
        next.property_revision = next
            .property_revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        next.mutation_revision = next
            .mutation_revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        next.pending_operation = Some(identity.operation);
        Ok((
            StorageRecord::NamespaceAuthority(Box::new(next)).encode()?,
            response,
        ))
    }

    pub(super) async fn prepare_property_mutation(
        &self,
        operation: &NamespaceOperation,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let Some(authority) = self
            .settle_property_target_with_budget(operation.context, &operation.identifier, budget)
            .await?
        else {
            return self.finish_property_error(operation, 404).await;
        };
        if authority.namespace != operation.namespace || authority.parent != operation.parent {
            return self.finish_property_error(operation, 404).await;
        }
        let payloads = PayloadStore::new(self.store.clone());
        let changes: PropertyChanges = serde_json::from_slice(&payloads.get(&operation.input).await?)
            .map_err(|_| ValidationError::Record)?;
        let (after, response) = match Self::prepare_property_bytes(&authority, &changes, operation.identity) {
            Ok(bytes) => bytes,
            Err(CatalogError::Invalid(
                ValidationError::RecordTooLarge | ValidationError::GenerationExhausted,
            )) => {
                return self.finish_property_error(operation, 400).await;
            }
            Err(error) => return Err(error),
        };
        let before = StorageRecord::NamespaceAuthority(Box::new(authority)).encode()?;
        let mut next = super::update_recovery::next_phase(operation, NamespacePhase::Publishing)?;
        let before = payloads
            .put(operation.context.catalog, operation.identity.operation, &before)
            .await?;
        let after = payloads
            .put(operation.context.catalog, operation.identity.operation, &after)
            .await?;
        payloads
            .put(operation.context.catalog, operation.identity.operation, &response)
            .await?;
        next.mutation = Some(NamespaceMutation {
            key: super::authority_key(operation.context.catalog, operation.namespace).encode()?,
            before: Some(before),
            after,
        });
        NamespaceJournal::new(self.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    async fn settle_property_target(
        &self,
        context: CatalogContext,
        identifier: &NamespaceIdentifier,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        self.settle_property_target_with_budget(context, identifier, &mut 16)
            .await
    }

    async fn settle_property_target_with_budget(
        &self,
        context: CatalogContext,
        identifier: &NamespaceIdentifier,
        budget: &mut usize,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        let selected = self.load(context, identifier).await?;
        let Some(pending) = selected
            .as_ref()
            .and_then(|authority| authority.pending_operation)
        else {
            return Ok(selected);
        };
        let authority = selected.as_ref().ok_or(ValidationError::Record)?;
        let creator = super::NamespaceCreator {
            repository: self.clone(),
            names: self.names.clone(),
        };
        Box::pin(creator.help_marker(context, authority.namespace, pending, budget)).await?;
        self.load(context, identifier).await
    }
}
