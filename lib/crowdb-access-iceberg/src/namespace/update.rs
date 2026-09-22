use crate::catalog::{CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::operation::{PayloadStore, RequestIdentity};
use crate::record::StorageRecord;

use super::{
    NamespaceAction, NamespaceAuthority, NamespaceIdentifier, NamespaceJournal, NamespaceLifecycle,
    NamespaceMutation, NamespaceOperation, NamespaceOutcome, NamespacePhase, NamespaceRepository,
    PropertyChanges,
};

#[derive(Clone, Debug)]
pub struct NamespacePropertyRequest {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub identifier: NamespaceIdentifier,
    pub changes: PropertyChanges,
}

impl NamespaceRepository {
    /// # Errors
    /// Rejects invalid changes, changed retry input, retired contexts and storage failures.
    /// Returns no outcome if the namespace is absent before an operation is installed.
    pub async fn update_properties(
        &self,
        request: &NamespacePropertyRequest,
    ) -> Result<Option<NamespaceOutcome>, CatalogError> {
        request.changes.validate()?;
        if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
            return Err(ValidationError::Text.into());
        }
        let input = serde_json::to_vec(&request.changes).map_err(|_| ValidationError::Record)?;
        let journal = NamespaceJournal::new(self.store.clone());
        if let Some(existing) = journal.load(request.context, request.identity.operation).await? {
            if existing.action != NamespaceAction::Update
                || existing.identity != request.identity
                || existing.principal != request.principal
                || existing.identifier != request.identifier
                || PayloadStore::new(self.store.clone()).get(&existing.input).await? != input
            {
                return Err(CatalogError::Conflict);
            }
            if existing.phase == NamespacePhase::Prepared {
                self.settle_property_target(request.context, &request.identifier)
                    .await?;
            }
            return self
                .resume_property_update(request.context, request.identity.operation)
                .await
                .map(Some);
        }
        let Some(authority) = self
            .settle_property_target(request.context, &request.identifier)
            .await?
        else {
            return Ok(None);
        };
        Self::prepare_property_bytes(&authority, &request.changes, request.identity)?;
        let input = PayloadStore::new(self.store.clone())
            .put(request.context.catalog, request.identity.operation, &input)
            .await?;
        journal
            .begin(NamespaceOperation {
                context: request.context,
                identity: request.identity,
                principal: request.principal.clone(),
                action: NamespaceAction::Update,
                identifier: request.identifier.clone(),
                namespace: authority.namespace,
                parent: authority.parent,
                phase: NamespacePhase::Prepared,
                revision: 1,
                input,
                mutation: None,
                scan_after: Vec::new(),
                scan_generation: 0,
                outcome: None,
            })
            .await?;
        self.resume_property_update(request.context, request.identity.operation)
            .await
            .map(Some)
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
