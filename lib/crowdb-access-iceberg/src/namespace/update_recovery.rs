use crate::catalog::{CasOutcome, CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::key::OperationId;
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;

use super::{
    NamespaceAction, NamespaceJournal, NamespaceOperation, NamespaceOutcome, NamespacePhase,
    NamespaceRepository, PropertyChanges,
};

impl NamespaceRepository {
    /// # Errors
    /// Rejects foreign operations, corrupt snapshots and retired contexts.
    /// Returns busy when bounded recovery cannot complete this invocation.
    pub async fn resume_property_update(
        &self,
        context: CatalogContext,
        identity: OperationId,
    ) -> Result<NamespaceOutcome, CatalogError> {
        self.resume_property_with_budget(context, identity, &mut 8).await
    }

    pub(super) async fn resume_property_with_budget(
        &self,
        context: CatalogContext,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<NamespaceOutcome, CatalogError> {
        let journal = NamespaceJournal::new(self.store.clone());
        while *budget > 0 {
            *budget -= 1;
            let operation = journal
                .load(context, identity)
                .await?
                .ok_or(ValidationError::Record)?;
            if operation.action != NamespaceAction::Update {
                return Err(ValidationError::Record.into());
            }
            match operation.phase {
                NamespacePhase::Prepared => self.prepare_property_mutation(&operation, budget).await?,
                NamespacePhase::Publishing => self.publish_property_mutation(&operation).await?,
                NamespacePhase::Published => self.finish_property_success(&operation).await?,
                NamespacePhase::Complete | NamespacePhase::Aborted => {
                    self.release_property_marker(&operation).await?;
                    self.check_context(context).await?;
                    return operation.outcome.ok_or_else(|| ValidationError::Record.into());
                }
                _ => return Err(ValidationError::Record.into()),
            }
        }
        Err(CatalogError::Busy)
    }

    async fn publish_property_mutation(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let payloads = PayloadStore::new(self.store.clone());
        let before = payloads
            .get(mutation.before.as_ref().ok_or(ValidationError::Record)?)
            .await?;
        let after = payloads.get(&mutation.after).await?;
        let key = super::authority_key(operation.context.catalog, operation.namespace);
        let StorageRecord::NamespaceAuthority(expected) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        let changes: PropertyChanges = serde_json::from_slice(&payloads.get(&operation.input).await?)
            .map_err(|_| ValidationError::Record)?;
        if mutation.key != key.encode()?
            || expected.identifier != operation.identifier
            || expected.parent != operation.parent
            || Self::prepare_property_bytes(&expected, &changes, operation.identity)?.0 != after
        {
            return Err(ValidationError::Record.into());
        }
        self.check_context(operation.context).await?;
        let outcome = self
            .store
            .compare_exchange(
                &mutation.key,
                Some(&before),
                &after,
                mutation_identity(&mutation.key, Some(&before), &after),
            )
            .await?;
        if matches!(&outcome, CasOutcome::Applied(_))
            || matches!(&outcome, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            NamespaceJournal::new(self.store.clone())
                .advance(operation, &next_phase(operation, NamespacePhase::Published)?)
                .await?;
            return Ok(());
        }
        let CasOutcome::Conflict(Some(value)) = outcome else {
            return Err(ValidationError::Record.into());
        };
        let StorageRecord::NamespaceAuthority(current) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if current.mutation_revision <= expected.mutation_revision
            || current.pending_operation == Some(operation.identity.operation)
        {
            return Err(ValidationError::Record.into());
        }
        let mut next = next_phase(operation, NamespacePhase::Prepared)?;
        next.mutation = None;
        NamespaceJournal::new(self.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    async fn finish_property_success(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let payloads = PayloadStore::new(self.store.clone());
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let before = payloads
            .get(mutation.before.as_ref().ok_or(ValidationError::Record)?)
            .await?;
        let key = super::authority_key(operation.context.catalog, operation.namespace);
        let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        let changes: PropertyChanges = serde_json::from_slice(&payloads.get(&operation.input).await?)
            .map_err(|_| ValidationError::Record)?;
        let (_, response) = Self::prepare_property_bytes(&authority, &changes, operation.identity)?;
        self.finish_property_outcome(operation, 200, &response).await
    }

    pub(super) async fn finish_property_error(
        &self,
        operation: &NamespaceOperation,
        status: u16,
    ) -> Result<(), CatalogError> {
        let (kind, message) = match status {
            400 => (
                "BadRequestException",
                "Namespace properties or revision exceed supported limits",
            ),
            404 => ("NoSuchNamespaceException", "Namespace does not exist"),
            _ => return Err(ValidationError::Record.into()),
        };
        let response = serde_json::to_vec(&serde_json::json!({
            "error": { "message": message, "type": kind, "code": status }
        }))
        .map_err(|_| ValidationError::Record)?;
        self.finish_property_outcome(operation, status, &response).await
    }

    async fn finish_property_outcome(
        &self,
        operation: &NamespaceOperation,
        status: u16,
        response: &[u8],
    ) -> Result<(), CatalogError> {
        let body = PayloadStore::new(self.store.clone())
            .put(operation.context.catalog, operation.identity.operation, response)
            .await?;
        let mut next = next_phase(operation, NamespacePhase::Complete)?;
        next.outcome = Some(NamespaceOutcome { status, body });
        NamespaceJournal::new(self.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    async fn release_property_marker(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        if operation
            .outcome
            .as_ref()
            .map_or(true, |outcome| outcome.status != 200)
        {
            return Ok(());
        }
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let before = PayloadStore::new(self.store.clone()).get(&mutation.after).await?;
        let key = super::authority_key(operation.context.catalog, operation.namespace);
        let StorageRecord::NamespaceAuthority(mut authority) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if authority.pending_operation != Some(operation.identity.operation)
            || mutation.key != key.encode()?
        {
            return Err(ValidationError::Record.into());
        }
        authority.pending_operation = None;
        authority.mutation_revision = authority
            .mutation_revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        let after = StorageRecord::NamespaceAuthority(authority).encode()?;
        self.check_context(operation.context).await?;
        self.store
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

pub(super) fn next_phase(
    operation: &NamespaceOperation,
    phase: NamespacePhase,
) -> Result<NamespaceOperation, CatalogError> {
    Ok(NamespaceOperation {
        phase,
        revision: operation
            .revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?,
        ..operation.clone()
    })
}
