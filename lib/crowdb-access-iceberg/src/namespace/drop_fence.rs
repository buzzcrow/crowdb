use crate::catalog::{CasOutcome, CatalogError};
use crate::error::ValidationError;
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;

use super::update_recovery::next_phase;
use super::{
    authority_key, NamespaceAction, NamespaceAuthority, NamespaceDropper, NamespaceJournal,
    NamespaceLifecycle, NamespaceMutation, NamespaceOperation, NamespacePhase,
};

impl NamespaceDropper {
    pub(super) async fn prepare_fence(
        &self,
        operation: &NamespaceOperation,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let Some(authority) = self
            .creator
            .repository
            .load(operation.context, &operation.identifier)
            .await?
        else {
            return self.finish_outcome(operation, 404).await;
        };
        if authority.namespace != operation.namespace || authority.parent != operation.parent {
            return self.finish_outcome(operation, 404).await;
        }
        if let Some(pending) = authority.pending_operation {
            let owner = NamespaceJournal::new(self.creator.repository.store.clone())
                .load(operation.context, pending)
                .await?
                .ok_or(ValidationError::Record)?;
            if owner.action == NamespaceAction::Drop {
                if owner.namespace != operation.namespace {
                    return Err(ValidationError::IdentityMismatch.into());
                }
                if owner.identity.operation == operation.identity.operation {
                    return Err(ValidationError::Record.into());
                }
                Box::pin(self.resume_with_budget(operation.context, pending, budget)).await?;
            } else {
                self.creator
                    .help_marker(operation.context, operation.namespace, pending, budget)
                    .await?;
            }
            return Ok(());
        }
        if authority.lifecycle != NamespaceLifecycle::Ready {
            return Err(ValidationError::Record.into());
        }
        authority
            .admission_fence
            .checked_add(2)
            .ok_or(ValidationError::GenerationExhausted)?;
        authority
            .mutation_revision
            .checked_add(3)
            .ok_or(ValidationError::GenerationExhausted)?;
        let before = StorageRecord::NamespaceAuthority(Box::new(authority.clone())).encode()?;
        let after = Self::transition(authority, operation, NamespaceLifecycle::Dropping)?.encode()?;
        self.persist_mutation(operation, NamespacePhase::Fencing, &before, &after)
            .await
    }

    pub(super) async fn apply_fence(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let (before, after, expected) = self.mutation_bytes(operation).await?;
        if expected.lifecycle != NamespaceLifecycle::Ready
            || expected.pending_operation.is_some()
            || Self::transition(expected.clone(), operation, NamespaceLifecycle::Dropping)?.encode()? != after
        {
            return Err(ValidationError::Record.into());
        }
        let key = authority_key(operation.context.catalog, operation.namespace).encode()?;
        self.creator.repository.check_context(operation.context).await?;
        let result = self
            .creator
            .names
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await?;
        let journal = NamespaceJournal::new(self.creator.repository.store.clone());
        if matches!(&result, CasOutcome::Applied(_))
            || matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            journal
                .advance(
                    operation,
                    &next_phase(operation, NamespacePhase::ProbingNamespaces)?,
                )
                .await?;
            return Ok(());
        }
        let CasOutcome::Conflict(Some(value)) = result else {
            return Err(ValidationError::Record.into());
        };
        let StorageRecord::NamespaceAuthority(current) = StorageRecord::decode(
            &authority_key(operation.context.catalog, operation.namespace),
            &value.bytes,
        )?
        else {
            return Err(ValidationError::Record.into());
        };
        if current.mutation_revision <= expected.mutation_revision {
            return Err(ValidationError::Record.into());
        }
        if current.lifecycle == NamespaceLifecycle::Tombstone {
            return self.finish_outcome(operation, 404).await;
        }
        let mut next = next_phase(operation, NamespacePhase::Prepared)?;
        next.mutation = None;
        journal.advance(operation, &next).await?;
        Ok(())
    }

    pub(super) fn transition(
        mut authority: NamespaceAuthority,
        operation: &NamespaceOperation,
        lifecycle: NamespaceLifecycle,
    ) -> Result<StorageRecord, CatalogError> {
        authority.admission_fence = authority
            .admission_fence
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        authority.mutation_revision = authority
            .mutation_revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        authority.lifecycle = lifecycle;
        authority.pending_operation = Some(operation.identity.operation);
        Ok(StorageRecord::NamespaceAuthority(Box::new(authority)))
    }

    pub(super) async fn persist_mutation(
        &self,
        operation: &NamespaceOperation,
        phase: NamespacePhase,
        before: &[u8],
        after: &[u8],
    ) -> Result<(), CatalogError> {
        let payloads = PayloadStore::new(self.creator.repository.store.clone());
        let before = payloads
            .put(operation.context.catalog, operation.identity.operation, before)
            .await?;
        let after = payloads
            .put(operation.context.catalog, operation.identity.operation, after)
            .await?;
        let mut next = next_phase(operation, phase)?;
        next.scan_after.clear();
        next.scan_generation = 0;
        next.mutation = Some(NamespaceMutation {
            key: authority_key(operation.context.catalog, operation.namespace).encode()?,
            before: Some(before),
            after,
        });
        NamespaceJournal::new(self.creator.repository.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    pub(super) async fn mutation_bytes(
        &self,
        operation: &NamespaceOperation,
    ) -> Result<(Vec<u8>, Vec<u8>, NamespaceAuthority), CatalogError> {
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let key = authority_key(operation.context.catalog, operation.namespace);
        if mutation.key != key.encode()? {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let payloads = PayloadStore::new(self.creator.repository.store.clone());
        let before = payloads
            .get(mutation.before.as_ref().ok_or(ValidationError::Record)?)
            .await?;
        let after = payloads.get(&mutation.after).await?;
        let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if authority.identifier != operation.identifier || authority.parent != operation.parent {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok((before, after, *authority))
    }
}
