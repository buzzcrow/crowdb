use crate::catalog::{CasOutcome, CatalogError, RootState};
use crate::error::ValidationError;
use crate::key::{IcebergKey, SystemScope};
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;

use super::update_recovery::next_phase;
use super::{
    authority_key, NamespaceCreator, NamespaceJournal, NamespaceLifecycle, NamespaceMutation,
    NamespaceOperation, NamespacePhase,
};

impl NamespaceCreator {
    pub(super) async fn prepare_admission(
        &self,
        operation: &NamespaceOperation,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let key = operation.parent.map_or_else(
            || IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                suffix: Vec::new(),
            },
            |parent| authority_key(operation.context.catalog, parent),
        );
        let Some(value) = self.names.get(&key.encode()?).await? else {
            return self.abort(operation, 400).await;
        };
        let after = match StorageRecord::decode(&key, &value.bytes)? {
            StorageRecord::NamespaceAuthority(mut parent) => {
                if parent.lifecycle != NamespaceLifecycle::Ready
                    || Some(parent.identifier.clone()) != operation.identifier.parent()
                {
                    return self.abort(operation, 400).await;
                }
                if let Some(pending) = parent.pending_operation {
                    self.help_marker(operation.context, parent.namespace, pending, budget)
                        .await?;
                    return Ok(());
                }
                parent
                    .mutation_revision
                    .checked_add(2)
                    .ok_or(ValidationError::GenerationExhausted)?;
                parent.mutation_revision += 1;
                parent.pending_operation = Some(operation.identity.operation);
                StorageRecord::NamespaceAuthority(parent).encode()?
            }
            StorageRecord::Active(root)
                if root.context == operation.context && root.state == RootState::Ready =>
            {
                value.bytes.clone()
            }
            StorageRecord::Active(_) => return Err(CatalogError::Conflict),
            _ => return Err(ValidationError::Record.into()),
        };
        let payloads = PayloadStore::new(self.repository.store.clone());
        let before = payloads
            .put(
                operation.context.catalog,
                operation.identity.operation,
                &value.bytes,
            )
            .await?;
        let after = payloads
            .put(operation.context.catalog, operation.identity.operation, &after)
            .await?;
        let mut next = next_phase(operation, NamespacePhase::Admitting)?;
        next.mutation = Some(NamespaceMutation {
            key: key.encode()?,
            before: Some(before),
            after,
        });
        NamespaceJournal::new(self.repository.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    pub(super) async fn finish_admission(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let payloads = PayloadStore::new(self.repository.store.clone());
        let before = payloads
            .get(mutation.before.as_ref().ok_or(ValidationError::Record)?)
            .await?;
        let after = payloads.get(&mutation.after).await?;
        Self::validate_admission(operation, &before, &after)?;
        self.repository.check_context(operation.context).await?;
        let result = self
            .names
            .compare_exchange(
                &mutation.key,
                Some(&before),
                &after,
                mutation_identity(&operation.key().encode()?, Some(&before), &after),
            )
            .await?;
        if matches!(&result, CasOutcome::Applied(_))
            || matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            NamespaceJournal::new(self.repository.store.clone())
                .advance(operation, &next_phase(operation, NamespacePhase::Admitted)?)
                .await?;
            return Ok(());
        }
        let CasOutcome::Conflict(Some(value)) = result else {
            return Err(ValidationError::Record.into());
        };
        let key = IcebergKey::decode(&mutation.key)?;
        match StorageRecord::decode(&key, &value.bytes)? {
            StorageRecord::NamespaceAuthority(current) => {
                let StorageRecord::NamespaceAuthority(previous) = StorageRecord::decode(&key, &before)?
                else {
                    return Err(ValidationError::Record.into());
                };
                if current.mutation_revision <= previous.mutation_revision {
                    return Err(ValidationError::Record.into());
                }
                if current.lifecycle != NamespaceLifecycle::Ready
                    || Some(current.identifier.clone()) != operation.identifier.parent()
                {
                    return self.abort(operation, 400).await;
                }
            }
            StorageRecord::Active(root)
                if root.context == operation.context && root.state == RootState::Ready => {}
            StorageRecord::Active(_) => return Err(CatalogError::Conflict),
            _ => return Err(ValidationError::Record.into()),
        }
        let mut next = next_phase(operation, NamespacePhase::Reserved)?;
        next.mutation = None;
        NamespaceJournal::new(self.repository.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    fn validate_admission(
        operation: &NamespaceOperation,
        before: &[u8],
        after: &[u8],
    ) -> Result<(), CatalogError> {
        let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
        let key = operation.parent.map_or_else(
            || IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                suffix: Vec::new(),
            },
            |parent| authority_key(operation.context.catalog, parent),
        );
        if mutation.key != key.encode()? {
            return Err(ValidationError::IdentityMismatch.into());
        }
        match StorageRecord::decode(&key, before)? {
            StorageRecord::NamespaceAuthority(mut parent) => {
                if parent.lifecycle != NamespaceLifecycle::Ready
                    || parent.pending_operation.is_some()
                    || Some(parent.identifier.clone()) != operation.identifier.parent()
                {
                    return Err(ValidationError::Record.into());
                }
                parent.mutation_revision = parent
                    .mutation_revision
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
                parent.pending_operation = Some(operation.identity.operation);
                if StorageRecord::NamespaceAuthority(parent).encode()? != after {
                    return Err(ValidationError::Record.into());
                }
            }
            StorageRecord::Active(root)
                if root.context == operation.context && root.state == RootState::Ready && before == after => {
            }
            _ => return Err(ValidationError::Record.into()),
        }
        Ok(())
    }

    pub(super) async fn release_admission(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let Some(parent) = operation.parent else {
            return Ok(());
        };
        let Some(mutation) = &operation.mutation else {
            return Ok(());
        };
        let key = authority_key(operation.context.catalog, parent);
        if mutation.key != key.encode()? {
            return Err(ValidationError::Record.into());
        }
        let before = PayloadStore::new(self.repository.store.clone())
            .get(&mutation.after)
            .await?;
        let StorageRecord::NamespaceAuthority(mut authority) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if authority.pending_operation != Some(operation.identity.operation) {
            return Err(ValidationError::Record.into());
        }
        authority.pending_operation = None;
        authority.mutation_revision = authority
            .mutation_revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        let after = StorageRecord::NamespaceAuthority(authority).encode()?;
        self.repository
            .cleanup_marker(&mutation.key, &before, &after)
            .await?;
        Ok(())
    }
}
