use crate::catalog::{CasOutcome, CatalogError};
use crate::error::ValidationError;
use crate::operation::{mutation_identity, PayloadStore};
use crate::record::StorageRecord;

use super::update_recovery::next_phase;
use super::{
    authority_key, name_key, NamespaceDropper, NamespaceJournal, NamespaceLifecycle, NamespaceOperation,
    NamespaceOutcome, NamespacePhase,
};

impl NamespaceDropper {
    pub(super) async fn apply_finish(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let (before, after, authority) = self.mutation_bytes(operation).await?;
        let lifecycle = if operation.phase == NamespacePhase::Restoring {
            NamespaceLifecycle::Ready
        } else {
            NamespaceLifecycle::Tombstone
        };
        if authority.lifecycle != NamespaceLifecycle::Dropping
            || authority.pending_operation != Some(operation.identity.operation)
            || Self::transition(authority, operation, lifecycle)?.encode()? != after
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
        if !matches!(&result, CasOutcome::Applied(_))
            && !matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            let current = NamespaceJournal::new(self.creator.repository.store.clone())
                .load(operation.context, operation.identity.operation)
                .await?
                .ok_or(ValidationError::Record)?;
            if current.phase == NamespacePhase::Complete {
                return Ok(());
            }
            return Err(ValidationError::Record.into());
        }
        self.finish_outcome(
            operation,
            if lifecycle == NamespaceLifecycle::Ready {
                409
            } else {
                204
            },
        )
        .await
    }

    pub(super) async fn finish_outcome(
        &self,
        operation: &NamespaceOperation,
        status: u16,
    ) -> Result<(), CatalogError> {
        let bytes = match status {
            204 => Vec::new(),
            404 | 409 => serde_json::to_vec(&serde_json::json!({
                "error": {
                    "code": status,
                    "type": if status == 404 { "NoSuchNamespaceException" } else { "NamespaceNotEmptyException" },
                    "message": if status == 404 { "Namespace does not exist" } else { "Namespace has children" }
                }
            })).map_err(|_| ValidationError::Record)?,
            _ => return Err(ValidationError::Record.into()),
        };
        let body = PayloadStore::new(self.creator.repository.store.clone())
            .put(operation.context.catalog, operation.identity.operation, &bytes)
            .await?;
        let mut next = next_phase(operation, NamespacePhase::Complete)?;
        next.outcome = Some(NamespaceOutcome { status, body });
        NamespaceJournal::new(self.creator.repository.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    pub(super) async fn cleanup(&self, operation: &NamespaceOperation) -> Result<(), CatalogError> {
        let outcome = operation.outcome.as_ref().ok_or(ValidationError::Record)?;
        match outcome.status {
            204 => {
                let key = name_key(
                    operation.context.catalog,
                    operation.parent,
                    operation.identifier.name(),
                )?;
                let encoded = key.encode()?;
                if let Some(value) = self.creator.names.get(&encoded).await? {
                    let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, &value.bytes)?
                    else {
                        return Err(ValidationError::Record.into());
                    };
                    if mapping.namespace == operation.namespace {
                        self.creator
                            .names
                            .delete_mapping(
                                &encoded,
                                &value.bytes,
                                mutation_identity(&encoded, Some(&value.bytes), &[]),
                            )
                            .await?;
                    }
                }
            }
            409 => {
                let mutation = operation.mutation.as_ref().ok_or(ValidationError::Record)?;
                let before = PayloadStore::new(self.creator.repository.store.clone())
                    .get(&mutation.after)
                    .await?;
                let key = authority_key(operation.context.catalog, operation.namespace);
                let StorageRecord::NamespaceAuthority(mut authority) = StorageRecord::decode(&key, &before)?
                else {
                    return Err(ValidationError::Record.into());
                };
                if authority.lifecycle != NamespaceLifecycle::Ready
                    || authority.pending_operation != Some(operation.identity.operation)
                {
                    return Err(ValidationError::Record.into());
                }
                authority.pending_operation = None;
                authority.mutation_revision = authority
                    .mutation_revision
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
                let after = StorageRecord::NamespaceAuthority(authority).encode()?;
                let key = key.encode()?;
                self.creator
                    .repository
                    .cleanup_marker(&key, &before, &after)
                    .await?;
            }
            404 => {}
            _ => return Err(ValidationError::Record.into()),
        }
        Ok(())
    }
}
