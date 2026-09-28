use super::{Phase, TableLifecycleOperation, TableLifecycles};
use crate::{
    catalog::{CasOutcome, CatalogError},
    error::ValidationError,
    namespace::{authority_key, NamespaceCreator, NamespaceLifecycle, NamespaceMutation},
    operation::mutation_identity,
    record::StorageRecord,
};

impl TableLifecycles {
    pub(super) async fn prepare_admission(
        &self,
        operation: &TableLifecycleOperation,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return self.abort(operation, 404, "NoSuchNamespaceException").await;
        };
        let StorageRecord::NamespaceAuthority(mut parent) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if parent.lifecycle != NamespaceLifecycle::Ready
            || Some(&parent.identifier) != operation.destination_namespace.as_ref()
        {
            return self.abort(operation, 404, "NoSuchNamespaceException").await;
        }
        if let Some(pending) = parent.pending_operation {
            Box::pin(NamespaceCreator::help_table_parent(
                self.store.clone(),
                self.names.clone(),
                operation.context,
                parent.namespace,
                pending,
                budget,
            ))
            .await?;
            return Ok(());
        }
        parent
            .mutation_revision
            .checked_add(2)
            .ok_or(ValidationError::GenerationExhausted)?;
        parent.mutation_revision += 1;
        parent.pending_operation = Some(operation.identity.operation);
        let after = StorageRecord::NamespaceAuthority(parent).encode()?;
        let mut next = operation.next(Phase::Admitting)?;
        next.admission = Some(NamespaceMutation {
            key: key.encode()?,
            before: Some(
                self.payloads
                    .put(
                        operation.context.catalog,
                        operation.identity.operation,
                        &value.bytes,
                    )
                    .await?,
            ),
            after: self
                .payloads
                .put(operation.context.catalog, operation.identity.operation, &after)
                .await?,
        });
        self.advance(operation, &next).await
    }

    pub(super) async fn admission_bytes(
        &self,
        operation: &TableLifecycleOperation,
    ) -> Result<(Vec<u8>, Vec<u8>), CatalogError> {
        let mutation = operation.admission.as_ref().ok_or(ValidationError::Record)?;
        let before = self
            .payloads
            .get(mutation.before.as_ref().ok_or(ValidationError::Record)?)
            .await?;
        let after = self.payloads.get(&mutation.after).await?;
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let StorageRecord::NamespaceAuthority(mut parent) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if parent.lifecycle != NamespaceLifecycle::Ready
            || parent.pending_operation.is_some()
            || Some(&parent.identifier) != operation.destination_namespace.as_ref()
            || mutation.key != key.encode()?
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
        Ok((before, after))
    }

    pub(super) async fn admit(&self, operation: &TableLifecycleOperation) -> Result<(), CatalogError> {
        let (before, after) = self.admission_bytes(operation).await?;
        self.current(operation).await?;
        let mutation = operation.admission.as_ref().ok_or(ValidationError::Record)?;
        let result = self
            .store
            .compare_exchange(
                &mutation.key,
                Some(&before),
                &after,
                mutation_identity(&operation.key().encode()?, Some(&before), &after),
            )
            .await?;
        if matches!(result, CasOutcome::Applied(_))
            || matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            return self.advance(operation, &operation.next(Phase::Publishing)?).await;
        }
        let CasOutcome::Conflict(Some(value)) = result else {
            return Err(CatalogError::Busy);
        };
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let StorageRecord::NamespaceAuthority(current) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let StorageRecord::NamespaceAuthority(previous) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if current.mutation_revision <= previous.mutation_revision {
            return Err(CatalogError::Busy);
        }
        if current.lifecycle != NamespaceLifecycle::Ready
            || Some(&current.identifier) != operation.destination_namespace.as_ref()
        {
            return self.abort(operation, 404, "NoSuchNamespaceException").await;
        }
        let mut next = operation.next(Phase::Reserved)?;
        next.admission = None;
        self.advance(operation, &next).await
    }

    pub(super) async fn check_admission(
        &self,
        operation: &TableLifecycleOperation,
    ) -> Result<(), CatalogError> {
        let (_, after) = self.admission_bytes(operation).await?;
        let key = authority_key(operation.context.catalog, operation.candidate.namespace).encode()?;
        if self
            .store
            .get(&key)
            .await?
            .as_ref()
            .map(|value| value.bytes.as_slice())
            != Some(after.as_slice())
        {
            return Err(CatalogError::Busy);
        }
        Ok(())
    }

    pub(super) async fn release_admission(
        &self,
        operation: &TableLifecycleOperation,
    ) -> Result<(), CatalogError> {
        let Some(mutation) = &operation.admission else {
            return Ok(());
        };
        let (_, before) = self.admission_bytes(operation).await?;
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let StorageRecord::NamespaceAuthority(mut parent) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        parent.pending_operation = None;
        parent.mutation_revision = parent
            .mutation_revision
            .checked_add(1)
            .ok_or(ValidationError::GenerationExhausted)?;
        let after = StorageRecord::NamespaceAuthority(parent).encode()?;
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
