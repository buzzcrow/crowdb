use super::{Error, Phase, TableCreateOperation, TableCreator};
use crate::{
    catalog::{CasOutcome, CatalogError},
    error::ValidationError,
    namespace::{authority_key, NamespaceCreator, NamespaceLifecycle, NamespaceMutation},
    operation::mutation_identity,
    record::StorageRecord,
};

impl TableCreator {
    pub(super) async fn parent_ready(&self, operation: &TableCreateOperation) -> Result<bool, Error> {
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let Some(value) = self.store.get(&key.encode()?).await.map_err(CatalogError::from)? else {
            return Ok(false);
        };
        let StorageRecord::NamespaceAuthority(parent) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok(parent.lifecycle == NamespaceLifecycle::Ready && parent.identifier == operation.namespace)
    }

    pub(super) async fn prepare_admission(
        &self,
        operation: &TableCreateOperation,
        budget: &mut usize,
    ) -> Result<(), Error> {
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let Some(value) = self.store.get(&key.encode()?).await.map_err(CatalogError::from)? else {
            return self.abort(operation, 404).await;
        };
        let StorageRecord::NamespaceAuthority(mut parent) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if parent.lifecycle != NamespaceLifecycle::Ready || parent.identifier != operation.namespace {
            return self.abort(operation, 404).await;
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
        let payloads = self.payloads();
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
        let mut next = operation.next(Phase::Admitting)?;
        next.admission = Some(NamespaceMutation {
            key: key.encode()?,
            before: Some(before),
            after,
        });
        self.journal().advance(operation, &next).await?;
        Ok(())
    }

    pub(super) async fn admit(&self, operation: &TableCreateOperation) -> Result<(), Error> {
        let (before, after) = self.admission_bytes(operation).await?;
        let mutation = operation.admission.as_ref().ok_or(ValidationError::Record)?;
        self.current(operation).await?;
        let result = self
            .store
            .compare_exchange(
                &mutation.key,
                Some(&before),
                &after,
                mutation_identity(&operation.key().encode()?, Some(&before), &after),
            )
            .await
            .map_err(CatalogError::from)?;
        if matches!(result, CasOutcome::Applied(_))
            || matches!(&result, CasOutcome::Conflict(Some(value)) if value.bytes == after)
        {
            self.journal()
                .advance(operation, &operation.next(Phase::Admitted)?)
                .await?;
            return Ok(());
        }
        let CasOutcome::Conflict(Some(value)) = result else {
            return Err(CatalogError::Busy.into());
        };
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let StorageRecord::NamespaceAuthority(current) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let StorageRecord::NamespaceAuthority(previous) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if current.mutation_revision <= previous.mutation_revision {
            return Err(CatalogError::Busy.into());
        }
        if current.lifecycle != NamespaceLifecycle::Ready || current.identifier != operation.namespace {
            return self.abort(operation, 404).await;
        }
        let mut next = operation.next(Phase::FilesReady)?;
        next.admission = None;
        self.journal().advance(operation, &next).await?;
        Ok(())
    }

    pub(super) async fn check_admission(&self, operation: &TableCreateOperation) -> Result<(), Error> {
        let (_, after) = self.admission_bytes(operation).await?;
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        if self
            .store
            .get(&key.encode()?)
            .await
            .map_err(CatalogError::from)?
            .as_ref()
            .map(|value| value.bytes.as_slice())
            != Some(after.as_slice())
        {
            return Err(CatalogError::Busy.into());
        }
        self.current(operation).await
    }

    pub(super) async fn admission_bytes(
        &self,
        operation: &TableCreateOperation,
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let mutation = operation.admission.as_ref().ok_or(ValidationError::Record)?;
        let payloads = self.payloads();
        let before = payloads
            .get(mutation.before.as_ref().ok_or(ValidationError::Record)?)
            .await?;
        let after = payloads.get(&mutation.after).await?;
        let key = authority_key(operation.context.catalog, operation.candidate.namespace);
        let StorageRecord::NamespaceAuthority(mut parent) = StorageRecord::decode(&key, &before)? else {
            return Err(ValidationError::Record.into());
        };
        if parent.identifier != operation.namespace
            || parent.lifecycle != NamespaceLifecycle::Ready
            || parent.pending_operation.is_some()
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
}
