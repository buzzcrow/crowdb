use std::sync::Arc;

use crate::{
    catalog::{CasOutcome, CatalogError, CatalogStore},
    commit::TableCreateOperation,
    error::ValidationError,
    namespace::{authority_key, NamespaceStore},
    operation::{mutation_identity, PayloadStore},
    record::StorageRecord,
    table::{name_key, retire_absent_table, storage_head_key, TableMappingState, TableSlot},
};

pub(super) async fn reconcile(
    store: Arc<dyn CatalogStore>,
    names: Arc<dyn NamespaceStore>,
    operation: &TableCreateOperation,
) -> Result<(), CatalogError> {
    operation.validate()?;
    let slot = retire_absent_table(store.as_ref(), operation.candidate.storage_location()).await?;
    settle_name(names.as_ref(), operation, &slot).await?;
    if matches!(&slot, TableSlot::Existing(head) if head.as_ref() == &operation.candidate) {
        settle_head(store.as_ref(), operation).await?;
    }
    settle_parent(store, operation).await
}

async fn settle_name(
    names: &dyn NamespaceStore,
    operation: &TableCreateOperation,
    slot: &TableSlot,
) -> Result<(), CatalogError> {
    let reserved = operation.mapping(TableMappingState::Reserved);
    let key = name_key(reserved.catalog, reserved.namespace, &reserved.name)?;
    let encoded = key.encode()?;
    let expected = StorageRecord::TableMapping(reserved).encode()?;
    let Some(current) = names.get(&encoded).await? else {
        return Ok(());
    };
    let StorageRecord::TableMapping(_) = StorageRecord::decode(&key, &current.bytes)? else {
        return Err(ValidationError::Record.into());
    };
    if current.bytes != expected {
        return Ok(());
    }
    let published = operation.mapping(TableMappingState::Published);
    if matches!(slot, TableSlot::Existing(head) if published.resolves(head)) {
        let bytes = StorageRecord::TableMapping(published).encode()?;
        match names
            .compare_exchange(
                &encoded,
                Some(&expected),
                &bytes,
                mutation_identity(&encoded, Some(&expected), &bytes),
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(current)) if current.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Busy),
        }
    } else {
        names
            .delete_mapping(
                &encoded,
                &expected,
                mutation_identity(&encoded, Some(&expected), &[]),
            )
            .await?;
        Ok(())
    }
}

async fn settle_head(store: &dyn CatalogStore, operation: &TableCreateOperation) -> Result<(), CatalogError> {
    let key = storage_head_key(operation.candidate.storage_location()).encode()?;
    let before = StorageRecord::TableHead(Box::new(operation.candidate.clone())).encode()?;
    let mut head = operation.candidate.clone();
    head.pending_operation = None;
    let bytes = StorageRecord::TableHead(Box::new(head)).encode()?;
    // An observed replacement belongs to another resource transition.
    store
        .compare_exchange(
            &key,
            Some(&before),
            &bytes,
            mutation_identity(&key, Some(&before), &bytes),
        )
        .await?;
    Ok(())
}

async fn settle_parent(
    store: Arc<dyn CatalogStore>,
    operation: &TableCreateOperation,
) -> Result<(), CatalogError> {
    let Some(admission) = &operation.admission else {
        return Ok(());
    };
    let captured = PayloadStore::new(store.clone()).get(&admission.after).await?;
    let key = authority_key(operation.context.catalog, operation.candidate.namespace);
    let StorageRecord::NamespaceAuthority(captured) = StorageRecord::decode(&key, &captured)? else {
        return Err(ValidationError::Record.into());
    };
    if admission.key != key.encode()?
        || captured.pending_operation != Some(operation.identity.operation)
        || captured.identifier != operation.namespace
    {
        return Err(ValidationError::Record.into());
    }
    let Some(value) = store.get(&admission.key).await? else {
        return Ok(());
    };
    let mut current = match StorageRecord::decode(&key, &value.bytes)? {
        StorageRecord::NamespaceAuthority(current) => current,
        StorageRecord::RetiredNamespace { .. } => return Ok(()),
        _ => return Err(ValidationError::Record.into()),
    };
    // An operation ID alone cannot distinguish a later use of the same parent.
    if current.pending_operation != captured.pending_operation
        || current.mutation_revision != captured.mutation_revision
    {
        return Ok(());
    }
    current.pending_operation = None;
    current.mutation_revision = current
        .mutation_revision
        .checked_add(1)
        .ok_or(ValidationError::GenerationExhausted)?;
    let bytes = StorageRecord::NamespaceAuthority(current).encode()?;
    match store
        .compare_exchange(
            &admission.key,
            Some(&value.bytes),
            &bytes,
            mutation_identity(&admission.key, Some(&value.bytes), &bytes),
        )
        .await?
    {
        CasOutcome::Applied(_) => Ok(()),
        CasOutcome::Conflict(Some(observed)) if observed.bytes == bytes => Ok(()),
        CasOutcome::Conflict(_) => Err(CatalogError::Busy),
    }
}
