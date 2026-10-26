// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError},
    error::ValidationError,
    key::IcebergKey,
    namespace::{
        authority_key, retire_absent_namespace, NamespaceAction, NamespaceDropper, NamespaceJournal,
        NamespaceMappingState, NamespaceSlot,
    },
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{super::DropBinding, DropProbe};

impl NamespaceDropper {
    pub(super) async fn inspect_drop_namespace(
        &self,
        context: CatalogContext,
        binding: &DropBinding,
        encoded: &[u8],
        bytes: &[u8],
        budget: &mut usize,
    ) -> Result<DropProbe, CatalogError> {
        let key = IcebergKey::decode(encoded)?;
        let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if mapping.parent != Some(binding.mapping.namespace) {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if mapping.state == NamespaceMappingState::ReservedResource {
            self.creator
                .settle_resource_mapping(context, &mapping, budget)
                .await?;
            return Ok(DropProbe::Restart);
        }
        if mapping.state == NamespaceMappingState::Reserved {
            let owner = NamespaceJournal::new(self.creator.repository.store.clone())
                .load(context, mapping.operation)
                .await?
                .ok_or(ValidationError::Record)?;
            if owner.action != NamespaceAction::Create
                || owner.parent != mapping.parent
                || owner.namespace != mapping.namespace
                || owner.identifier.parent().as_ref() != Some(&binding.identifier)
                || owner.identifier.name() != mapping.name
            {
                return Err(ValidationError::Record.into());
            }
            Box::pin(
                self.creator
                    .resume_with_budget(context, mapping.operation, budget),
            )
            .await?;
            return Ok(DropProbe::Restart);
        }
        let key = authority_key(context.catalog, mapping.namespace);
        let authority = if let Some(value) = self.creator.names.get(&key.encode()?).await? {
            match StorageRecord::decode(&key, &value.bytes)? {
                StorageRecord::NamespaceAuthority(authority) => Some(authority),
                StorageRecord::RetiredNamespace { .. } => None,
                _ => return Err(ValidationError::Record.into()),
            }
        } else {
            match retire_absent_namespace(
                self.creator.repository.store.as_ref(),
                context.catalog,
                mapping.namespace,
            )
            .await?
            {
                NamespaceSlot::Retired => None,
                NamespaceSlot::Existing { authority, .. } => Some(authority),
            }
        };
        if authority.as_ref().is_some_and(|authority| {
            mapping.resolves(authority) && authority.identifier.parent().as_ref() == Some(&binding.identifier)
        }) {
            return Ok(DropProbe::Nonempty);
        }
        if let Some(current) = self.delete_stale_drop_mapping(encoded, bytes).await? {
            if *budget == 0 {
                return Err(CatalogError::Busy);
            }
            *budget -= 1;
            return Box::pin(self.inspect_drop_namespace(context, binding, encoded, &current, budget)).await;
        }
        Ok(DropProbe::Empty)
    }

    pub(super) async fn inspect_drop_table(
        &self,
        context: CatalogContext,
        binding: &DropBinding,
        encoded: &[u8],
        bytes: &[u8],
        budget: &mut usize,
    ) -> Result<DropProbe, CatalogError> {
        let key = IcebergKey::decode(encoded)?;
        let StorageRecord::TableMapping(mapping) = StorageRecord::decode(&key, bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if mapping.namespace != binding.mapping.namespace {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if mapping.state == crate::table::TableMappingState::Reserved {
            Box::pin(crate::commit::TableCreator::help_reservation(
                self.creator.repository.store.clone(),
                self.creator.names.clone(),
                context,
                &mapping,
                budget,
            ))
            .await?;
            return Ok(DropProbe::Restart);
        }
        let key = crate::table::storage_head_key(mapping.storage_location());
        let value = self
            .creator
            .names
            .get(&key.encode()?)
            .await?
            .ok_or(ValidationError::Record)?;
        let record = StorageRecord::decode(&key, &value.bytes)?;
        if matches!(&record, StorageRecord::TableHead(head) if mapping.resolves(head)) {
            return Ok(DropProbe::Nonempty);
        }
        if !matches!(
            record,
            StorageRecord::TableHead(_) | StorageRecord::RetiredTable(_)
        ) {
            return Err(ValidationError::Record.into());
        }
        if let Some(current) = self.delete_stale_drop_mapping(encoded, bytes).await? {
            if *budget == 0 {
                return Err(CatalogError::Busy);
            }
            *budget -= 1;
            return Box::pin(self.inspect_drop_table(context, binding, encoded, &current, budget)).await;
        }
        Ok(DropProbe::Empty)
    }

    async fn delete_stale_drop_mapping(
        &self,
        key: &[u8],
        bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, CatalogError> {
        match self
            .creator
            .names
            .delete_mapping(key, bytes, mutation_identity(key, Some(bytes), &[]))
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(None) => Ok(None),
            CasOutcome::Conflict(Some(current)) => Ok(Some(current.bytes)),
        }
    }
}
