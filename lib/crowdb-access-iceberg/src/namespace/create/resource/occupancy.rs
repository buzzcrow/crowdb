// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError},
    error::ValidationError,
    key::NamespaceId,
    namespace::{authority_key, NamespaceAuthority, NamespaceCreator, NamespaceLifecycle},
    operation::mutation_identity,
    record::StorageRecord,
};

impl NamespaceCreator {
    pub(super) async fn claim_resource_parent(
        &self,
        context: CatalogContext,
        mut parent: NamespaceAuthority,
        child: &NamespaceAuthority,
        budget: &mut usize,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        let name_epoch = parent.name_epoch;
        while *budget > 0 {
            *budget -= 1;
            if parent.catalog != context.catalog
                || parent.name_epoch != name_epoch
                || Some(parent.namespace) != child.parent
                || Some(parent.identifier.clone()) != child.identifier.parent()
            {
                return Err(ValidationError::IdentityMismatch.into());
            }
            if parent.lifecycle != NamespaceLifecycle::Ready {
                return Ok(None);
            }
            if parent.creating_child == Some(child.namespace) {
                return Ok(Some(parent));
            }
            if parent.creating_child.is_some() {
                parent = Box::pin(Self::help_creating_child(
                    self.repository.store.clone(),
                    self.names.clone(),
                    context,
                    parent,
                    budget,
                ))
                .await?;
                continue;
            }
            if let Some(pending) = parent.pending_operation {
                Box::pin(self.help_marker(context, parent.namespace, pending, budget)).await?;
                parent = self
                    .load_resource_parent(context, parent.namespace)
                    .await?
                    .ok_or(CatalogError::Conflict)?;
                continue;
            }
            if let Some(rename) = &parent.rename {
                crate::table::TableRenames::from_parts(self.repository.store.clone(), self.names.clone())
                    .recover(context, rename)
                    .await?;
                parent = self
                    .load_resource_parent(context, parent.namespace)
                    .await?
                    .ok_or(CatalogError::Conflict)?;
                continue;
            }
            let key = authority_key(context.catalog, parent.namespace);
            let encoded = key.encode()?;
            let before = StorageRecord::NamespaceAuthority(Box::new(parent.clone())).encode()?;
            let mut next = parent.clone();
            next.creating_child = Some(child.namespace);
            next.mutation_revision = next
                .mutation_revision
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
            let after = StorageRecord::NamespaceAuthority(Box::new(next.clone())).encode()?;
            match self
                .names
                .compare_exchange(
                    &encoded,
                    Some(&before),
                    &after,
                    mutation_identity(&encoded, Some(&before), &after),
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(Some(next)),
                CasOutcome::Conflict(Some(value)) => match StorageRecord::decode(&key, &value.bytes)? {
                    StorageRecord::NamespaceAuthority(current) => parent = *current,
                    StorageRecord::RetiredNamespace { .. } => return Ok(None),
                    _ => return Err(ValidationError::Record.into()),
                },
                CasOutcome::Conflict(None) => return Ok(None),
            }
        }
        Err(CatalogError::Busy)
    }

    pub(super) async fn release_resource_parent(
        &self,
        mut parent: NamespaceAuthority,
        child: NamespaceId,
    ) -> Result<NamespaceAuthority, CatalogError> {
        let key = authority_key(parent.catalog, parent.namespace);
        let encoded = key.encode()?;
        for _ in 0..16 {
            if parent.creating_child != Some(child) {
                return Ok(parent);
            }
            let before = StorageRecord::NamespaceAuthority(Box::new(parent.clone())).encode()?;
            let mut next = parent.clone();
            next.creating_child = None;
            next.mutation_revision = next
                .mutation_revision
                .checked_add(1)
                .ok_or(ValidationError::GenerationExhausted)?;
            let after = StorageRecord::NamespaceAuthority(Box::new(next.clone())).encode()?;
            match self
                .names
                .compare_exchange(
                    &encoded,
                    Some(&before),
                    &after,
                    mutation_identity(&encoded, Some(&before), &after),
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(next),
                CasOutcome::Conflict(Some(value)) => {
                    let StorageRecord::NamespaceAuthority(current) =
                        StorageRecord::decode(&key, &value.bytes)?
                    else {
                        return Err(CatalogError::Conflict);
                    };
                    if current.identifier != parent.identifier
                        || current.name_epoch != parent.name_epoch
                        || current.parent != parent.parent
                    {
                        return Err(CatalogError::Conflict);
                    }
                    parent = *current;
                }
                CasOutcome::Conflict(None) => return Err(CatalogError::Conflict),
            }
        }
        Err(CatalogError::Busy)
    }

    pub(super) async fn load_resource_parent(
        &self,
        context: CatalogContext,
        namespace: NamespaceId,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        let key = authority_key(context.catalog, namespace);
        let Some(value) = self.names.get(&key.encode()?).await? else {
            return Ok(None);
        };
        match StorageRecord::decode(&key, &value.bytes)? {
            StorageRecord::NamespaceAuthority(authority) => Ok(Some(*authority)),
            StorageRecord::RetiredNamespace { .. } => Ok(None),
            _ => Err(ValidationError::Record.into()),
        }
    }
}
