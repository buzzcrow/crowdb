// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError, CatalogStore, StoredValue},
    error::ValidationError,
    namespace::{
        authority_key, name_key, retire_absent_namespace, retire_namespace_slot, NamespaceAuthority,
        NamespaceCreator, NamespaceLifecycle, NamespaceMapping, NamespaceMappingState, NamespaceSlot,
        NamespaceStore,
    },
    operation::mutation_identity,
    record::StorageRecord,
};

impl NamespaceCreator {
    pub(crate) async fn help_creating_child(
        store: Arc<dyn CatalogStore>,
        names: Arc<dyn NamespaceStore>,
        context: CatalogContext,
        parent: NamespaceAuthority,
        budget: &mut usize,
    ) -> Result<NamespaceAuthority, CatalogError> {
        let child = parent.creating_child.ok_or(ValidationError::Record)?;
        if parent.catalog != context.catalog {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let creator = Self::from_parts(store, names);
        let key = authority_key(context.catalog, child);
        let stored = creator.names.get(&key.encode()?).await?;
        let slot = match stored {
            Some(stored) => match StorageRecord::decode(&key, &stored.bytes)? {
                StorageRecord::NamespaceAuthority(authority) => NamespaceSlot::Existing { authority, stored },
                StorageRecord::RetiredNamespace { .. } => NamespaceSlot::Retired,
                _ => return Err(ValidationError::Record.into()),
            },
            None => {
                retire_absent_namespace(creator.repository.store.as_ref(), context.catalog, child).await?
            }
        };
        if let NamespaceSlot::Existing { authority, stored } = slot {
            if authority.parent != Some(parent.namespace)
                || authority.identifier.parent().as_ref() != Some(&parent.identifier)
            {
                return Err(ValidationError::IdentityMismatch.into());
            }
            let mapping_key = name_key(
                context.catalog,
                Some(parent.namespace),
                authority.identifier.name(),
            )?;
            if let Some(value) = creator.names.get(&mapping_key.encode()?).await? {
                let StorageRecord::NamespaceMapping(mapping) =
                    StorageRecord::decode(&mapping_key, &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                if mapping.namespace == child && mapping.name_epoch == authority.name_epoch {
                    if mapping.state == NamespaceMappingState::ReservedResource
                        && authority.lifecycle == NamespaceLifecycle::Ready
                    {
                        creator.publish_resource_name(&mapping).await?;
                    } else if mapping.state != NamespaceMappingState::Published
                        && authority.lifecycle != NamespaceLifecycle::Tombstone
                    {
                        return Err(CatalogError::Busy);
                    }
                } else if authority.lifecycle != NamespaceLifecycle::Tombstone {
                    retire_namespace_slot(
                        creator.repository.store.as_ref(),
                        context.catalog,
                        child,
                        Some(stored),
                    )
                    .await?;
                }
            } else if authority.lifecycle != NamespaceLifecycle::Tombstone {
                retire_namespace_slot(
                    creator.repository.store.as_ref(),
                    context.catalog,
                    child,
                    Some(stored),
                )
                .await?;
            }
        }
        if *budget == 0 {
            return Err(CatalogError::Busy);
        }
        *budget -= 1;
        creator.release_resource_parent(parent, child).await
    }

    pub(in crate::namespace) async fn settle_resource_mapping(
        &self,
        context: CatalogContext,
        mapping: &NamespaceMapping,
        budget: &mut usize,
    ) -> Result<bool, CatalogError> {
        if *budget == 0 || mapping.state != NamespaceMappingState::ReservedResource {
            return Err(CatalogError::Busy);
        }
        *budget -= 1;
        let key = authority_key(context.catalog, mapping.namespace);
        let stored = self.names.get(&key.encode()?).await?;
        let slot = match stored {
            Some(stored) => match StorageRecord::decode(&key, &stored.bytes)? {
                StorageRecord::NamespaceAuthority(authority) => NamespaceSlot::Existing { authority, stored },
                StorageRecord::RetiredNamespace { .. } => NamespaceSlot::Retired,
                _ => return Err(ValidationError::Record.into()),
            },
            None => {
                retire_absent_namespace(self.repository.store.as_ref(), context.catalog, mapping.namespace)
                    .await?
            }
        };
        let NamespaceSlot::Existing { authority, stored } = slot else {
            return self.remove_resource_mapping(mapping).await;
        };
        if authority.parent != mapping.parent
            || authority.identifier.name() != mapping.name
            || authority.name_epoch != mapping.name_epoch
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        if authority.lifecycle != NamespaceLifecycle::Ready {
            return self.cancel_resource_name(mapping, Some(stored)).await;
        }
        let occupied = if let Some(parent) = mapping.parent {
            let Some(parent) = self.load_resource_parent(context, parent).await? else {
                return self.cancel_resource_name(mapping, Some(stored)).await;
            };
            let Some(parent) =
                Box::pin(self.claim_resource_parent(context, parent, &authority, budget)).await?
            else {
                return self.cancel_resource_name(mapping, Some(stored)).await;
            };
            Some(parent)
        } else {
            None
        };
        self.publish_resource_name(mapping).await?;
        if let Some(parent) = occupied {
            self.release_resource_parent(parent, authority.namespace).await?;
        }
        Ok(true)
    }

    pub(super) async fn cancel_resource_name(
        &self,
        mapping: &NamespaceMapping,
        current: Option<StoredValue>,
    ) -> Result<bool, CatalogError> {
        if self.remove_resource_mapping(mapping).await? {
            return Ok(true);
        }
        retire_namespace_slot(
            self.repository.store.as_ref(),
            mapping.catalog,
            mapping.namespace,
            current,
        )
        .await?;
        Ok(false)
    }

    pub(super) async fn remove_resource_mapping(
        &self,
        mapping: &NamespaceMapping,
    ) -> Result<bool, CatalogError> {
        let key = name_key(mapping.catalog, mapping.parent, &mapping.name)?.encode()?;
        let before = StorageRecord::NamespaceMapping(mapping.clone()).encode()?;
        match self
            .names
            .delete_mapping(&key, &before, mutation_identity(&key, Some(&before), &[]))
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(None) => Ok(false),
            CasOutcome::Conflict(Some(value)) => {
                let mut published = mapping.clone();
                published.state = NamespaceMappingState::Published;
                if value.bytes == StorageRecord::NamespaceMapping(published).encode()? {
                    Ok(true)
                } else {
                    Err(CatalogError::Conflict)
                }
            }
        }
    }
}
