// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{NamespaceCreateRequest, NamespaceCreationOutcome, NamespaceCreator};
use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError, StoredValue},
    error::ValidationError,
    key::{NamespaceId, OperationId},
    namespace::{
        authority_key, name_key, NamespaceAuthority, NamespaceLifecycle, NamespaceMapping,
        NamespaceMappingState,
    },
    operation::mutation_identity,
    record::StorageRecord,
};

mod occupancy;
mod settlement;

impl NamespaceCreator {
    pub(super) async fn create_resource(
        &self,
        request: &NamespaceCreateRequest,
    ) -> Result<NamespaceCreationOutcome, CatalogError> {
        if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
            return Err(ValidationError::Text.into());
        }
        self.repository.check_context(request.context).await?;
        let parent = if let Some(identifier) = request.identifier.parent() {
            Some(
                self.repository
                    .resolve(request.context, &identifier)
                    .await?
                    .ok_or(ValidationError::Text)?,
            )
        } else {
            None
        };
        if parent
            .as_ref()
            .is_some_and(|parent| parent.lifecycle != NamespaceLifecycle::Ready)
        {
            return Err(ValidationError::Text.into());
        }
        let child = NamespaceAuthority {
            catalog: request.context.catalog,
            namespace: NamespaceId::random(),
            parent: parent.as_ref().map(|parent| parent.namespace),
            identifier: request.identifier.clone(),
            name_epoch: 1,
            property_revision: 1,
            admission_fence: 1,
            mutation_revision: 1,
            lifecycle: NamespaceLifecycle::Ready,
            pending_operation: None,
            creating_child: None,
            rename: None,
            properties: request.properties.clone(),
        };
        let bytes = StorageRecord::NamespaceAuthority(Box::new(child.clone())).encode()?;
        let response = creation_response(request, 200)?;
        let mapping = NamespaceMapping {
            catalog: child.catalog,
            parent: child.parent,
            name: child.identifier.name().into(),
            namespace: child.namespace,
            name_epoch: child.name_epoch,
            operation: OperationId::random(),
            state: NamespaceMappingState::ReservedResource,
        };
        if !self
            .reserve_resource_name(request.context, &mapping, parent.as_ref(), &mut 16)
            .await?
        {
            self.repository.check_context(request.context).await?;
            return creation_response(request, 409);
        }
        let key = authority_key(child.catalog, child.namespace).encode()?;
        let stored = match self
            .names
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await?
        {
            CasOutcome::Applied(revision) => StoredValue { bytes, revision },
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => value,
            CasOutcome::Conflict(_) => return Err(CatalogError::Conflict),
        };
        let occupied = if let Some(parent) = parent {
            if let Some(parent) = self
                .claim_resource_parent(request.context, parent, &child, &mut 16)
                .await?
            {
                Some(parent)
            } else {
                if self.cancel_resource_name(&mapping, Some(stored)).await? {
                    self.repository.check_context(request.context).await?;
                    return Ok(response);
                }
                return Err(ValidationError::Text.into());
            }
        } else {
            None
        };
        self.publish_resource_name(&mapping).await?;
        if let Some(parent) = occupied {
            self.release_resource_parent(parent, child.namespace).await?;
        }
        self.repository.check_context(request.context).await?;
        Ok(response)
    }

    async fn reserve_resource_name(
        &self,
        context: CatalogContext,
        mapping: &NamespaceMapping,
        parent: Option<&NamespaceAuthority>,
        budget: &mut usize,
    ) -> Result<bool, CatalogError> {
        let key = name_key(mapping.catalog, mapping.parent, &mapping.name)?;
        let encoded = key.encode()?;
        let bytes = StorageRecord::NamespaceMapping(mapping.clone()).encode()?;
        while *budget > 0 {
            *budget -= 1;
            let current = match self
                .names
                .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
                .await?
            {
                CasOutcome::Applied(_) => return Ok(true),
                CasOutcome::Conflict(Some(current)) if current.bytes == bytes => return Ok(true),
                CasOutcome::Conflict(Some(current)) => current,
                CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
            };
            let StorageRecord::NamespaceMapping(existing) = StorageRecord::decode(&key, &current.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            match existing.state {
                NamespaceMappingState::ReservedResource => {
                    if self.settle_resource_mapping(context, &existing, budget).await? {
                        return Ok(false);
                    }
                }
                NamespaceMappingState::Reserved => {
                    Box::pin(self.resume_with_budget(context, existing.operation, budget)).await?;
                    return Err(CatalogError::Busy);
                }
                NamespaceMappingState::Published => {
                    let child_key = authority_key(existing.catalog, existing.namespace);
                    let stored = self.names.get(&child_key.encode()?).await?;
                    if let Some(stored) = &stored {
                        match StorageRecord::decode(&child_key, &stored.bytes)? {
                            StorageRecord::NamespaceAuthority(authority) if existing.resolves(&authority) => {
                                if let Some(parent) =
                                    parent.filter(|parent| parent.creating_child == Some(existing.namespace))
                                {
                                    self.release_resource_parent(parent.clone(), existing.namespace)
                                        .await?;
                                }
                                return Ok(false);
                            }
                            StorageRecord::NamespaceAuthority(_) => {
                                self.remove_resource_mapping(&existing).await?;
                                continue;
                            }
                            StorageRecord::RetiredNamespace { .. } => {}
                            _ => return Err(ValidationError::Record.into()),
                        }
                    }
                    crate::namespace::retire_namespace_slot(
                        self.repository.store.as_ref(),
                        existing.catalog,
                        existing.namespace,
                        stored,
                    )
                    .await?;
                    self.remove_resource_mapping(&existing).await?;
                }
            }
        }
        Err(CatalogError::Busy)
    }

    async fn publish_resource_name(&self, mapping: &NamespaceMapping) -> Result<(), CatalogError> {
        let key = name_key(mapping.catalog, mapping.parent, &mapping.name)?.encode()?;
        let before = StorageRecord::NamespaceMapping(mapping.clone()).encode()?;
        let mut published = mapping.clone();
        published.state = NamespaceMappingState::Published;
        let after = StorageRecord::NamespaceMapping(published).encode()?;
        match self
            .names
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == after => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict),
        }
    }
}

fn creation_response(
    request: &NamespaceCreateRequest,
    status: u16,
) -> Result<NamespaceCreationOutcome, CatalogError> {
    let body = if status == 200 {
        serde_json::to_vec(&serde_json::json!({ "namespace": request.identifier.components(), "properties": request.properties.entries() }))
    } else {
        serde_json::to_vec(&serde_json::json!({ "error": { "code": 409, "type": "AlreadyExistsException", "message": "Namespace already exists" } }))
    }.map_err(|_| ValidationError::Record)?;
    if body.len() > crate::operation::MAX_PAYLOAD_BYTES {
        return Err(ValidationError::RecordTooLarge.into());
    }
    Ok(NamespaceCreationOutcome { status, body })
}
