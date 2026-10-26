// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    catalog::{CatalogContext, CatalogError},
    error::ValidationError,
    namespace::{
        authority_key, name_key, NamespaceAuthority, NamespaceDropRequest, NamespaceDropper,
        NamespaceLifecycle, NamespaceMapping,
    },
    record::StorageRecord,
};

mod binding;
mod probe;
mod settlement;

pub(super) use binding::DropBinding;
use probe::DropProbe;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceDropOutcome {
    pub status: u16,
    pub body: Vec<u8>,
}

impl NamespaceDropper {
    pub(super) async fn drop_resource(
        &self,
        request: &NamespaceDropRequest,
    ) -> Result<Option<NamespaceDropOutcome>, CatalogError> {
        if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
            return Err(ValidationError::Text.into());
        }
        self.creator.repository.check_context(request.context).await?;
        let Some(binding) = self
            .select_drop_binding(request.context, &request.identifier)
            .await?
        else {
            self.creator.repository.check_context(request.context).await?;
            return Ok(None);
        };
        let restored = self
            .drive_resource_drop(request.context, binding, &mut 16)
            .await?;
        let status = if restored.is_some() { 409 } else { 204 };
        let body = if status == 204 {
            Vec::new()
        } else {
            serde_json::to_vec(&serde_json::json!({
                "error": {"code": 409, "type": "NamespaceNotEmptyException", "message": "Namespace has children"}
            })).map_err(|_| ValidationError::Record)?
        };
        Ok(Some(NamespaceDropOutcome { status, body }))
    }

    pub(in crate::namespace) async fn help_resource_drop(
        &self,
        context: CatalogContext,
        authority: NamespaceAuthority,
        budget: &mut usize,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        if authority.lifecycle != NamespaceLifecycle::Dropping || authority.pending_operation.is_some() {
            return Err(ValidationError::Record.into());
        }
        let key = name_key(context.catalog, authority.parent, authority.identifier.name())?;
        let Some(value) = self.creator.names.get(&key.encode()?).await? else {
            return Err(CatalogError::Busy);
        };
        let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        let identifier = authority.identifier.clone();
        let authority_bytes = StorageRecord::NamespaceAuthority(Box::new(authority)).encode()?;
        let binding = DropBinding::new(identifier, mapping, value.bytes, authority_bytes)?;
        self.drive_resource_drop(context, binding, budget).await
    }

    pub(in crate::namespace) async fn resume_resource_drop(
        &self,
        context: CatalogContext,
        mapping: NamespaceMapping,
        mapping_bytes: Vec<u8>,
        authority: NamespaceAuthority,
        authority_bytes: Vec<u8>,
    ) -> Result<(), CatalogError> {
        if authority.lifecycle != NamespaceLifecycle::Dropping || authority.pending_operation.is_some() {
            return Err(ValidationError::Record.into());
        }
        let binding = DropBinding::new(
            authority.identifier.clone(),
            mapping,
            mapping_bytes,
            authority_bytes,
        )?;
        let _ = self.drive_resource_drop(context, binding, &mut 16).await?;
        Ok(())
    }

    async fn drive_resource_drop(
        &self,
        context: CatalogContext,
        mut binding: DropBinding,
        budget: &mut usize,
    ) -> Result<Option<NamespaceAuthority>, CatalogError> {
        let mut fenced = binding
            .authority
            .as_ref()
            .is_some_and(|authority| authority.lifecycle == NamespaceLifecycle::Dropping);
        while *budget > 0 {
            *budget -= 1;
            let Some(authority) = binding.authority.as_ref() else {
                self.creator.repository.check_context(context).await?;
                self.finish_resource_drop(context, &binding).await?;
                return Ok(None);
            };
            if authority.lifecycle == NamespaceLifecycle::Tombstone {
                self.creator.repository.check_context(context).await?;
                self.finish_resource_drop(context, &binding).await?;
                return Ok(None);
            }
            if fenced && authority.lifecycle == NamespaceLifecycle::Ready {
                let restored = authority.clone();
                self.creator.repository.check_context(context).await?;
                return Ok(Some(restored));
            }
            if authority.creating_child.is_some()
                || authority.rename.is_some()
                || authority.pending_operation.is_some()
            {
                let authority = authority.clone();
                self.settle_drop_occupancy(context, &mut binding, authority, budget)
                    .await?;
                continue;
            }
            if authority.lifecycle == NamespaceLifecycle::Ready {
                let authority = authority.clone();
                self.fence_resource_drop(context, &mut binding, authority).await?;
                continue;
            }
            fenced = true;
            match self.probe_resource_drop(context, &binding, budget).await? {
                DropProbe::Restart => {}
                proof => {
                    if self.commit_resource_drop(context, &mut binding, proof).await? {
                        return Ok(binding.authority);
                    }
                }
            }
        }
        Err(CatalogError::Busy)
    }

    async fn settle_drop_occupancy(
        &self,
        context: CatalogContext,
        binding: &mut DropBinding,
        authority: NamespaceAuthority,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        if authority.creating_child.is_some() {
            let next = Box::pin(crate::namespace::NamespaceCreator::help_creating_child(
                self.creator.repository.store.clone(),
                self.creator.names.clone(),
                context,
                authority,
                budget,
            ))
            .await?;
            return binding.observe(StorageRecord::NamespaceAuthority(Box::new(next)).encode()?);
        }
        if let Some(rename) = &authority.rename {
            crate::table::TableRenames::from_parts(
                self.creator.repository.store.clone(),
                self.creator.names.clone(),
            )
            .recover(context, rename)
            .await?;
        } else if let Some(owner) = authority.pending_operation {
            Box::pin(
                self.creator
                    .help_marker(context, authority.namespace, owner, budget),
            )
            .await?;
        }
        let value = self
            .creator
            .names
            .get(&authority_key(context.catalog, authority.namespace).encode()?)
            .await?
            .ok_or(CatalogError::Busy)?;
        binding.observe(value.bytes)
    }
}
