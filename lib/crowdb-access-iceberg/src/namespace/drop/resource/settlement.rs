// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError},
    error::ValidationError,
    namespace::{authority_key, name_key, NamespaceAuthority, NamespaceDropper, NamespaceLifecycle},
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{probe::DropProbe, DropBinding};

impl NamespaceDropper {
    pub(super) async fn fence_resource_drop(
        &self,
        context: CatalogContext,
        binding: &mut DropBinding,
        authority: NamespaceAuthority,
    ) -> Result<(), CatalogError> {
        authority
            .admission_fence
            .checked_add(2)
            .ok_or(ValidationError::GenerationExhausted)?;
        authority
            .mutation_revision
            .checked_add(2)
            .ok_or(ValidationError::GenerationExhausted)?;
        let mut next = authority;
        next.admission_fence += 1;
        next.mutation_revision += 1;
        next.lifecycle = NamespaceLifecycle::Dropping;
        let after = StorageRecord::NamespaceAuthority(Box::new(next)).encode()?;
        self.creator.repository.check_context(context).await?;
        self.exchange_drop_authority(binding, after).await?;
        Ok(())
    }

    pub(super) async fn commit_resource_drop(
        &self,
        context: CatalogContext,
        binding: &mut DropBinding,
        proof: DropProbe,
    ) -> Result<bool, CatalogError> {
        let authority = binding.authority.as_ref().ok_or(ValidationError::Record)?;
        if authority.lifecycle != NamespaceLifecycle::Dropping || authority.pending_operation.is_some() {
            return Err(ValidationError::Record.into());
        }
        let after = match proof {
            DropProbe::Empty => StorageRecord::RetiredNamespace {
                catalog: authority.catalog,
                namespace: authority.namespace,
            }
            .encode()?,
            DropProbe::Nonempty => {
                let mut next = authority.clone();
                next.lifecycle = NamespaceLifecycle::Ready;
                next.admission_fence = next
                    .admission_fence
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
                next.mutation_revision = next
                    .mutation_revision
                    .checked_add(1)
                    .ok_or(ValidationError::GenerationExhausted)?;
                StorageRecord::NamespaceAuthority(Box::new(next)).encode()?
            }
            DropProbe::Restart => return Err(ValidationError::Record.into()),
        };
        self.creator.repository.check_context(context).await?;
        let committed = self.exchange_drop_authority(binding, after).await?;
        if binding.authority.is_none()
            || binding
                .authority
                .as_ref()
                .is_some_and(|authority| authority.lifecycle == NamespaceLifecycle::Tombstone)
        {
            binding.authority = None;
            self.finish_resource_drop(context, binding).await?;
            return Ok(true);
        }
        if committed {
            self.creator.repository.check_context(context).await?;
            return Ok(true);
        }
        // In particular, F1 -> Ready F2 -> Dropping F3 cannot reuse F1's
        // cursor or empty proof. The caller scans both F3 ranges from scratch.
        Ok(false)
    }

    async fn exchange_drop_authority(
        &self,
        binding: &mut DropBinding,
        after: Vec<u8>,
    ) -> Result<bool, CatalogError> {
        let key = authority_key(binding.mapping.catalog, binding.mapping.namespace).encode()?;
        let before = binding.authority_bytes.as_slice();
        let (bytes, committed) = match self
            .creator
            .names
            .compare_exchange(
                &key,
                Some(before),
                &after,
                mutation_identity(&key, Some(before), &after),
            )
            .await?
        {
            CasOutcome::Applied(_) => (after, true),
            CasOutcome::Conflict(Some(value)) => {
                let committed = value.bytes == after;
                (value.bytes, committed)
            }
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
        };
        binding.observe(bytes)?;
        Ok(committed)
    }

    pub(super) async fn finish_resource_drop(
        &self,
        context: CatalogContext,
        binding: &DropBinding,
    ) -> Result<(), CatalogError> {
        let key = name_key(
            binding.mapping.catalog,
            binding.mapping.parent,
            &binding.mapping.name,
        )?;
        let encoded = key.encode()?;
        match self
            .creator
            .names
            .delete_mapping(
                &encoded,
                &binding.mapping_bytes,
                mutation_identity(&encoded, Some(&binding.mapping_bytes), &[]),
            )
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(None) => {}
            CasOutcome::Conflict(Some(value)) => {
                let StorageRecord::NamespaceMapping(current) = StorageRecord::decode(&key, &value.bytes)?
                else {
                    return Err(ValidationError::Record.into());
                };
                if current.namespace == binding.mapping.namespace {
                    return Err(CatalogError::Busy);
                }
            }
        }
        self.creator.repository.check_context(context).await?;
        Ok(())
    }
}
