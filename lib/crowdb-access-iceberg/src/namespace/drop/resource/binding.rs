// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    catalog::{CatalogContext, CatalogError},
    error::ValidationError,
    namespace::{
        authority_key, name_key, NamespaceAuthority, NamespaceDropper, NamespaceIdentifier,
        NamespaceLifecycle, NamespaceMapping, NamespaceMappingState,
    },
    record::StorageRecord,
};

pub(in crate::namespace::drop) struct DropBinding {
    pub(super) identifier: NamespaceIdentifier,
    pub(super) mapping: NamespaceMapping,
    pub(super) mapping_bytes: Vec<u8>,
    pub(super) authority: Option<NamespaceAuthority>,
    pub(super) authority_bytes: Vec<u8>,
}

impl DropBinding {
    pub(super) fn new(
        identifier: NamespaceIdentifier,
        mapping: NamespaceMapping,
        mapping_bytes: Vec<u8>,
        authority_bytes: Vec<u8>,
    ) -> Result<Self, CatalogError> {
        let mut binding = Self {
            identifier,
            mapping,
            mapping_bytes,
            authority: None,
            authority_bytes: Vec::new(),
        };
        if binding.mapping.state != NamespaceMappingState::Published {
            return Err(ValidationError::Record.into());
        }
        binding.observe(authority_bytes)?;
        Ok(binding)
    }

    pub(super) fn observe(&mut self, bytes: Vec<u8>) -> Result<(), CatalogError> {
        let key = authority_key(self.mapping.catalog, self.mapping.namespace);
        let authority = match StorageRecord::decode(&key, &bytes)? {
            StorageRecord::RetiredNamespace { .. } => None,
            StorageRecord::NamespaceAuthority(authority) => {
                if authority.catalog != self.mapping.catalog
                    || authority.namespace != self.mapping.namespace
                    || authority.parent != self.mapping.parent
                    || authority.name_epoch != self.mapping.name_epoch
                    || authority.identifier != self.identifier
                    || authority.identifier.name() != self.mapping.name
                {
                    return Err(ValidationError::IdentityMismatch.into());
                }
                if self.authority.as_ref().is_some_and(|before| {
                    authority.mutation_revision < before.mutation_revision
                        || authority.admission_fence < before.admission_fence
                        || authority.property_revision < before.property_revision
                        || (authority.mutation_revision == before.mutation_revision
                            && authority.as_ref() != before)
                }) {
                    return Err(ValidationError::Record.into());
                }
                Some(*authority)
            }
            _ => return Err(ValidationError::Record.into()),
        };
        self.authority = authority;
        self.authority_bytes = bytes;
        Ok(())
    }
}

impl NamespaceDropper {
    pub(super) async fn select_drop_binding(
        &self,
        context: CatalogContext,
        identifier: &NamespaceIdentifier,
    ) -> Result<Option<DropBinding>, CatalogError> {
        let mut parent = None;
        for (index, name) in identifier.components().iter().enumerate() {
            let key = name_key(context.catalog, parent, name)?;
            let Some(mapping_value) = self.creator.names.get(&key.encode()?).await? else {
                return Ok(None);
            };
            let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, &mapping_value.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            if mapping.state != NamespaceMappingState::Published {
                return Ok(None);
            }
            let key = authority_key(context.catalog, mapping.namespace);
            let Some(value) = self.creator.names.get(&key.encode()?).await? else {
                return Ok(None);
            };
            let leaf = index + 1 == identifier.components().len();
            let authority = match StorageRecord::decode(&key, &value.bytes)? {
                StorageRecord::NamespaceAuthority(authority) => authority,
                StorageRecord::RetiredNamespace { .. } if leaf => {
                    return DropBinding::new(identifier.clone(), mapping, mapping_value.bytes, value.bytes)
                        .map(Some);
                }
                StorageRecord::RetiredNamespace { .. } => return Ok(None),
                _ => return Err(ValidationError::Record.into()),
            };
            if authority.catalog != mapping.catalog
                || authority.namespace != mapping.namespace
                || authority.parent != mapping.parent
                || authority.name_epoch != mapping.name_epoch
                || authority.identifier.name() != mapping.name
                || authority.identifier.components() != &identifier.components()[..=index]
            {
                return Ok(None);
            }
            if leaf {
                return DropBinding::new(identifier.clone(), mapping, mapping_value.bytes, value.bytes)
                    .map(Some);
            }
            if authority.lifecycle == NamespaceLifecycle::Tombstone {
                return Ok(None);
            }
            parent = Some(authority.namespace);
        }
        Ok(None)
    }
}
