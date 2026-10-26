// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    catalog::{CasOutcome, CatalogError, CatalogStore, StoredValue},
    error::ValidationError,
    key::{CatalogId, NamespaceId},
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{authority_key, NamespaceAuthority};

pub(crate) enum NamespaceSlot {
    Retired,
    Existing {
        authority: Box<NamespaceAuthority>,
        stored: StoredValue,
    },
}

pub(crate) async fn retire_absent_namespace(
    store: &dyn CatalogStore,
    catalog: CatalogId,
    namespace: NamespaceId,
) -> Result<NamespaceSlot, CatalogError> {
    let key = authority_key(catalog, namespace);
    let encoded = key.encode()?;
    let bytes = StorageRecord::RetiredNamespace { catalog, namespace }.encode()?;
    match store
        .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
        .await?
    {
        CasOutcome::Applied(_) => Ok(NamespaceSlot::Retired),
        CasOutcome::Conflict(Some(stored)) => match StorageRecord::decode(&key, &stored.bytes)? {
            StorageRecord::RetiredNamespace { .. } => Ok(NamespaceSlot::Retired),
            StorageRecord::NamespaceAuthority(authority) => Ok(NamespaceSlot::Existing { authority, stored }),
            _ => Err(ValidationError::Record.into()),
        },
        CasOutcome::Conflict(None) => Err(CatalogError::Busy),
    }
}

pub(crate) async fn retire_namespace_slot(
    store: &dyn CatalogStore,
    catalog: CatalogId,
    namespace: NamespaceId,
    mut expected: Option<StoredValue>,
) -> Result<(), CatalogError> {
    let key = authority_key(catalog, namespace);
    let encoded = key.encode()?;
    let bytes = StorageRecord::RetiredNamespace { catalog, namespace }.encode()?;
    for _ in 0..16 {
        let before = expected.as_ref().map(|value| value.bytes.as_slice());
        match store
            .compare_exchange(
                &encoded,
                before,
                &bytes,
                mutation_identity(&encoded, before, &bytes),
            )
            .await?
        {
            CasOutcome::Applied(_) => return Ok(()),
            CasOutcome::Conflict(Some(stored)) => match StorageRecord::decode(&key, &stored.bytes)? {
                StorageRecord::RetiredNamespace { .. } => return Ok(()),
                StorageRecord::NamespaceAuthority(_) => expected = Some(stored),
                _ => return Err(ValidationError::Record.into()),
            },
            CasOutcome::Conflict(None) => expected = None,
        }
    }
    Err(CatalogError::Busy)
}
