// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve only the accepted metadata binding; never select an unrelated group.

use crowdb_protocol::mgmt::SystemBootstrapIdentity;

use super::{DefaultMetadataGroup, DefaultPreparation, DefaultStorageBinding};
use crate::{
    error::{Error, Result},
    ops::OpContext,
};

async fn active(ctx: &OpContext, identity: &SystemBootstrapIdentity) -> Result<DefaultPreparation> {
    let (accepted, _) = super::read(ctx, identity)
        .await?
        .ok_or_else(|| Error::Config("ordinary metadata group preparation has not been accepted".into()))?;
    if accepted.retired {
        return Err(Error::Config(
            "ordinary metadata group preparation was explicitly retired".into(),
        ));
    }
    let metadata = &accepted.metadata;
    let actual = ctx
        .sysmd()
        .get_group(metadata.store_id, metadata.group_id)
        .await?
        .ok_or_else(|| Error::Config("accepted ordinary metadata group is not published".into()))?;
    if actual.store_id != metadata.store_id || actual.group_id != metadata.group_id {
        return Err(Error::Config(
            "ordinary metadata group identity differs from its accepted binding".into(),
        ));
    }
    Ok(accepted)
}

/// Resolve the fixed ordinary group without reserving or recreating it.
///
/// # Errors
/// Rejects absent, retired or conflicting preparation/publication and lost authority.
pub async fn metadata_binding(
    ctx: &OpContext,
    identity: &SystemBootstrapIdentity,
) -> Result<DefaultMetadataGroup> {
    Ok(active(ctx, identity).await?.metadata)
}

/// Resolve one admitted node's storage metadata and initial disk-group reservation.
///
/// # Errors
/// Rejects changed rack/node mapping and unavailable or retired metadata authority.
pub async fn storage_binding(
    ctx: &OpContext,
    identity: &SystemBootstrapIdentity,
    rack: u64,
    node: u64,
) -> Result<DefaultStorageBinding> {
    active(ctx, identity)
        .await?
        .storage
        .into_iter()
        .find(|binding| binding.rack_id == rack && binding.node_id == node)
        .ok_or_else(|| Error::Config("node storage is outside the accepted default binding".into()))
}
