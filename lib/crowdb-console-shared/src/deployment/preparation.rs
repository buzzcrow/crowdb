// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One Group-0-owned default preparation, shared by all authorized node runners.

mod binding;
mod record;
pub use binding::{chunk_kv_binding, metadata_binding, storage_binding};

pub use record::{
    DefaultChunkKvBootstrap, DefaultMetadataGroup, DefaultPreparation, DefaultReplica, DefaultStorageBinding,
};

use crowdb_kv_client::{Error as KvError, GetOutcome, ReadMode};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;

use super::{PreparedBootstrap, CLUSTER_KEY};
use crate::{
    error::{Error, Result},
    ops::OpContext,
};

pub const DEFAULT_PREPARATION_KEY: &[u8] = b"/deployment/default-preparation";

pub(crate) async fn check_group_creation(ctx: &OpContext, store: u64, group: u64) -> Result<()> {
    if for_group(ctx, store, group)
        .await?
        .is_some_and(|current| !current.retired)
    {
        return Err(Error::Config(
            "group is reserved by accepted default preparation".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn retire_group(ctx: &OpContext, store: u64, group: u64) -> Result<()> {
    if let Some(current) = for_group(ctx, store, group).await? {
        retire(ctx, &current.bootstrap, &current.operation_id).await?;
    }
    Ok(())
}

async fn for_group(ctx: &OpContext, store: u64, group: u64) -> Result<Option<DefaultPreparation>> {
    if (store, group) != (0, 1) {
        return Ok(None);
    }
    let GetOutcome::Found { value, .. } = ctx
        .kv()
        .get(0, 0, DEFAULT_PREPARATION_KEY, ReadMode::Linearizable, None)
        .await?
    else {
        return Ok(None);
    };
    if value.len() > 32768 {
        return Err(Error::Config("default preparation exceeds its size bound".into()));
    }
    let current: DefaultPreparation = serde_json::from_slice(&value).map_err(config_error)?;
    let bootstrap = confirmed(ctx, &current.bootstrap).await?;
    current.validate(&bootstrap)?;
    Ok(Some(current))
}

/// Reserve fixed identities before creating any ordinary group, disks or services.
///
/// # Errors
/// Rejects unconfirmed/changed authority, malformed or retired operations and
/// preexisting objects without this operation's ownership. CAS ambiguity is
/// resolved through authoritative readback, never a new local operation.
pub async fn reserve(ctx: &OpContext, identity: &SystemBootstrapIdentity) -> Result<DefaultPreparation> {
    tokio::time::timeout(std::time::Duration::from_secs(3), reserve_inner(ctx, identity))
        .await
        .map_err(|_| Error::Config("default preparation authority reservation timed out".into()))?
}

async fn reserve_inner(ctx: &OpContext, identity: &SystemBootstrapIdentity) -> Result<DefaultPreparation> {
    let bootstrap = confirmed(ctx, identity).await?;
    let candidate = DefaultPreparation::new(&bootstrap)?;
    let bytes = serde_json::to_vec(&candidate).map_err(config_error)?;
    for _ in 0..10 {
        if let Some((current, _)) = read_record(ctx, &bootstrap).await? {
            if current.retired {
                return Err(Error::Config("default preparation was explicitly retired".into()));
            }
            return Ok(current);
        }
        if let Err(error) = check_unowned_objects(ctx, &candidate).await {
            // A winning runner may have published and begun creating objects
            // between our absent read and the ownership check.
            if let Some((current, _)) = read_record(ctx, &bootstrap).await? {
                if !current.retired {
                    return Ok(current);
                }
            }
            return Err(error);
        }
        match ctx.kv().put_cas(0, 0, DEFAULT_PREPARATION_KEY, &bytes, 0).await {
            Ok(_) => return Ok(candidate),
            Err(KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(KvError::OutcomeUnknown.into())
}

/// Read the accepted preparation without reserving or repairing anything.
///
/// # Errors
/// Rejects unavailable/conflicting authority and invalid committed records.
pub async fn read(
    ctx: &OpContext,
    identity: &SystemBootstrapIdentity,
) -> Result<Option<(DefaultPreparation, u64)>> {
    let bootstrap = confirmed(ctx, identity).await?;
    read_record(ctx, &bootstrap).await
}

/// Permanently retire this default operation before explicit topology deletion.
///
/// # Errors
/// Rejects changed operation identity, missing authority and failed publication.
pub async fn retire(ctx: &OpContext, identity: &SystemBootstrapIdentity, operation: &str) -> Result<()> {
    let bootstrap = confirmed(ctx, identity).await?;
    for _ in 0..10 {
        let (mut current, revision) = read_record(ctx, &bootstrap)
            .await?
            .ok_or_else(|| Error::Config("default preparation has not been accepted".into()))?;
        if current.operation_id != operation {
            return Err(Error::Config("default preparation operation differs".into()));
        }
        if current.retired {
            return Ok(());
        }
        current.retired = true;
        let bytes = serde_json::to_vec(&current).map_err(config_error)?;
        match ctx
            .kv()
            .put_cas(0, 0, DEFAULT_PREPARATION_KEY, &bytes, revision)
            .await
        {
            Ok(_) => return Ok(()),
            Err(KvError::CasFailed { .. } | KvError::CasBusy | KvError::OutcomeUnknown) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(KvError::OutcomeUnknown.into())
}

async fn confirmed(ctx: &OpContext, identity: &SystemBootstrapIdentity) -> Result<PreparedBootstrap> {
    let GetOutcome::Found { value, .. } = ctx
        .kv()
        .get(0, 0, CLUSTER_KEY, ReadMode::Linearizable, None)
        .await?
    else {
        return Err(Error::Config(
            "default preparation requires committed cluster authority".into(),
        ));
    };
    if value.len() > 1024 * 1024 {
        return Err(Error::Config(
            "committed cluster identity exceeds its size bound".into(),
        ));
    }
    let bootstrap: PreparedBootstrap = serde_json::from_slice(&value).map_err(config_error)?;
    bootstrap.validate()?;
    if bootstrap.identity != *identity {
        return Err(Error::Config(
            "default preparation cluster identity differs".into(),
        ));
    }
    Ok(bootstrap)
}

async fn read_record(
    ctx: &OpContext,
    bootstrap: &PreparedBootstrap,
) -> Result<Option<(DefaultPreparation, u64)>> {
    let GetOutcome::Found { value, revision } = ctx
        .kv()
        .get(0, 0, DEFAULT_PREPARATION_KEY, ReadMode::Linearizable, None)
        .await?
    else {
        return Ok(None);
    };
    if value.len() > 32768 {
        return Err(Error::Config("default preparation exceeds its size bound".into()));
    }
    let current: DefaultPreparation = serde_json::from_slice(&value).map_err(config_error)?;
    current.validate(bootstrap)?;
    Ok(Some((current, revision)))
}

async fn check_unowned_objects(ctx: &OpContext, candidate: &DefaultPreparation) -> Result<()> {
    let groups = ctx.sysmd().list_disk_groups().await?;
    for binding in &candidate.storage {
        let reservation =
            crowdb_protocol::disk_storage::DiskGroupIdReservation::key_for(binding.disk_group_id);
        if groups.iter().any(|group| group.dg_id == binding.disk_group_id)
            || matches!(
                ctx.kv()
                    .get(0, 0, reservation.as_bytes(), ReadMode::Linearizable, None)
                    .await?,
                GetOutcome::Found { .. }
            )
        {
            return Err(Error::Config(
                "default disk group ID already has a global owner".into(),
            ));
        }
    }
    let metadata = &candidate.metadata;
    if ctx
        .membership()
        .read(metadata.store_id, metadata.group_id)
        .await?
        .is_some()
    {
        return Err(Error::Config(
            "default metadata membership already has an owner".into(),
        ));
    }
    if ctx
        .sysmd()
        .get_group(metadata.store_id, metadata.group_id)
        .await?
        .is_some()
        || !ctx
            .sysmd()
            .list_replicas_in_group(metadata.store_id, metadata.group_id)
            .await?
            .is_empty()
    {
        return Err(Error::Config(
            "default metadata group already exists without accepted ownership".into(),
        ));
    }
    for binding in &candidate.storage {
        if ctx
            .sysmd()
            .get_disk_group(binding.rack_id, binding.node_id, binding.disk_group_id)
            .await?
            .is_some()
            || ctx
                .sysmd()
                .get_bind(binding.rack_id, binding.node_id, binding.disk_group_id)
                .await?
                .is_some()
        {
            return Err(Error::Config(
                "default storage binding already exists without accepted ownership".into(),
            ));
        }
    }
    Ok(())
}

fn config_error(error: impl std::fmt::Display) -> Error {
    Error::Config(error.to_string())
}
