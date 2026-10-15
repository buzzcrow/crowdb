// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit rejoin checks the original node medium and restores Offline only.

use super::Failure;
use crate::{
    error::{err_400, err_409, err_502},
    state::AppState,
};
use axum::{
    extract::{Path, State},
    Json,
};
use crowdb_console_shared::deployment::admission::remote_control;
use crowdb_protocol::{
    mgmt::node::{
        storage::{CheckRetainedDisk, RetainedDiskCondition, RetainedMedium},
        NodeControl,
    },
    DiskIdExt,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RejoinRequest {
    removal_operation_id: String,
}
#[derive(Serialize)]
pub(crate) struct RejoinOutcome {
    operation_id: String,
    source_removal_operation_id: String,
    state: super::lifecycle::DiskLifecycle,
}

pub(crate) async fn rejoin(
    State(state): State<AppState>,
    Path((node_id, group, disk)): Path<(u64, u64, String)>,
    Json(body): Json<RejoinRequest>,
) -> Result<Json<RejoinOutcome>, Failure> {
    if body.removal_operation_id.is_empty() || body.removal_operation_id.len() > 128 {
        return Err(err_400("A retained removal operation identity is required"));
    }
    let (hardware, key) = super::authority::disk(&state, node_id, group, &disk).await?;
    let backing = hardware
        .disk_storage(&key)
        .await
        .map_err(|error| err_502(error.to_string()))?
        .ok_or_else(|| err_409("Retained backing is unavailable"))?;
    let receipt = hardware
        .disk_removal_receipt(&key)
        .await
        .map_err(|error| err_502(error.to_string()))?
        .filter(|receipt| receipt.operation_id == body.removal_operation_id)
        .ok_or_else(|| err_409("The removal operation changed; inspect the current candidate"))?;
    let node = super::remote_node(&state, node_id)?;
    let binding =
        crate::node::cluster_binding(&state)?.ok_or_else(|| err_409("Rejoin is waiting for Group 0"))?;
    let observation = remote_control(
        &node,
        &NodeControl::CheckRetainedDisk {
            request: CheckRetainedDisk {
                version: 1,
                cluster_id: binding.bootstrap.cluster_id,
                rack_id: key.rack_id,
                node_id,
                disk_group_id: group,
                disk_id: key.disk_id.to_display_string(),
                condition: RetainedDiskCondition::Removed {
                    operation_id: body.removal_operation_id.clone(),
                },
            },
        },
    )
    .await
    .map_err(|error| {
        err_502(format!(
            "Original medium check failed: {error}; disk membership was not changed"
        ))
    })?;
    let medium: RetainedMedium =
        serde_json::from_value(observation).map_err(|error| err_502(error.to_string()))?;
    if medium.version != 1
        || medium.disk_id != disk
        || medium.backend != backing.backend
        || medium.backing_identity != backing.backing_identity
        || Some(medium.capacity_bytes)
            != backing
                .geometry
                .capacity_units
                .checked_mul(u64::from(backing.geometry.unit_size_bytes))
    {
        return Err(err_409(
            "Original medium could not be verified; membership was not restored",
        ));
    }
    let outcome = hardware
        .rejoin_disk_storage(&key, &receipt.operation_id)
        .await
        .map_err(|error| {
            err_502(format!(
                "Rejoin outcome is unconfirmed: {error}; refresh the same candidate before retrying"
            ))
        })?;
    let observed = super::lifecycle::observe(&hardware, &key).await?;
    Ok(Json(RejoinOutcome {
        operation_id: outcome.operation_id,
        source_removal_operation_id: receipt.operation_id,
        state: observed,
    }))
}
