// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Original-medium verification shared by explicit Rejoin and Online triggers.
use super::Failure;
use crate::{
    error::{err_409, err_502},
    state::AppState,
};
use crowdb_console_shared::deployment::admission::remote_control;
use crowdb_protocol::{
    disk_storage::DiskStorageRecord,
    key::DiskKey,
    mgmt::node::{
        storage::{CheckRetainedDisk, RetainedDiskCondition, RetainedMedium},
        NodeControl,
    },
    DiskIdExt,
};

pub(super) async fn verify(
    state: &AppState,
    key: &DiskKey,
    backing: &DiskStorageRecord,
    condition: RetainedDiskCondition,
) -> Result<(), Failure> {
    let node = super::remote_node(state, key.node_id)?;
    let binding = crate::node::cluster_binding(state)?
        .ok_or_else(|| err_409("Original medium check is waiting for Group 0"))?;
    let observation = remote_control(
        &node,
        &NodeControl::CheckRetainedDisk {
            request: CheckRetainedDisk {
                version: 1,
                cluster_id: binding.bootstrap.cluster_id,
                rack_id: key.rack_id,
                node_id: key.node_id,
                disk_group_id: key.disk_group_id,
                disk_id: key.disk_id.to_display_string(),
                condition,
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
        || medium.disk_id != key.disk_id.to_display_string()
        || medium.backend != backing.backend
        || medium.backing_identity != backing.backing_identity
        || Some(medium.capacity_bytes)
            != backing
                .geometry
                .capacity_units
                .checked_mul(u64::from(backing.geometry.unit_size_bytes))
    {
        return Err(err_409(
            "Original medium could not be verified; disk membership was not changed",
        ));
    }
    Ok(())
}
