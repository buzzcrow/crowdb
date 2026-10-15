// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolve exact disk placement from bound hardware authority.

use super::Failure;
use crate::{
    error::{err_400, err_404, err_409, err_502},
    state::AppState,
};
use crowdb_kv_client::HardwareClient;
use crowdb_protocol::{key::DiskKey, DiskIdExt};

pub(super) async fn disk(
    state: &AppState,
    node_id: u64,
    group: u64,
    disk: &str,
) -> Result<(HardwareClient, DiskKey), Failure> {
    let disk_id = crowdb_protocol::common::DiskId::from_display_string(disk).map_err(err_400)?;
    if disk_id.to_display_string() != disk {
        return Err(err_400("Disk identity must use canonical display format"));
    }
    let (hardware, rack_id) = node(state, node_id).await?;
    Ok((
        hardware,
        DiskKey {
            rack_id,
            node_id,
            disk_group_id: group,
            disk_id,
        },
    ))
}

pub(super) async fn node(state: &AppState, node_id: u64) -> Result<(HardwareClient, u64), Failure> {
    if state.node_monitor_url.is_none() {
        return Err(err_404("Node monitor is not configured"));
    }
    let hardware = crate::mgmt::build_hardware_client(state)
        .await
        .ok_or_else(|| err_409("Storage operations are waiting for Group 0"))?;
    let nodes = hardware
        .list_nodes()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let mut location = nodes.iter().filter(|(_, id, _)| *id == node_id);
    let rack_id = location
        .next()
        .map(|(rack, _, _)| *rack)
        .ok_or_else(|| err_404("Node not found"))?;
    if location.next().is_some() {
        return Err(err_409("Node identity has multiple hardware locations"));
    }
    Ok((hardware, rack_id))
}
