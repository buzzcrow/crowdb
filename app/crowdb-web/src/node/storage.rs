// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated monitor storage observations, independent of disk registration.

mod authority;
pub(crate) mod backing;
pub(crate) mod candidates;
pub(crate) mod files;
pub(crate) mod lifecycle;
pub(crate) mod rejoin;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::deployment::admission::{local_control, remote_control};
use crowdb_protocol::mgmt::node::{storage::DiskInventory, NodeControl};

use crate::{
    error::{err_404, err_502, map_config_err, ErrorBody},
    state::AppState,
};

type Failure = (StatusCode, Json<ErrorBody>);

pub(crate) async fn local(State(state): State<AppState>) -> Result<Json<DiskInventory>, Failure> {
    if state.node_monitor_url.is_none() {
        return Err(err_404("Node monitor is not configured"));
    }
    let value = local_control(&super::control_socket(), &NodeControl::DiskInventory)
        .await
        .map_err(map_config_err)?;
    serde_json::from_value(value)
        .map(Json)
        .map_err(|error| err_502(error.to_string()))
}

pub(crate) async fn remote(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<DiskInventory>, Failure> {
    let node = remote_node(&state, id)?;
    let value = remote_control(&node, &NodeControl::DiskInventory)
        .await
        .map_err(map_config_err)?;
    serde_json::from_value(value)
        .map(Json)
        .map_err(|error| err_502(error.to_string()))
}

pub(super) fn remote_node(
    state: &AppState,
    id: u64,
) -> Result<crowdb_console_shared::config::NodeEntry, Failure> {
    if state.node_monitor_url.is_none() {
        return Err(err_404("Node monitor is not configured"));
    }
    let mut node = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .node(id)
        .cloned()
        .ok_or_else(|| err_404("Node not found"))?;
    node.ssh_key = Some(super::node_key_path(state).to_string_lossy().into_owned());
    Ok(node)
}
