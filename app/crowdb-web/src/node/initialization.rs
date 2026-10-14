// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Submit bootstrap to its monitor owner and read durable progress.

use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::deployment::{admission::remote_control, PreparedBootstrap};
use crowdb_protocol::mgmt::node::{NodeControl, NodeServiceCredentials};
use serde_json::{json, Value};

use crate::{
    error::{err_404, err_502, map_config_err, ErrorBody},
    state::AppState,
};

type Failure = (StatusCode, Json<ErrorBody>);

pub(crate) async fn trigger(
    state: &AppState,
    nodes: &[u64],
    create_data_group: Option<bool>,
) -> Result<Value, Failure> {
    let config = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .clone();
    let operation = PreparedBootstrap::open_with_nodes(
        &state.runtime_root.join("prepared-bootstrap.json"),
        &config,
        nodes,
        super::records(state)?,
    )
    .map_err(map_config_err)?;
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(state.runtime_root.as_ref())
        .map_err(|error| err_502(error.to_string()))?;
    control(
        state,
        &operation,
        NodeControl::Initialize {
            manifest: serde_json::to_value(&operation).map_err(|error| err_502(error.to_string()))?,
            credentials: NodeServiceCredentials {
                environment: credentials.server_env(),
            },
            create_data_group,
        },
    )
    .await
}

pub(crate) async fn status(State(state): State<AppState>) -> Result<Json<Value>, Failure> {
    if state.node_monitor_url.is_none() {
        return Err(err_404("Node monitor is not configured"));
    }
    let path = state.runtime_root.join("prepared-bootstrap.json");
    if !path.exists() {
        return Ok(Json(json!({"phase": "idle"})));
    }
    let operation = PreparedBootstrap::load(&path).map_err(map_config_err)?;
    control(&state, &operation, NodeControl::Initialization)
        .await
        .map(Json)
}

async fn control(
    state: &AppState,
    operation: &PreparedBootstrap,
    request: NodeControl,
) -> Result<Value, Failure> {
    let initiator = operation
        .nodes
        .iter()
        .filter(|node| operation.intent.members().contains(&node.node_id))
        .min_by_key(|node| &node.discovery_id)
        .ok_or_else(|| err_502("No authenticated bootstrap member"))?;
    let mut node = operation
        .intent
        .to_config()
        .node(initiator.node_id)
        .cloned()
        .ok_or_else(|| err_502("Bootstrap initiator is missing from topology"))?;
    node.ssh_key = Some(super::node_key_path(state).to_string_lossy().into_owned());
    remote_control(&node, &request).await.map_err(map_config_err)
}
