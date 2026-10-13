// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated endpoint and rack changes for an allocated node.

use super::admission::{as_node, binding, control_socket, node_key_path, observe, save, AdmitRequest};
use crate::{
    error::{err_409, err_502, map_config_err, map_persist_err, ErrorBody},
    state::AppState,
};
use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::deployment::{
    admission, node_update,
    registry::{self, NodeRecord},
};

pub(crate) async fn update(
    State(state): State<AppState>,
    Json(body): Json<AdmitRequest>,
) -> Result<Json<NodeRecord>, (StatusCode, Json<ErrorBody>)> {
    let _operation =
        crate::services::Operation::claim(&state, vec!["node/admission".into(), "cluster/init".into()])?;
    let binding = binding(&state)?.ok_or_else(|| err_409("An active cluster is required"))?;
    let (host, handshake) = observe(&state, &body).await?;
    if handshake.advertisement.cluster_id.as_deref() != Some(binding.bootstrap.cluster_id.as_str()) {
        return Err(err_409("Node is not bound to this cluster"));
    }
    let ctx = state.op_context().await.map_err(map_config_err)?;
    let (registry, _) = registry::read(ctx.kv())
        .await
        .map_err(map_config_err)?
        .filter(|(registry, _)| registry.cluster_id == binding.bootstrap.cluster_id)
        .ok_or_else(|| err_409("Cluster mapping unavailable"))?;
    let mut record = registry
        .nodes
        .into_iter()
        .find(|node| node.discovery_id == body.discovery_id && node.confirmed && !node.cancelled)
        .ok_or_else(|| err_409("Confirmed node mapping required"))?;
    if record.physical_host_id != handshake.physical_host_id {
        return Err(err_409("Physical host identity changed"));
    }
    record.host = host;
    record.rack_id = body.rack_id;
    record.ssh_user = body.ssh_user;
    record.ssh_port = body.ssh_port;
    let key = node_key_path(&state);
    let mut target = as_node(&record);
    target.ssh_password = body.ssh_password;
    if target.ssh_password.is_none() {
        target.ssh_key = Some(key.to_string_lossy().into_owned());
    }
    let config = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .clone();
    admission::prepare(
        &control_socket(),
        &key,
        &record.operation_id,
        &record.discovery_id,
        &target,
        &config.nodes,
    )
    .await
    .map_err(map_config_err)?;
    let record = node_update::apply(&ctx, &binding.bootstrap.cluster_id, record)
        .await
        .map_err(map_config_err)?;
    let nodes = registry::read(ctx.kv())
        .await
        .map_err(map_config_err)?
        .ok_or_else(|| err_409("Cluster mapping unavailable"))?
        .0
        .nodes;
    save(&state.runtime_root.join("confirmed-nodes.json"), &nodes)?;
    save(
        &state
            .runtime_root
            .join(format!("admission-{}.json", record.discovery_id)),
        &record,
    )?;
    state.reload_group0().await.map_err(map_config_err)?;
    state.persist().map_err(map_persist_err)?;
    Ok(Json(record))
}
