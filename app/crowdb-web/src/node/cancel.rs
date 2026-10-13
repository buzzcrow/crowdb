// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    error::{err_400, err_409, err_502, map_config_err, ErrorBody},
    state::AppState,
};
use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::{
    config::NodeEntry,
    deployment::{admission, registry},
};
use crowdb_protocol::mgmt::node::NodeControl;
use serde::{Deserialize, Serialize};

#[derive(Default, Deserialize, Serialize)]
struct Progress {
    operation_id: String,
    completed: Vec<u64>,
    local_complete: bool,
    pending: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancelRequest {
    discovery_id: String,
    operation_id: String,
}

pub(crate) async fn cancel(
    State(state): State<AppState>,
    Json(request): Json<CancelRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let id = uuid::Uuid::parse_str(&request.discovery_id).map_err(|_| err_400("Invalid discovery UUID"))?;
    let _operation =
        crate::services::Operation::claim(&state, vec!["node/admission".into(), "cluster/init".into()])?;
    if super::admission::binding(&state)?.is_none()
        && state.runtime_root.join("prepared-bootstrap.json").exists()
    {
        return Err(err_409("Submitted bootstrap requires explicit system cleanup"));
    }
    let path = state.runtime_root.join(format!("admission-{id}.json"));
    let binding = super::admission::binding(&state)?;
    let mut record: registry::NodeRecord = if let Some(binding) = &binding {
        let (registry, _) = registry::read(state.kv_client().await.as_ref())
            .await
            .map_err(map_config_err)?
            .filter(|(registry, _)| registry.cluster_id == binding.bootstrap.cluster_id)
            .ok_or_else(|| err_409("Cluster mapping unavailable"))?;
        registry
            .nodes
            .into_iter()
            .find(|node| node.discovery_id == request.discovery_id)
            .ok_or_else(|| err_409("Admission mapping not found"))?
    } else {
        serde_json::from_slice(&std::fs::read(&path).map_err(|error| err_502(error.to_string()))?)
            .map_err(|error| err_502(error.to_string()))?
    };
    if record.operation_id != request.operation_id || record.confirmed {
        return Err(err_409(
            "Admission is confirmed or has another operation identity",
        ));
    }
    if let Some(binding) = &binding {
        registry::cancel(
            state.kv_client().await.as_ref(),
            &binding.bootstrap.cluster_id,
            &record.discovery_id,
            &record.operation_id,
        )
        .await
        .map_err(map_config_err)?;
    }
    record.cancelled = true;
    super::save(&path, &record)?;
    if let Some(binding) = &binding {
        let mut target = super::admission::as_node(&record);
        target.ssh_key = Some(super::node_key_path(&state).to_string_lossy().into_owned());
        let command = NodeControl::CancelAdmission {
            bootstrap: binding.bootstrap.clone(),
            node_id: record.node_id,
            admission: crowdb_protocol::mgmt::node::NodeAdmissionGrant {
                operation_id: record.operation_id.clone(),
                management_seeds: binding.management_seeds.clone(),
            },
        };
        if let Err(error) = admission::remote_control(&target, &command).await {
            return Ok(Json(serde_json::json!({"cancelled": true,
                "pending": [format!("Node {}: {error}", record.node_id)]})));
        }
    }
    let pending = remove_keys(&state, &record).await?;
    if pending.is_empty() {
        if let Some(binding) = binding {
            registry::complete_cancellation(
                state.kv_client().await.as_ref(),
                &binding.bootstrap.cluster_id,
                &record,
            )
            .await
            .map_err(map_config_err)?;
        }
        let mut config = state.config.write().map_err(|error| err_502(error.to_string()))?;
        config.nodes.retain(|node| node.id != record.node_id);
        config
            .servers
            .retain(|server| server.node_id != Some(record.node_id));
        drop(config);
        state.persist().map_err(|error| err_502(error.to_string()))?;
    }
    Ok(Json(serde_json::json!({"cancelled": true, "pending": pending})))
}

async fn remove_keys(
    state: &AppState,
    record: &registry::NodeRecord,
) -> Result<Vec<String>, (StatusCode, Json<ErrorBody>)> {
    let command = NodeControl::RemoveKey {
        operation_id: record.operation_id.clone(),
    };
    let mut nodes = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .nodes
        .clone();
    if !nodes
        .iter()
        .any(|node| node.host == record.host && node.ssh_port == record.ssh_port)
    {
        nodes.push(NodeEntry {
            id: record.node_id,
            rack_id: record.rack_id,
            host: record.host.clone(),
            ssh_port: record.ssh_port,
            ssh_user: record.ssh_user.clone(),
            ssh_key: None,
            ssh_password: None,
            ssh_credential_ref: Some("id_ed25519".into()),
        });
    }
    let progress_path = state
        .runtime_root
        .join(format!("cancel-{}.json", record.discovery_id));
    let mut progress: Progress = match std::fs::read(&progress_path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| err_502(error.to_string()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Progress::default(),
        Err(error) => return Err(err_502(error.to_string())),
    };
    if progress.operation_id != record.operation_id {
        progress = Progress {
            operation_id: record.operation_id.clone(),
            ..Progress::default()
        };
    }
    progress.pending.clear();
    for mut node in nodes {
        if progress.completed.contains(&node.id) {
            continue;
        }
        node.ssh_key = Some(super::node_key_path(state).to_string_lossy().into_owned());
        match admission::remote_control(&node, &command).await {
            Ok(_) => {
                progress.completed.push(node.id);
            }
            Err(error) => progress.pending.push(format!("Node {}: {error}", node.id)),
        }
        super::save(&progress_path, &progress)?;
    }
    if !progress.local_complete {
        match admission::local_control(&super::admission::control_socket(), &command).await {
            Ok(_) => progress.local_complete = true,
            Err(error) => progress.pending.push(error.to_string()),
        }
    }
    super::save(&progress_path, &progress)?;
    Ok(progress.pending)
}
