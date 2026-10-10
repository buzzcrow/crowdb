// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    error::{err_400, err_409, err_502, map_config_err, ErrorBody},
    state::AppState,
};
use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::deployment::{admission, PreparedBootstrap};
use crowdb_protocol::mgmt::node::NodeControl;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CleanupRequest {
    operation_id: String,
    confirm_delete_system_store: bool,
}

#[derive(Default, Serialize, Deserialize)]
struct CleanupProgress {
    operation_id: String,
    completed: Vec<u64>,
    pending: Vec<u64>,
    errors: Vec<String>,
}

pub(crate) async fn cleanup(
    State(state): State<AppState>,
    Json(request): Json<CleanupRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    if !request.confirm_delete_system_store {
        return Err(err_400("Explicit system-store deletion confirmation required"));
    }
    let _operation =
        crate::services::Operation::claim(&state, vec!["cluster/init".into(), "node/admission".into()])?;
    let prepared = state.runtime_root.join("prepared-bootstrap.json");
    let publication = state.runtime_root.join("confirmed-cluster.json");
    let operation = PreparedBootstrap::load(if prepared.exists() {
        &prepared
    } else {
        &publication
    })
    .map_err(map_config_err)?;
    if operation.identity.operation_id != request.operation_id {
        return Err(err_409("Cleanup differs from retained bootstrap operation"));
    }
    let config = cleanup_config(&state, &operation)?;
    let progress_path = state.runtime_root.join("cleanup-progress.json");
    let mut progress: CleanupProgress = if progress_path.exists() {
        serde_json::from_slice(&std::fs::read(&progress_path).map_err(|error| err_502(error.to_string()))?)
            .map_err(|error| err_502(error.to_string()))?
    } else {
        CleanupProgress {
            operation_id: request.operation_id,
            pending: config.nodes.iter().map(|node| node.id).collect(),
            ..CleanupProgress::default()
        }
    };
    if progress.operation_id != operation.identity.operation_id {
        return Err(err_409("Another cleanup remains pending"));
    }
    progress.errors.clear();
    super::save(&progress_path, &progress)?;
    for member in progress.pending.clone() {
        let mut node = config
            .node(member)
            .cloned()
            .ok_or_else(|| err_409("Retained member missing"))?;
        node.ssh_key = Some(super::node_key_path(&state).to_string_lossy().into_owned());
        match admission::remote_control(
            &node,
            &NodeControl::Cleanup {
                bootstrap: operation.identity.clone(),
                confirm_delete_system_store: true,
            },
        )
        .await
        {
            Ok(_) => {
                progress.pending.retain(|id| *id != member);
                progress.completed.push(member);
            }
            Err(error) => progress.errors.push(format!("Node {member}: {error}")),
        }
        super::save(&progress_path, &progress)?;
    }
    if progress.pending.is_empty() {
        super::save(
            &state.runtime_root.join(format!(
                "retired-bootstrap-{}.json",
                operation.identity.operation_id
            )),
            &operation,
        )?;
        for entry in
            std::fs::read_dir(state.runtime_root.as_ref()).map_err(|error| err_502(error.to_string()))?
        {
            let entry = entry.map_err(|error| err_502(error.to_string()))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("admission-")
                || [
                    "prepared-bootstrap.json",
                    "confirmed-cluster.json",
                    "confirmed-nodes.json",
                    "bootstrap-progress.json",
                    "cleanup-progress.json",
                ]
                .contains(&name.as_ref())
            {
                std::fs::remove_file(entry.path()).map_err(|error| err_502(error.to_string()))?;
            }
        }
        *state.config.write().map_err(|error| err_502(error.to_string()))? =
            crowdb_console_shared::config::ConsoleConfig::default();
        state.persist().map_err(|error| err_502(error.to_string()))?;
        std::fs::File::open(state.runtime_root.as_ref())
            .and_then(|directory| directory.sync_all())
            .map_err(|error| err_502(error.to_string()))?;
    }
    Ok(Json(
        serde_json::to_value(progress).map_err(|error| err_502(error.to_string()))?,
    ))
}

fn cleanup_config(
    state: &AppState,
    operation: &PreparedBootstrap,
) -> Result<crowdb_console_shared::ConsoleConfig, (StatusCode, Json<ErrorBody>)> {
    let mut config = operation.intent.to_config();
    if let Ok(bytes) = std::fs::read(state.runtime_root.join("confirmed-nodes.json")) {
        let nodes: Vec<crowdb_console_shared::deployment::registry::NodeRecord> =
            serde_json::from_slice(&bytes).map_err(|error| err_502(error.to_string()))?;
        for node in nodes.into_iter().filter(|node| !node.cancelled) {
            let mut target = super::admission::as_node(&node);
            target.ssh_credential_ref = Some("id_ed25519".into());
            if let Some(current) = config.nodes.iter_mut().find(|current| current.id == node.node_id) {
                *current = target;
            } else {
                config.nodes.push(target);
            }
        }
    }
    Ok(config)
}
