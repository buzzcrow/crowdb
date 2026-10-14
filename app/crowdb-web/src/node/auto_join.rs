// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Workspace policy schedules admissions; accepted bootstrap belongs to monitors.

mod runner;

use std::fs;

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    error::{err_400, err_409, err_502, ErrorBody},
    state::AppState,
};

type Failure = (StatusCode, Json<ErrorBody>);

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Policy {
    version: u32,
    enabled: bool,
    /// None means the uncommitted local workspace; never a discovered foreign cluster.
    target_cluster_id: Option<String>,
    #[serde(default)]
    local_draft_selected: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            version: 1,
            enabled: true,
            target_cluster_id: None,
            local_draft_selected: false,
        }
    }
}

fn read(state: &AppState) -> Result<Policy, Failure> {
    let path = state.runtime_root.join("auto-join.json");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Policy::default()),
        Err(error) => return Err(err_502(error.to_string())),
    };
    if !metadata.is_file() || metadata.len() > 4096 {
        return Err(err_409("Invalid Auto join policy"));
    }
    let policy: Policy = serde_json::from_slice(&fs::read(path).map_err(|error| err_502(error.to_string()))?)
        .map_err(|error| err_502(error.to_string()))?;
    if policy.version != 1 {
        return Err(err_409("Unsupported Auto join policy version"));
    }
    Ok(policy)
}

fn supported(state: &AppState) -> Result<(), Failure> {
    if state.node_monitor_url.is_none() || state.managed_mode {
        return Err(err_409("Auto join requires a standard node workspace"));
    }
    Ok(())
}

pub(crate) async fn status(State(state): State<AppState>) -> Result<Json<Value>, Failure> {
    supported(&state)?;
    let policy = read(&state)?;
    let path = state.runtime_root.join("auto-join-status.json");
    match fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.is_file() || metadata.len() > 65536 => {
            return Err(err_409("Invalid Auto join observation"));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(err_502(error.to_string())),
    }
    let observation: Value = match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| err_502(error.to_string()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            json!({"phase": "waiting", "members": []})
        }
        Err(error) => return Err(err_502(error.to_string())),
    };
    Ok(Json(
        json!({"enabled": policy.enabled, "target_cluster_id": policy.target_cluster_id,
        "mode": "standard", "required_voters": 3, "observation": observation}),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Update {
    enabled: bool,
    target: Option<Target>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Target {
    LocalDraft,
}

pub(crate) async fn update(
    State(state): State<AppState>,
    Json(body): Json<Update>,
) -> Result<Json<Value>, Failure> {
    supported(&state)?;
    if state.readonly {
        return Err(err_400("Workspace is readonly"));
    }
    let mut resources = vec!["node/auto-join".into()];
    if body.target.is_some() {
        resources.extend(["cluster/init".into(), "node/admission".into()]);
    }
    let _operation = crate::services::Operation::claim(&state, resources)?;
    let mut policy = read(&state)?;
    if body.target.is_some() {
        if super::status(State(state.clone())).await.0["phase"] != "unbound_draft" {
            return Err(err_409("A local draft requires an unbound workspace"));
        }
        if let Some(target) = &policy.target_cluster_id {
            let root = state
                .runtime_root
                .parent()
                .ok_or_else(|| err_409("Node root is absent"))?;
            let path = root.join("node-cleaned.json");
            let metadata = fs::symlink_metadata(&path)
                .map_err(|_| err_409("Recover or explicitly clean up the retained target first"))?;
            if !metadata.is_file() || metadata.len() > 4096 {
                return Err(err_409("Invalid completed cleanup record"));
            }
            let cleaned: crowdb_protocol::mgmt::SystemBootstrapIdentity =
                serde_json::from_slice(&fs::read(path).map_err(|error| err_502(error.to_string()))?)
                    .map_err(|error| err_502(error.to_string()))?;
            if cleaned.cluster_id != *target {
                return Err(err_409("Completed cleanup belongs to another target"));
            }
        }
        policy.target_cluster_id = None;
        policy.local_draft_selected = true;
    }
    policy.enabled = body.enabled;
    super::save(&state.runtime_root.join("auto-join.json"), &policy)?;
    status(State(state.clone())).await
}

/// Keep policy execution independent of browser connections. The monitor owns
/// any initialization already accepted when this worker stops.
pub(crate) fn start(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if supported(&state).is_err() || state.readonly {
            return;
        }
        runner::run(state).await;
    })
}
