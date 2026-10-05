// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    extract::{Path, State},
    Json,
};
use crowdb_console_shared::{
    config::{LocalLaunchSpec, ServerEntry, ServiceType},
    lifecycle,
};
use serde_json::{json, Value};

use super::{operation::Operation, Failure};
use crate::{
    error::{err_400, err_404, err_409, err_500, err_502},
    state::AppState,
};

#[derive(Clone, Copy)]
enum Action {
    Restart,
    Stop,
    Delete,
}

pub(super) async fn restart(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Failure> {
    act(state, id, Action::Restart, false).await
}
pub(super) async fn stop(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Failure> {
    act(state, id, Action::Stop, false).await
}
pub(super) async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, Failure> {
    act(state, id, Action::Delete, false).await
}

pub(super) async fn delete_for_reset(state: AppState, id: String) -> Result<Json<Value>, Failure> {
    act(state, id, Action::Delete, true).await
}

async fn act(state: AppState, id: String, action: Action, reset: bool) -> Result<Json<Value>, Failure> {
    let (entry, launch) = {
        let config = state.config.read().unwrap();
        let entry = config
            .servers
            .iter()
            .find(|entry| entry.id == id)
            .cloned()
            .ok_or_else(|| err_404("Service instance not found"))?;
        if !matches!(
            entry.service_type,
            ServiceType::Chunkdb | ServiceType::Diskio | ServiceType::ChunkKv | ServiceType::AccessServer
        ) {
            return Err(err_400(
                "Use the typed KV or DiskDB lifecycle endpoint for this instance",
            ));
        }
        let launch = config
            .local_launches
            .get(&id)
            .cloned()
            .ok_or_else(|| err_409("Instance has no retained local launch specification"))?;
        (entry, launch)
    };
    let claim = if reset {
        Operation::claim_cleanup
    } else {
        Operation::claim
    };
    let operation = claim(
        &state,
        vec![
            format!("node/{}", entry.node_id.unwrap_or(0)),
            format!("service/{id}"),
        ],
    )?;
    tokio::spawn(async move {
        let _operation = operation;
        run(&state, &entry, &launch, action).await
    })
    .await
    .map_err(|error| err_500(format!("Service operation failed: {error}")))?
}

async fn run(
    state: &AppState,
    entry: &ServerEntry,
    launch: &LocalLaunchSpec,
    action: Action,
) -> Result<Json<Value>, Failure> {
    if matches!(action, Action::Restart)
        && entry.service_type == ServiceType::AccessServer
        && launch.access_health_url().is_none()
    {
        return Err(err_409(
            "Access health listener requires reconciliation before restart",
        ));
    }
    let pid = matching_pid(entry, launch)?;
    // Persist the operator's stop intent before signalling the old process.
    {
        let mut config = state.config.write().unwrap();
        let current = config
            .servers
            .iter_mut()
            .find(|current| current.id == entry.id)
            .ok_or_else(|| err_409("Service was removed during the operation"))?;
        if current.pid != entry.pid {
            return Err(err_409("Service process changed; refresh before retrying"));
        }
        current.auto_start = false;
    }
    state.persist().map_err(|error| err_500(error.to_string()))?;
    let replacement = if matches!(action, Action::Restart) {
        Some(
            lifecycle::restart_local_service(&entry.id, pid.unwrap_or(0), launch)
                .await
                .map_err(|error| err_502(format!("Service restart failed: {error}")))?,
        )
    } else {
        if let Some(pid) = pid {
            tokio::task::spawn_blocking(move || lifecycle::stop_pid(pid))
                .await
                .map_err(|error| err_500(error.to_string()))?
                .map_err(|error| err_502(error.to_string()))?;
        }
        None
    };
    {
        let mut config = state.config.write().unwrap();
        if matches!(action, Action::Delete) {
            config.servers.retain(|current| current.id != entry.id);
            config.local_launches.remove(&entry.id);
        } else if let Some(current) = config.servers.iter_mut().find(|current| current.id == entry.id) {
            current.pid = replacement;
            current.auto_start = replacement.is_some();
        }
    }
    if let Some(pid) = replacement {
        super::publication::publish(state, &entry.id, pid).await?;
    } else {
        state.persist().map_err(|error| err_500(error.to_string()))?;
    }
    Ok(Json(
        json!({"id":entry.id,"pid":replacement,"removed":matches!(action, Action::Delete),"data_preserved":true}),
    ))
}

fn matching_pid(entry: &ServerEntry, launch: &LocalLaunchSpec) -> Result<Option<u32>, Failure> {
    let Some(pid) = entry
        .pid
        .filter(|pid| *pid != 0 && lifecycle::process_is_alive(*pid))
    else {
        return Ok(None);
    };
    let cwd = std::fs::read_link(format!("/proc/{pid}/cwd"))
        .map_err(|error| err_409(format!("Cannot verify service process: {error}")))?;
    let expected = std::fs::canonicalize(&launch.workdir).map_err(|error| err_409(error.to_string()))?;
    if cwd != expected {
        return Err(err_409(
            "Recorded PID belongs to a different workspace; refusing to signal it",
        ));
    }
    let command =
        std::fs::read(format!("/proc/{pid}/cmdline")).map_err(|error| err_409(error.to_string()))?;
    let arguments: Vec<_> = command
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .collect();
    let program = std::fs::canonicalize(&launch.program).map_err(|error| err_409(error.to_string()))?;
    let executable =
        std::fs::read_link(format!("/proc/{pid}/exe")).map_err(|error| err_409(error.to_string()))?;
    // Linux retains the original executable mapping after an atomic upgrade.
    // Keep verifying the exact workspace and arguments before signalling it.
    let replaced_program = executable
        .to_str()
        .and_then(|path| path.strip_suffix(" (deleted)"))
        .is_some_and(|path| std::path::Path::new(path) == program);
    if (executable != program && !replaced_program)
        || arguments.get(1..).map_or(true, |args| {
            args.len() != launch.args.len()
                || args
                    .iter()
                    .zip(&launch.args)
                    .any(|(actual, expected)| *actual != expected.as_bytes())
        })
    {
        return Err(err_409("Recorded PID command does not match the retained launch"));
    }
    Ok(Some(pid))
}
