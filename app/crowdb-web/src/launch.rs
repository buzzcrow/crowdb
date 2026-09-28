// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bare-metal process controls; launch policy never supplies cluster topology.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use crowdb_console_shared::config::web::LaunchRegistry;
use crowdb_console_shared::error::{Error, Result};
use crowdb_console_shared::launch::{LaunchRuntime, ProcessIdentity};
use serde::Serialize;

use crate::error::{map_err, ErrorBody};
use crate::state::AppState;

type HttpResult<T> = std::result::Result<T, (StatusCode, Json<ErrorBody>)>;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/launches", get(list))
        .route("/api/launches/:node/:service/start", post(start))
        .route("/api/launches/:node/:service/restart", post(restart))
        .route("/api/launches/:node/:service/stop", post(stop))
}

fn policy(state: &AppState) -> Result<(LaunchRegistry, LaunchRuntime)> {
    let path = state
        .launch_registry_path
        .as_ref()
        .ok_or_else(|| Error::NotFound {
            kind: "launch registry".into(),
            id: "bare-metal".into(),
        })?;
    Ok((LaunchRegistry::load(path)?, LaunchRuntime::for_registry(path)?))
}

#[derive(Serialize)]
struct LaunchView {
    node_id: u64,
    service_id: String,
    host: String,
    auto_start: bool,
    process: Option<ProcessIdentity>,
}

async fn list(State(state): State<AppState>) -> HttpResult<Json<Vec<LaunchView>>> {
    let (registry, runtime) = policy(&state).map_err(map_err)?;
    let mut views = Vec::new();
    for launch in registry.launches {
        let process = runtime.status(&launch).await.map_err(map_err)?;
        views.push(LaunchView {
            node_id: launch.node_id,
            service_id: launch.service_id,
            host: launch.host,
            auto_start: launch.auto_start,
            process,
        });
    }
    Ok(Json(views))
}

enum Action {
    Start,
    Restart,
    Stop,
}

async fn act(state: &AppState, node: u64, service: &str, action: Action) -> Result<Option<ProcessIdentity>> {
    let (registry, runtime) = policy(state)?;
    let launch = registry
        .launches
        .iter()
        .find(|launch| launch.node_id == node && launch.service_id == service)
        .ok_or_else(|| Error::NotFound {
            kind: "configured launch".into(),
            id: format!("{node}/{service}"),
        })?;
    match action {
        Action::Start => runtime.start(launch).await.map(Some),
        Action::Restart => runtime.restart(launch).await.map(Some),
        Action::Stop => {
            runtime.stop(launch).await?;
            Ok(None)
        }
    }
}

async fn start(
    State(state): State<AppState>,
    Path((node, service)): Path<(u64, String)>,
) -> HttpResult<Json<Option<ProcessIdentity>>> {
    act(&state, node, &service, Action::Start)
        .await
        .map(Json)
        .map_err(map_err)
}

async fn restart(
    State(state): State<AppState>,
    Path((node, service)): Path<(u64, String)>,
) -> HttpResult<Json<Option<ProcessIdentity>>> {
    act(&state, node, &service, Action::Restart)
        .await
        .map(Json)
        .map_err(map_err)
}

async fn stop(
    State(state): State<AppState>,
    Path((node, service)): Path<(u64, String)>,
) -> HttpResult<StatusCode> {
    act(&state, node, &service, Action::Stop).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT)
}
