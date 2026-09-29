// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bare-metal hardware routes backed by confirmed Group 0 state.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::config::{NodeEntry, RackEntry};
use crowdb_console_shared::ops::hardware;
use serde::Deserialize;

use crate::error::ErrorBody;
use crate::managed_logical::api_error;
use crate::state::AppState;

type ApiError = (StatusCode, Json<ErrorBody>);

#[derive(Deserialize)]
pub(crate) struct CreateRack {
    id: u64,
    #[serde(default)]
    name: String,
}

#[derive(Deserialize)]
pub(crate) struct NodeFilter {
    rack_id: Option<u64>,
}

pub(crate) async fn list_racks(State(state): State<AppState>) -> Result<Json<Vec<RackEntry>>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_racks_from_group0(&ctx)
        .await
        .map(Json)
        .map_err(api_error)
}

pub(crate) async fn add_rack(
    State(state): State<AppState>,
    Json(body): Json<CreateRack>,
) -> Result<(StatusCode, Json<RackEntry>), ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    let rack = hardware::add_rack_to_group0(&ctx, body.id, &body.name)
        .await
        .map_err(api_error)?;
    Ok((StatusCode::CREATED, Json(rack)))
}

pub(crate) async fn remove_rack(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::remove_rack_from_group0(&ctx, id)
        .await
        .map_err(api_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn list_nodes(
    State(state): State<AppState>,
    Query(filter): Query<NodeFilter>,
) -> Result<Json<Vec<NodeEntry>>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_nodes_from_group0(&ctx, filter.rack_id)
        .await
        .map(Json)
        .map_err(api_error)
}

pub(crate) async fn add_node(
    State(state): State<AppState>,
    Json(node): Json<NodeEntry>,
) -> Result<(StatusCode, Json<NodeEntry>), ApiError> {
    if node.ssh_key.is_some() || node.ssh_password.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorBody {
                error: "inline SSH secrets are not accepted; use ssh_credential_ref".into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(api_error)?;
    let node = hardware::add_node_to_group0(&ctx, node)
        .await
        .map_err(api_error)?;
    Ok((StatusCode::CREATED, Json(node)))
}

pub(crate) async fn remove_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::remove_node_from_group0(&ctx, id)
        .await
        .map_err(api_error)?;
    Ok(StatusCode::NO_CONTENT)
}
