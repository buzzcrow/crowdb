// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bare-metal hardware routes backed by confirmed Group 0 state.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::config::{DiskEntry, DiskGroupEntry, NodeEntry, RackEntry};
use crowdb_console_shared::ops::hardware;
use serde::Deserialize;

use crate::error::ErrorBody;
use crate::managed_logical::api_error;
use crate::state::AppState;
use crowdb_console_shared::error::Error as ConsoleError;

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

#[derive(Deserialize)]
pub(crate) struct CreateDiskGroup {
    id: u64,
    #[serde(default)]
    name: String,
}

pub(crate) async fn list_racks(State(state): State<AppState>) -> Result<Json<Vec<RackEntry>>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_racks_from_group0(&ctx)
        .await
        .map(Json)
        .map_err(api_error)
}

pub(crate) async fn get_rack(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<RackEntry>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_racks_from_group0(&ctx)
        .await
        .map_err(api_error)?
        .into_iter()
        .find(|rack| rack.id == id)
        .map(Json)
        .ok_or_else(|| {
            api_error(ConsoleError::NotFound {
                kind: "rack".into(),
                id: id.to_string(),
            })
        })
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

pub(crate) async fn list_rack_nodes(
    State(state): State<AppState>,
    Path(rack_id): Path<u64>,
) -> Result<Json<Vec<NodeEntry>>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_nodes_from_group0(&ctx, Some(rack_id))
        .await
        .map(Json)
        .map_err(api_error)
}

pub(crate) async fn get_node(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<Json<NodeEntry>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_nodes_from_group0(&ctx, None)
        .await
        .map_err(api_error)?
        .into_iter()
        .find(|node| node.id == id)
        .map(Json)
        .ok_or_else(|| {
            api_error(ConsoleError::NotFound {
                kind: "node".into(),
                id: id.to_string(),
            })
        })
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

pub(crate) async fn list_disk_groups(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<Vec<DiskGroupEntry>>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_disk_groups_from_group0(&ctx, node_id)
        .await
        .map(Json)
        .map_err(api_error)
}

pub(crate) async fn get_disk_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(u64, u64)>,
) -> Result<Json<DiskGroupEntry>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_disk_groups_from_group0(&ctx, node_id)
        .await
        .map_err(api_error)?
        .into_iter()
        .find(|group| group.id == dg_id)
        .map(Json)
        .ok_or_else(|| {
            api_error(ConsoleError::NotFound {
                kind: "disk_group".into(),
                id: dg_id.to_string(),
            })
        })
}

pub(crate) async fn add_disk_group(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<CreateDiskGroup>,
) -> Result<(StatusCode, Json<DiskGroupEntry>), ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    let group = hardware::add_disk_group_to_group0(&ctx, node_id, body.id, &body.name)
        .await
        .map_err(api_error)?;
    Ok((StatusCode::CREATED, Json(group)))
}

pub(crate) async fn remove_disk_group(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(u64, u64)>,
) -> Result<StatusCode, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::remove_disk_group_from_group0(&ctx, node_id, dg_id)
        .await
        .map_err(api_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn list_disks(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(u64, u64)>,
) -> Result<Json<Vec<DiskEntry>>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_disks_from_group0(&ctx, node_id, dg_id)
        .await
        .map(Json)
        .map_err(api_error)
}

pub(crate) async fn get_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id, disk_id)): Path<(u64, u64, String)>,
) -> Result<Json<DiskEntry>, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::list_disks_from_group0(&ctx, node_id, dg_id)
        .await
        .map_err(api_error)?
        .into_iter()
        .find(|disk| disk.disk_id == disk_id)
        .map(Json)
        .ok_or_else(|| {
            api_error(ConsoleError::NotFound {
                kind: "disk".into(),
                id: disk_id,
            })
        })
}

pub(crate) async fn add_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id)): Path<(u64, u64)>,
    Json(body): Json<hardware::AddDiskInput>,
) -> Result<(StatusCode, Json<DiskEntry>), ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    let disk = hardware::add_disk_to_group0(&ctx, node_id, dg_id, &body)
        .await
        .map_err(api_error)?;
    Ok((StatusCode::CREATED, Json(disk)))
}

pub(crate) async fn remove_disk(
    State(state): State<AppState>,
    Path((node_id, dg_id, disk_id)): Path<(u64, u64, String)>,
) -> Result<StatusCode, ApiError> {
    let ctx = state.op_context().await.map_err(api_error)?;
    hardware::remove_disk_from_group0(&ctx, node_id, dg_id, &disk_id)
        .await
        .map_err(api_error)?;
    Ok(StatusCode::NO_CONTENT)
}
