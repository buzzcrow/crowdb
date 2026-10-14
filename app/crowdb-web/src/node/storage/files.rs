// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authorized file creation is accepted and executed by the owning monitor.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::deployment::admission::remote_control;
use crowdb_protocol::mgmt::node::{
    storage::{
        FileDiskAdmission, FilePreparation, FilePreparationPage, PrepareDiskFile,
        DEFAULT_FILE_CAPACITY_BYTES, DEFAULT_FILE_RESERVE_BYTES,
    },
    NodeControl,
};
use serde::Deserialize;

use crate::{
    error::{err_400, err_409, err_502, map_config_err},
    state::AppState,
};

use super::{remote_node, Failure};

const UNIT_SIZE: u32 = 1024 * 1024;
const ZONE_SIZE: u64 = 32 * 1024 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateFileDisk {
    operation_id: String,
    disk_group_id: u64,
    #[serde(default = "default_capacity")]
    capacity_bytes: u64,
    #[serde(default)]
    sparse: bool,
    #[serde(default = "default_reserve")]
    reserve_bytes: u64,
}
fn default_capacity() -> u64 {
    DEFAULT_FILE_CAPACITY_BYTES
}
fn default_reserve() -> u64 {
    DEFAULT_FILE_RESERVE_BYTES
}

#[derive(Deserialize)]
pub(crate) struct FilePageQuery {
    after: Option<String>,
    #[serde(default = "page_limit")]
    limit: u32,
}
fn page_limit() -> u32 {
    32
}

pub(crate) async fn create(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<CreateFileDisk>,
) -> Result<(StatusCode, Json<FilePreparation>), Failure> {
    let node = remote_node(&state, node_id)?;
    canonical_operation(&body.operation_id)?;
    if body.capacity_bytes == 0
        || body.capacity_bytes > i64::MAX as u64
        || body.capacity_bytes % u64::from(UNIT_SIZE) != 0
    {
        return Err(err_400(
            "File capacity must be a positive multiple of 1 MiB within signed 64-bit bounds",
        ));
    }
    let binding = crate::node::cluster_binding(&state)?
        .ok_or_else(|| err_409("File disk creation is waiting for Group 0"))?;
    let request = PrepareDiskFile {
        operation_id: body.operation_id,
        cluster_id: binding.bootstrap.cluster_id,
        node_id,
        capacity_bytes: body.capacity_bytes,
        sparse: body.sparse,
        reserve_bytes: body.reserve_bytes,
        admission: Some(FileDiskAdmission {
            rack_id: node.rack_id,
            disk_group_id: body.disk_group_id,
            unit_size_bytes: UNIT_SIZE,
            zone_size_bytes: ZONE_SIZE.min(body.capacity_bytes),
            media_type: None,
        }),
    };
    let value = remote_control(&node, &NodeControl::PrepareDiskFile { request })
        .await
        .map_err(map_config_err)?;
    let accepted = serde_json::from_value(value).map_err(|error| err_502(error.to_string()))?;
    Ok((StatusCode::ACCEPTED, Json(accepted)))
}

pub(crate) async fn status(
    State(state): State<AppState>,
    Path((node_id, operation_id)): Path<(u64, String)>,
) -> Result<Json<FilePreparation>, Failure> {
    canonical_operation(&operation_id)?;
    let node = remote_node(&state, node_id)?;
    let value = remote_control(&node, &NodeControl::DiskFilePreparation { operation_id })
        .await
        .map_err(map_config_err)?;
    serde_json::from_value(value)
        .map(Json)
        .map_err(|error| err_502(error.to_string()))
}

pub(crate) async fn list(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Query(query): Query<FilePageQuery>,
) -> Result<Json<FilePreparationPage>, Failure> {
    let node = remote_node(&state, node_id)?;
    if let Some(after) = &query.after {
        canonical_operation(after)?;
    }
    if query.limit == 0 || query.limit > 32 {
        return Err(err_400("File operation page limit must be 1..32"));
    }
    let value = remote_control(
        &node,
        &NodeControl::DiskFilePreparations {
            after: query.after,
            limit: query.limit,
        },
    )
    .await
    .map_err(map_config_err)?;
    serde_json::from_value(value)
        .map(Json)
        .map_err(|error| err_502(error.to_string()))
}

fn canonical_operation(id: &str) -> Result<(), Failure> {
    let parsed = uuid::Uuid::parse_str(id).map_err(|_| err_400("Operation ID must be a canonical UUID"))?;
    if parsed.to_string() != id {
        return Err(err_400("Operation ID must be a canonical UUID"));
    }
    Ok(())
}
