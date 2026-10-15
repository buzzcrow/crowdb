// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact retained backing properties without lossy JSON UUID words.

use axum::{
    extract::{Path, State},
    Json,
};
use crowdb_protocol::mgmt::node::storage::DiskBackend;
use serde::Serialize;

use super::Failure;
use crate::{error::err_502, state::AppState};

#[derive(Serialize)]
pub(crate) struct DiskBackingInfo {
    version: u32,
    operation_id: String,
    backend: DiskBackend,
    backing_identity: String,
    media_type: Option<i32>,
    sparse: bool,
    removed: bool,
    container_path: String,
    capacity_units: String,
    unit_size_bytes: u32,
    zone_size_units: String,
    zone_count: u32,
    owner: Option<String>,
    binding: Option<String>,
    observed_at_ms: u64,
}

pub(crate) async fn get(
    State(state): State<AppState>,
    Path((node_id, group, disk)): Path<(u64, u64, String)>,
) -> Result<Json<Option<DiskBackingInfo>>, Failure> {
    let (hardware, key) = super::authority::disk(&state, node_id, group, &disk).await?;
    let rack_id = key.rack_id;
    let Some(record) = hardware
        .disk_storage(&key)
        .await
        .map_err(|error| err_502(error.to_string()))?
    else {
        return Ok(Json(None));
    };
    let owner = hardware
        .get_owner(rack_id, node_id, group)
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let binding = hardware
        .get_bind(rack_id, node_id, group)
        .await
        .map_err(|error| err_502(error.to_string()))?;
    Ok(Json(Some(DiskBackingInfo {
        version: record.version,
        operation_id: record.operation_id,
        backend: record.backend,
        backing_identity: record.backing_identity,
        media_type: record.media_type,
        sparse: record.sparse,
        removed: record.removed,
        container_path: record.geometry.device_path,
        capacity_units: record.geometry.capacity_units.to_string(),
        unit_size_bytes: record.geometry.unit_size_bytes,
        zone_size_units: record.geometry.zone_size_units.to_string(),
        zone_count: record.geometry.zone_count,
        owner: owner.map(|owner| format!("diskdb-{}", owner.instance_id)),
        binding: binding.map(|binding| format!("{}/{}", binding.store_id, binding.group_id)),
        observed_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    })))
}
