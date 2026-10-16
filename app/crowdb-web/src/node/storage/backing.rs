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
    #[serde(skip_serializing_if = "Option::is_none")]
    physical_backing: Option<PhysicalBackingInfo>,
}

#[derive(Serialize)]
struct PhysicalBackingInfo {
    host_id: String,
    resource_identity: String,
    capacity_bytes: String,
    usable_bytes: String,
    tail_bytes: String,
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
    let usable = record
        .geometry
        .capacity_units
        .checked_mul(u64::from(record.geometry.unit_size_bytes))
        .ok_or_else(|| err_502("Logical disk capacity overflow"))?;
    let physical_backing = record
        .physical_backing
        .as_ref()
        .map(|backing| {
            Ok::<_, Failure>(PhysicalBackingInfo {
                host_id: backing.physical_host_id.clone(),
                resource_identity: backing.resource_identity.clone(),
                capacity_bytes: backing.capacity_bytes.to_string(),
                usable_bytes: usable.to_string(),
                tail_bytes: backing
                    .capacity_bytes
                    .checked_sub(usable)
                    .ok_or_else(|| err_502("Physical capacity is smaller than usable geometry"))?
                    .to_string(),
            })
        })
        .transpose()?;
    let owner = hardware
        .get_owner(rack_id, node_id, group)
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let binding = hardware
        .get_bind(rack_id, node_id, group)
        .await
        .map_err(|error| err_502(error.to_string()))?;
    Ok(Json(Some(DiskBackingInfo {
        physical_backing,
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
