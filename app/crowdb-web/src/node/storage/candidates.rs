// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Retained Removed candidates come from Group 0, even if local media is missing.

use super::Failure;
use crate::{
    error::{err_400, err_502},
    state::AppState,
};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use crowdb_protocol::{disk_storage::DiskStorageRecord, mgmt::node::storage::DiskBackend, DiskIdExt};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageQuery {
    after: Option<String>,
    #[serde(default = "page_limit")]
    limit: u32,
}
fn page_limit() -> u32 {
    32
}

#[derive(Serialize)]
pub(crate) struct RemovedDisk {
    disk_id: String,
    disk_group_id: u64,
    operation_id: String,
    container_path: String,
    backend: DiskBackend,
    backing_identity: String,
    media_type: Option<i32>,
    sparse: bool,
    capacity_units: String,
    unit_size_bytes: u32,
}
impl From<DiskStorageRecord> for RemovedDisk {
    fn from(record: DiskStorageRecord) -> Self {
        Self {
            disk_id: record.disk_id.to_display_string(),
            disk_group_id: record.disk_group_id,
            operation_id: record.operation_id,
            container_path: record.geometry.device_path,
            backend: record.backend,
            backing_identity: record.backing_identity,
            media_type: record.media_type,
            sparse: record.sparse,
            capacity_units: record.geometry.capacity_units.to_string(),
            unit_size_bytes: record.geometry.unit_size_bytes,
        }
    }
}
#[derive(Serialize)]
pub(crate) struct RemovedPage {
    items: Vec<RemovedDisk>,
    next: Option<String>,
}

pub(crate) async fn list(
    State(state): State<AppState>,
    Path(node): Path<u64>,
    Query(query): Query<PageQuery>,
) -> Result<Json<RemovedPage>, Failure> {
    let (hardware, rack) = super::authority::node(&state, node).await?;
    let prefix = DiskStorageRecord::node_prefix(rack, node);
    if !(1..=64).contains(&query.limit)
        || query
            .after
            .as_ref()
            .is_some_and(|after| !after.starts_with(&prefix) || after.len() > 256)
    {
        return Err(err_400("Invalid retained candidate page limit or node cursor"));
    }
    let (records, next) = hardware
        .disk_storage_page(rack, node, query.after.as_deref(), query.limit)
        .await
        .map_err(|error| err_502(error.to_string()))?;
    Ok(Json(RemovedPage {
        items: records
            .into_iter()
            .filter(|record| record.removed)
            .map(Into::into)
            .collect(),
        next,
    }))
}
