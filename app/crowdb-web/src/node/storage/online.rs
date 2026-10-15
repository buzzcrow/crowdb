// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! UI triggers durable validation; the current `DiskDB` owner completes it.
use super::{lifecycle::Revision, Failure};
use crate::{
    error::{err_409, err_502},
    state::AppState,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_protocol::{
    disk_storage::{DiskOnlineRecord, DiskOnlineState},
    mgmt::node::storage::RetainedDiskCondition,
};
use serde::Serialize;

#[derive(Serialize)]
pub(crate) struct OnlineOperation {
    operation_id: String,
    offline_revision: String,
    state: DiskOnlineState,
    error: Option<String>,
}
impl From<DiskOnlineRecord> for OnlineOperation {
    fn from(record: DiskOnlineRecord) -> Self {
        Self {
            operation_id: record.operation_id,
            offline_revision: record.offline_revision.to_string(),
            state: record.state,
            error: record.error,
        }
    }
}

pub(crate) async fn request(
    State(state): State<AppState>,
    Path((node, group, disk)): Path<(u64, u64, String)>,
    Json(body): Json<Revision>,
) -> Result<(StatusCode, Json<OnlineOperation>), Failure> {
    let expected = body.parse()?;
    let (hardware, key) = super::authority::disk(&state, node, group, &disk).await?;
    if let Some(operation) = hardware
        .disk_online_operation(&key)
        .await
        .map_err(|e| err_502(e.to_string()))?
    {
        if operation.offline_revision == expected && operation.state != DiskOnlineState::Failed {
            let reconciled = hardware
                .request_disk_online(&key, expected)
                .await
                .map_err(|error| err_409(error.to_string()))?;
            return Ok((StatusCode::ACCEPTED, Json(reconciled.into())));
        }
    }
    let backing = hardware
        .disk_storage(&key)
        .await
        .map_err(|e| err_502(e.to_string()))?
        .filter(|record| !record.removed)
        .ok_or_else(|| err_409("Online requires registered retained backing"))?;
    super::retained_medium::verify(
        &state,
        &key,
        &backing,
        RetainedDiskCondition::Offline {
            revision: expected.to_string(),
        },
    )
    .await?;
    let operation = hardware
        .request_disk_online(&key, expected)
        .await
        .map_err(|error| {
            err_502(format!(
        "Online request outcome is unconfirmed: {error}; inspect the same disk revision before retrying"
    ))
        })?;
    Ok((StatusCode::ACCEPTED, Json(operation.into())))
}
