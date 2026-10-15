// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact-revision Offline and removal; no byte deletion or inferred evacuation.

use super::Failure;
use crate::{
    error::{err_400, err_409, err_502},
    state::AppState,
};
use axum::{
    extract::{Path, State},
    Json,
};
use crowdb_kv_client::{Error, HardwareClient};
use crowdb_protocol::{common::HwStatus, disk_storage::DiskRemovalRecord, key::DiskKey, DiskIdExt};
use serde::{Deserialize, Serialize};

type DiskPath = Path<(u64, u64, String)>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Revision {
    expected_revision: String,
}

impl Revision {
    pub(super) fn parse(&self) -> Result<u64, Failure> {
        let revision = self
            .expected_revision
            .parse::<u64>()
            .map_err(|_| err_400("Disk revision must be a decimal string"))?;
        if revision == 0 || revision.to_string() != self.expected_revision {
            return Err(err_400(
                "Disk revision must be a positive canonical decimal string",
            ));
        }
        Ok(revision)
    }
}

#[derive(Serialize)]
pub(crate) struct DiskLifecycle {
    version: u32,
    disk_id: String,
    removed: bool,
    offline: bool,
    disk_revision: Option<String>,
    remove_conditions: Vec<String>,
    operation_id: Option<String>,
    removed_from_revision: Option<String>,
    rejoin_operation_id: Option<String>,
    rejoin_source_removal_operation_id: Option<String>,
    online: Option<super::online::OnlineOperation>,
}

#[derive(Serialize)]
pub(crate) struct RemovalOutcome {
    operation_id: String,
    disk_id: String,
    removed: bool,
    offline_revision: String,
}
impl From<DiskRemovalRecord> for RemovalOutcome {
    fn from(record: DiskRemovalRecord) -> Self {
        Self {
            operation_id: record.operation_id,
            disk_id: record.disk.disk_id.to_display_string(),
            removed: true,
            offline_revision: record.offline_revision.to_string(),
        }
    }
}

fn failure(error: Error) -> Failure {
    match error { Error::Server(reason) => err_409(reason), other => err_502(format!("Storage outcome is unconfirmed: {other}; refresh authority before retrying the same disk revision")) }
}

pub(crate) async fn status(
    State(state): State<AppState>,
    Path((node, group, disk)): DiskPath,
) -> Result<Json<DiskLifecycle>, Failure> {
    let (hardware, key) = super::authority::disk(&state, node, group, &disk).await?;
    observe(&hardware, &key).await.map(Json)
}

pub(super) async fn observe(hardware: &HardwareClient, key: &DiskKey) -> Result<DiskLifecycle, Failure> {
    let backing = hardware
        .disk_storage(key)
        .await
        .map_err(failure)?
        .ok_or_else(|| {
            err_409("Legacy disk backing is unknown; retained identity is required for this lifecycle")
        })?;
    if backing.removed {
        let receipt = hardware
            .disk_removal_receipt(key)
            .await
            .map_err(failure)?
            .ok_or_else(|| err_409("Removed disk has no committed operation receipt"))?;
        if receipt.disk != backing {
            return Err(err_409("Removed backing differs from the operation receipt"));
        }
        return Ok(DiskLifecycle {
            version: 1,
            disk_id: key.disk_id.to_display_string(),
            removed: true,
            offline: true,
            disk_revision: None,
            remove_conditions: Vec::new(),
            operation_id: Some(receipt.operation_id),
            removed_from_revision: Some(receipt.offline_revision.to_string()),
            rejoin_operation_id: None,
            rejoin_source_removal_operation_id: None,
            online: None,
        });
    }
    let readiness = hardware.disk_removal_readiness(key).await.map_err(failure)?;
    let current = hardware
        .get_disk_with_revision(key)
        .await
        .map_err(failure)?
        .ok_or_else(|| err_409("Disk membership changed during observation"))?;
    if current.1 != readiness.disk_revision {
        return Err(err_409("Disk revision changed during observation; refresh"));
    }
    let rejoin = hardware
        .disk_rejoin_receipt(key)
        .await
        .map_err(failure)?
        .filter(|receipt| receipt.restored() == backing);
    let online = hardware
        .disk_online_operation(key)
        .await
        .map_err(failure)?
        .filter(|operation| {
            operation.disk == backing
                && (operation.offline_revision == current.1
                    || (current.0.status == HwStatus::Up as i32
                        && operation.state == crowdb_protocol::disk_storage::DiskOnlineState::Complete))
        });
    Ok(DiskLifecycle {
        version: 1,
        disk_id: key.disk_id.to_display_string(),
        removed: false,
        offline: current.0.status == HwStatus::Offline as i32,
        disk_revision: Some(current.1.to_string()),
        remove_conditions: readiness.conditions,
        operation_id: (current.0.status == HwStatus::Offline as i32)
            .then(|| format!("offline/{}/{}", key.disk_id.to_display_string(), current.1)),
        removed_from_revision: None,
        rejoin_operation_id: rejoin.as_ref().map(|receipt| receipt.operation_id.clone()),
        rejoin_source_removal_operation_id: rejoin.map(|receipt| receipt.source.operation_id),
        online: online.map(Into::into),
    })
}

pub(crate) async fn offline(
    State(state): State<AppState>,
    Path((node, group, disk)): DiskPath,
    Json(body): Json<Revision>,
) -> Result<Json<DiskLifecycle>, Failure> {
    let expected = body.parse()?;
    let (hardware, key) = super::authority::disk(&state, node, group, &disk).await?;
    let committed = hardware
        .offline_disk_storage(&key, expected)
        .await
        .map_err(failure)?;
    let conditions = match hardware.disk_removal_readiness(&key).await {
        Ok(readiness) if readiness.disk_revision == committed => readiness.conditions,
        Ok(_) => vec!["Offline committed; membership changed afterwards. Refresh authority.".into()],
        Err(error) => vec![format!(
            "Offline committed; exclusion observations unavailable: {error}"
        )],
    };
    Ok(Json(DiskLifecycle {
        version: 1,
        disk_id: key.disk_id.to_display_string(),
        removed: false,
        offline: true,
        disk_revision: Some(committed.to_string()),
        remove_conditions: conditions,
        operation_id: Some(format!(
            "offline/{}/{}",
            key.disk_id.to_display_string(),
            committed
        )),
        removed_from_revision: None,
        rejoin_operation_id: None,
        rejoin_source_removal_operation_id: None,
        online: None,
    }))
}

pub(crate) async fn remove(
    State(state): State<AppState>,
    Path((node, group, disk)): DiskPath,
    Json(body): Json<Revision>,
) -> Result<Json<RemovalOutcome>, Failure> {
    let expected = body.parse()?;
    let (hardware, key) = super::authority::disk(&state, node, group, &disk).await?;
    let result = hardware
        .remove_disk_storage(&key, expected)
        .await
        .map_err(failure)?;
    Ok(Json(result.into()))
}
