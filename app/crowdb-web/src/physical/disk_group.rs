// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded `DiskGroup` provisioning with reconciliation after unknown outcomes.
use crate::{
    error::{err_500, err_502, map_config_err, map_persist_err, ErrorBody},
    state::AppState,
};
use axum::{http::StatusCode, Json};
use crowdb_console_shared::{
    cluster::{DiskGroupId, NodeId},
    config::DiskGroupEntry,
    ops,
};
type Failure = (StatusCode, Json<ErrorBody>);

pub(crate) async fn create(
    state: AppState,
    node_id: NodeId,
    id: DiskGroupId,
    name: String,
    binding: crowdb_protocol::common::BindMapValue,
) -> Result<(StatusCode, Json<DiskGroupEntry>), Failure> {
    let operation = crate::services::Operation::claim(&state, vec![format!("disk-group/{node_id}/{id}")])?;
    let work = tokio::spawn(async move {
        let _operation = operation;
        provision(state, node_id, id, name, binding).await
    });
    // Dropping JoinHandle detaches authorized work; it cannot cancel between
    // authoritative publication and its local launch/configuration update.
    tokio::time::timeout(std::time::Duration::from_secs(8), work).await
        .map_err(|_| err_502("DiskGroup creation outcome unknown after 8 seconds. Refresh the Node and reconcile this ID before retrying."))?
        .map_err(|error| err_500(format!("DiskGroup provisioning task failed: {error}")))?
}

async fn provision(
    state: AppState,
    node_id: NodeId,
    id: DiskGroupId,
    name: String,
    binding: crowdb_protocol::common::BindMapValue,
) -> Result<(StatusCode, Json<DiskGroupEntry>), Failure> {
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let entry = ops::hardware::add_disk_group_bound_to_group0(&ctx, node_id, id, &name, Some(binding))
        .await
        .map_err(map_config_err)?;
    {
        // Group-0 publication can overlap service registration. Merge only
        // this entry so the older operation snapshot cannot erase services.
        let mut config = state.config.write().unwrap();
        if !config
            .disk_groups
            .iter()
            .any(|g| g.node_id == node_id && g.id == id)
        {
            config.add_disk_group(entry.clone()).map_err(map_config_err)?;
        }
    }
    state.persist().map_err(map_persist_err)?;

    Ok((StatusCode::CREATED, Json(entry)))
}
