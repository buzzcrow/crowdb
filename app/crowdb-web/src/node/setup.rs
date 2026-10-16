// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Admission submits an immutable target intent; progress stays target-owned.

use crate::{
    error::{err_409, err_502, map_config_err},
    services::Failure,
    state::AppState,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::{
    config::NodeEntry,
    deployment::{admission::remote_control, registry::NodeRecord},
};
use crowdb_protocol::mgmt::node::{
    setup::{NodeSetupChoices, NodeSetupRequest, NodeSetupStatus},
    NodeControl,
};

pub(super) async fn accept(
    state: &AppState,
    record: &NodeRecord,
    target: &NodeEntry,
    choices: Option<NodeSetupChoices>,
) -> Result<(), Failure> {
    let mut target = target.clone();
    target.ssh_password = None;
    target.ssh_key = Some(super::node_key_path(state).to_string_lossy().into_owned());
    let existing: Option<NodeSetupRequest> = serde_json::from_value(
        remote_control(&target, &NodeControl::NodeSetupIntent)
            .await
            .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    let choices = choices
        .or_else(|| {
            existing
                .filter(|request| request.operation_id == record.operation_id)
                .map(|request| request.choices)
        })
        .unwrap_or_default();
    let request = NodeSetupRequest {
        version: 1,
        discovery_id: record.discovery_id.clone(),
        operation_id: record.operation_id.clone(),
        choices,
    };
    request.validate().map_err(crate::error::err_400)?;
    let value = remote_control(&target, &NodeControl::AcceptNodeSetup { request })
        .await
        .map_err(map_config_err)?;
    let observed: NodeSetupStatus =
        serde_json::from_value(value).map_err(|error| err_502(error.to_string()))?;
    validate(state, record, &observed)?;
    Ok(())
}

pub(crate) async fn status(
    State(state): State<AppState>,
    Path(node): Path<u64>,
) -> Result<Json<Option<NodeSetupStatus>>, Failure> {
    let target = super::storage::remote_node(&state, node)?;
    let observed: Option<NodeSetupStatus> = serde_json::from_value(
        remote_control(&target, &NodeControl::NodeSetup)
            .await
            .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    if let Some(observed) = &observed {
        validate(&state, &record(&state, node).await?, observed)?;
    }
    Ok(Json(observed))
}

pub(crate) async fn retry(
    State(state): State<AppState>,
    Path(node): Path<u64>,
) -> Result<(StatusCode, Json<NodeSetupStatus>), Failure> {
    let target = super::storage::remote_node(&state, node)?;
    let record = record(&state, node).await?;
    let request: Option<NodeSetupRequest> = serde_json::from_value(
        remote_control(&target, &NodeControl::NodeSetupIntent)
            .await
            .map_err(map_config_err)?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    let request = request.ok_or_else(|| err_409("No accepted node setup exists"))?;
    if request.discovery_id != record.discovery_id || request.operation_id != record.operation_id {
        return Err(err_409("Node setup intent differs from retained admission"));
    }
    let result = remote_control(&target, &NodeControl::AcceptNodeSetup { request })
        .await
        .map_err(map_config_err)?;
    let status = serde_json::from_value(result).map_err(|error| err_502(error.to_string()))?;
    validate(&state, &record, &status)?;
    Ok((StatusCode::ACCEPTED, Json(status)))
}

async fn record(state: &AppState, node: u64) -> Result<NodeRecord, Failure> {
    let records = super::admissions(State(state.clone())).await?.0;
    let mut matching = records
        .into_iter()
        .filter(|record| record.node_id == node && !record.cancelled);
    let record = matching
        .next()
        .ok_or_else(|| err_409("Node setup has no retained admission"))?;
    if matching.next().is_some() {
        return Err(err_409("Node setup has ambiguous admission identity"));
    }
    Ok(record)
}

fn validate(state: &AppState, record: &NodeRecord, observed: &NodeSetupStatus) -> Result<(), Failure> {
    let binding = super::cluster_binding(state)?;
    observed
        .validate_identity(
            &record.discovery_id,
            &record.operation_id,
            record.node_id,
            binding
                .as_ref()
                .map(|binding| binding.bootstrap.cluster_id.as_str()),
        )
        .map_err(err_409)
}
