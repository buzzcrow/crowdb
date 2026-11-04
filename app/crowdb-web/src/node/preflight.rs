// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authenticated inventory preparation never creates admission membership.

use super::admission::AdmitRequest;
use crate::{
    error::{err_400, err_409, err_502, map_config_err},
    services::Failure,
    state::AppState,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::deployment::{admission, registry::NodeRecord};
use crowdb_protocol::mgmt::node::{storage::DiskInventory, NodeControl};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, os::unix::fs::PermissionsExt, path::PathBuf};

#[derive(Serialize)]
pub(crate) struct PreparedAccess {
    operation_id: String,
    inventory: DiskInventory,
    accepted_choices: Option<crowdb_protocol::mgmt::node::setup::NodeSetupChoices>,
    policy_choices: Option<crowdb_protocol::mgmt::node::setup::NodeSetupChoices>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PreparedCandidate {
    discovery_id: String,
    operation_id: String,
    rack_id: u64,
    ssh_user: String,
    ssh_port: u16,
}

pub(super) fn summaries(
    state: &AppState,
    candidates: &[crowdb_protocol::mgmt::node::CandidateNode],
) -> Result<Vec<PreparedCandidate>, Failure> {
    let mut prepared = Vec::new();
    for candidate in candidates {
        let id = uuid::Uuid::parse_str(&candidate.advertisement.discovery_id)
            .map_err(|_| err_409("Invalid candidate identity"))?;
        if let Some(record) = read(state, id)? {
            prepared.push(PreparedCandidate {
                discovery_id: record.discovery_id,
                operation_id: record.operation_id,
                rack_id: record.rack_id,
                ssh_user: record.ssh_user,
                ssh_port: record.ssh_port,
            });
        }
    }
    Ok(prepared)
}

fn path(state: &AppState, id: uuid::Uuid) -> PathBuf {
    state.runtime_root.join(format!("node-preflight-{id}.json"))
}

fn read(state: &AppState, id: uuid::Uuid) -> Result<Option<NodeRecord>, Failure> {
    let path = path(state, id);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata)
            if metadata.is_file()
                && metadata.len() <= 8192
                && metadata.permissions().mode() & 0o777 == 0o600 =>
        {
            let record: NodeRecord =
                serde_json::from_slice(&std::fs::read(path).map_err(|error| err_502(error.to_string()))?)
                    .map_err(|error| err_502(error.to_string()))?;
            if record.discovery_id != id.to_string() || record.confirmed || record.cancelled {
                return Err(err_409("Invalid candidate access preparation"));
            }
            Ok(Some(record))
        }
        Ok(_) => Err(err_409("Invalid candidate access preparation file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(err_502(error.to_string())),
    }
}

pub(crate) async fn prepare(
    State(state): State<AppState>,
    Json(body): Json<AdmitRequest>,
) -> Result<Json<PreparedAccess>, Failure> {
    let id = uuid::Uuid::parse_str(&body.discovery_id).map_err(|_| err_400("Invalid discovery UUID"))?;
    let _claim =
        crate::services::Operation::claim(&state, vec!["node/admission".into(), "cluster/init".into()])?;
    let (host, handshake) = super::admission::observe(&state, &body).await?;
    let config = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .clone();
    if !config.racks.iter().any(|rack| rack.id == body.rack_id) {
        return Err(err_400("Select an existing rack"));
    }
    let records = super::records(&state)?;
    let record = if let Some(record) = read(&state, id)? {
        check(&record, &body, &host, &handshake.physical_host_id)?;
        record
    } else if let Some(record) = records
        .iter()
        .find(|record| record.discovery_id == body.discovery_id && !record.cancelled)
    {
        if record.confirmed {
            return Err(err_409("Use authenticated Update for an admitted node"));
        }
        record.clone()
    } else {
        NodeRecord {
            web_port: 9090,
            discovery_id: id.to_string(),
            node_id: super::admission::next_node_id(&config, &records)?,
            physical_host_id: handshake.physical_host_id,
            rack_id: body.rack_id,
            host,
            ssh_user: body.ssh_user.clone(),
            ssh_port: body.ssh_port,
            operation_id: uuid::Uuid::new_v4().to_string(),
            host_key: None,
            confirmed: false,
            cancelled: false,
        }
    };
    // A separate prefix prevents automatic bootstrap from selecting a form that
    // authenticated successfully but has not submitted its storage choices.
    super::save(&path(&state, id), &record)?;
    let key = super::node_key_path(&state);
    let mut target = super::admission::as_node(&record);
    target.ssh_password = body.ssh_password;
    if target.ssh_password.is_none() {
        target.ssh_key = Some(key.to_string_lossy().into_owned());
    }
    let mut observed = admission::remote_controls(
        &target,
        &[
            NodeControl::Identity,
            NodeControl::DiskInventory,
            NodeControl::DeploymentPolicy,
            NodeControl::NodeSetupIntent,
        ],
    )
    .await
    .map_err(map_config_err)?;
    if observed[0]["discovery_id"] != record.discovery_id {
        return Err(err_409("Authenticated node identity differs from discovery"));
    }
    let accepted: Option<crowdb_protocol::mgmt::node::setup::NodeSetupRequest> = serde_json::from_value(
        observed
            .pop()
            .ok_or_else(|| err_502("Missing accepted setup observation"))?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    if accepted.as_ref().is_some_and(|request| {
        request.operation_id != record.operation_id || request.discovery_id != record.discovery_id
    }) {
        return Err(err_409(
            "Candidate has another accepted setup; resume its original admission",
        ));
    }
    let policy: Option<crowdb_protocol::mgmt::node::deployment::NodeDeploymentPolicy> =
        serde_json::from_value(
            observed
                .pop()
                .ok_or_else(|| err_502("Missing target startup policy"))?,
        )
        .map_err(|error| err_502(error.to_string()))?;
    if let Some(policy) = &policy {
        policy.validate().map_err(err_400)?;
    }
    let inventory = serde_json::from_value(
        observed
            .pop()
            .ok_or_else(|| err_502("Missing authenticated inventory"))?,
    )
    .map_err(|error| err_502(error.to_string()))?;
    Ok(Json(PreparedAccess {
        operation_id: record.operation_id,
        inventory,
        accepted_choices: accepted.map(|request| request.choices),
        policy_choices: policy.map(|policy| policy.setup),
    }))
}

fn check(record: &NodeRecord, body: &AdmitRequest, host: &str, physical_host: &str) -> Result<(), Failure> {
    if record.host != host
        || record.physical_host_id != physical_host
        || record.rack_id != body.rack_id
        || record.ssh_user != body.ssh_user
        || record.ssh_port != body.ssh_port
        || body
            .preflight_operation_id
            .as_ref()
            .is_some_and(|id| *id != record.operation_id)
    {
        return Err(err_409(
            "Candidate access choices changed; resume the original preparation",
        ));
    }
    Ok(())
}

pub(super) fn accepted(
    state: &AppState,
    body: &AdmitRequest,
    host: &str,
    physical_host: &str,
) -> Result<Option<NodeRecord>, Failure> {
    let Some(operation) = &body.preflight_operation_id else {
        return Ok(None);
    };
    let id = uuid::Uuid::parse_str(&body.discovery_id).map_err(|_| err_400("Invalid discovery UUID"))?;
    let record = read(state, id)?
        .filter(|record| record.operation_id == *operation)
        .ok_or_else(|| err_409("Candidate access preparation is unavailable or differs"))?;
    check(&record, body, host, physical_host)?;
    Ok(Some(record))
}

pub(super) fn pending(state: &AppState) -> Result<BTreeSet<String>, Failure> {
    let mut result = BTreeSet::new();
    for entry in std::fs::read_dir(state.runtime_root.as_ref()).map_err(|error| err_502(error.to_string()))? {
        let entry = entry.map_err(|error| err_502(error.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name
            .strip_prefix("node-preflight-")
            .and_then(|name| name.strip_suffix(".json"))
        {
            let id = uuid::Uuid::parse_str(id).map_err(|_| err_409("Invalid prepared access identity"))?;
            if let Some(record) = read(state, id)? {
                result.insert(record.discovery_id);
            }
            if result.len() > 4096 {
                return Err(err_409("Prepared access inventory exceeds its bound"));
            }
        }
    }
    Ok(result)
}

pub(super) fn finish(state: &AppState, record: &NodeRecord) -> Result<(), Failure> {
    let id = uuid::Uuid::parse_str(&record.discovery_id).map_err(|_| err_400("Invalid discovery UUID"))?;
    if read(state, id)?.is_some() {
        std::fs::remove_file(path(state, id)).map_err(|error| err_502(error.to_string()))?;
        std::fs::File::open(state.runtime_root.as_ref())
            .and_then(|root| root.sync_all())
            .map_err(|error| err_502(error.to_string()))?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiscardReview {
    operation_id: String,
}

pub(crate) async fn discard(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<DiscardReview>,
) -> Result<StatusCode, Failure> {
    let _claim =
        crate::services::Operation::claim(&state, vec!["node/admission".into(), "cluster/init".into()])?;
    if let Some(record) = read(&state, id)? {
        if record.operation_id != body.operation_id {
            return Err(err_409("Candidate review operation changed"));
        }
        finish(&state, &record)?;
    }
    Ok(StatusCode::NO_CONTENT)
}
