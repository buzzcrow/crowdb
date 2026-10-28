// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use axum::{extract::State, Json};
use crowdb_console_shared::deployment::PreparedBootstrap;
use crowdb_protocol::mgmt::node::deployment::DeploymentMode;
use serde_json::{json, Value};

use super::Failure;
use crate::{
    error::{err_409, err_502, map_config_err},
    node,
    state::AppState,
};

pub(super) async fn run(state: AppState) {
    let mut attempts = BTreeMap::new();
    loop {
        let observation = match step(&state, &mut attempts).await {
            Ok(value) => value,
            Err((_, error)) => json!({"phase": "failed", "error": error.error}),
        };
        if let Err((_, error)) = node::save(&state.runtime_root.join("auto-join-status.json"), &observation) {
            tracing::error!(error = %error.error, "cannot persist Auto join observation");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn pin_target(state: &AppState, cluster_id: &str) -> Result<(), Failure> {
    let _operation = crate::services::Operation::claim(state, vec!["node/auto-join".into()])?;
    let mut policy = super::read(state)?;
    if let Some(target) = &policy.target_cluster_id {
        if target != cluster_id {
            return Err(err_409("Auto join target differs from this cluster"));
        }
    } else {
        policy.target_cluster_id = Some(cluster_id.into());
        node::save(&state.runtime_root.join("auto-join.json"), &policy)?;
    }
    Ok(())
}

async fn step(
    state: &AppState,
    attempts: &mut BTreeMap<String, (Instant, String)>,
) -> Result<Value, Failure> {
    let policy = super::read(state)?;
    if !state.runtime_root.join("auto-join.json").exists() {
        let _operation = crate::services::Operation::claim(state, vec!["node/auto-join".into()])?;
        if !state.runtime_root.join("auto-join.json").exists() {
            node::save(&state.runtime_root.join("auto-join.json"), &policy)?;
        }
    }
    if !policy.enabled {
        return Ok(json!({"phase": "disabled"}));
    }
    let authority = node::status(State(state.clone())).await.0;
    let binding = node::cluster_binding(state)?;
    if let Some(binding) = &binding {
        pin_target(state, &binding.bootstrap.cluster_id)?;
        if authority["available"] != true {
            return Ok(json!({"phase": "authority-unavailable", "cluster_id": binding.bootstrap.cluster_id}));
        }
    } else if state.runtime_root.join("prepared-bootstrap.json").exists() {
        let operation = PreparedBootstrap::load(&state.runtime_root.join("prepared-bootstrap.json"))
            .map_err(map_config_err)?;
        pin_target(state, &operation.identity.cluster_id)?;
        let progress = node::initialization::status(State(state.clone())).await?.0;
        return Ok(
            json!({"phase": if progress["phase"] == "failed" { "failed" } else { "initializing" },
            "members": operation.intent.members(), "error": progress["error"],
            "operation_id": operation.identity.operation_id}),
        );
    } else if policy.target_cluster_id.is_some() {
        return Err(err_409(
            "Auto join target is not bound; recover the accepted cluster before joining nodes",
        ));
    }
    if binding.is_none() && authority["phase"] != "unbound_draft" {
        return Ok(json!({"phase": "recovery-required", "authority": authority}));
    }
    admit_candidates(state, attempts, binding.is_some()).await
}

async fn admit_candidates(
    state: &AppState,
    attempts: &mut BTreeMap<String, (Instant, String)>,
    bound: bool,
) -> Result<Value, Failure> {
    if bound {
        return finish(state, true, Vec::new(), Vec::new()).await;
    }
    let deployment = super::deployment(state)?;
    let snapshot = node::candidates(State(state.clone()))
        .await
        .map_err(|status| {
            (
                status,
                Json(crate::error::ErrorBody {
                    error: "Discovery unavailable".into(),
                }),
            )
        })?
        .0;
    let selected = super::formation::candidates(&snapshot, &deployment)?;
    if let Some(waiting) = super::formation::waiting_for_peers(state, &snapshot, &selected, &deployment)? {
        return Ok(waiting);
    }
    super::formation::prepare_local_rack(state, &deployment).await?;
    let config = state
        .config
        .read()
        .map_err(|error| err_502(error.to_string()))?
        .clone();
    let records = node::records(state)?;
    let prepared_access = node::preflight::pending(state)?;
    let Some(rack) = config.racks.first() else {
        return Ok(json!({"phase": "needs-rack"}));
    };
    let candidates: Vec<_> = selected
        .into_iter()
        .filter(|candidate| {
            !prepared_access.contains(&candidate.advertisement.discovery_id)
                || records.iter().any(|record| {
                    record.discovery_id == candidate.advertisement.discovery_id
                        && !record.cancelled
                        && config.node(record.node_id).is_some()
                })
        })
        .collect();
    attempts.retain(|id, _| {
        candidates
            .iter()
            .any(|candidate| candidate.advertisement.discovery_id == *id)
    });
    let mut eligible = Vec::new();
    let mut needs_access = Vec::new();
    for candidate in candidates {
        let id = &candidate.advertisement.discovery_id;
        let record = records.iter().find(|record| record.discovery_id == *id);
        if record.is_some_and(|record| record.cancelled) {
            continue;
        }
        if let Some(record) = record.filter(|record| config.node(record.node_id).is_some() && !bound) {
            eligible.push(record.node_id);
            if eligible.len() == usize::from(deployment.initial_voters) {
                break;
            }
            continue;
        }
        if let Some(observation) = admit_candidate(state, attempts, candidate, record, rack.id, bound).await?
        {
            return Ok(observation);
        }
        needs_access.push(json!({"discovery_id": id, "error": attempts.get(id).map(|(_, error)| error)}));
    }
    finish(state, bound, eligible, needs_access).await
}

async fn finish(
    state: &AppState,
    bound: bool,
    eligible: Vec<u64>,
    needs_access: Vec<Value>,
) -> Result<Value, Failure> {
    if bound {
        let path = state.runtime_root.join("confirmed-cluster.json");
        let operation = PreparedBootstrap::load(&path).map_err(map_config_err)?;
        return Ok(json!({"phase": "ready", "needs_access": needs_access,
            "members": operation.intent.members(), "operation_id": operation.identity.operation_id}));
    }
    let required = super::deployment(state)?.initial_voters;
    if eligible.len() < usize::from(required) {
        return Ok(
            json!({"phase": "waiting", "members": eligible, "required_voters": required, "needs_access": needs_access}),
        );
    }
    if !super::read(state)?.enabled {
        return Ok(json!({"phase": "disabled"}));
    }
    let _operation =
        crate::services::Operation::claim(state, vec!["cluster/init".into(), "node/admission".into()])?;
    let progress = node::initialization::trigger(state, &eligible, Some(false)).await?;
    Ok(json!({"phase": "initializing", "members": eligible, "operation": progress}))
}

async fn admit_candidate(
    state: &AppState,
    attempts: &mut BTreeMap<String, (Instant, String)>,
    candidate: &crowdb_protocol::mgmt::node::CandidateNode,
    record: Option<&crowdb_console_shared::deployment::registry::NodeRecord>,
    default_rack: u64,
    bound: bool,
) -> Result<Option<Value>, Failure> {
    let id = &candidate.advertisement.discovery_id;
    if attempts
        .get(id)
        .is_some_and(|(last, _)| last.elapsed() < Duration::from_secs(60))
    {
        return Ok(None);
    }
    let deployment = super::deployment(state)?;
    // Only compatible local formation can use the documented test credential.
    let password = if deployment.mode == DeploymentMode::Local {
        Some(super::formation::local_password(state)?)
    } else {
        None
    };
    let body = node::admission::AdmitRequest {
        discovery_id: id.clone(),
        rack_id: record.map_or(default_rack, |record| record.rack_id),
        ssh_user: record.map_or_else(|| "crowdb".into(), |record| record.ssh_user.clone()),
        ssh_port: record.map_or(crowdb_protocol::SSH_PORT, |record| record.ssh_port),
        ssh_password: password,
        preflight_operation_id: None,
        setup: None,
        expected_formation: Some(deployment.formation()),
    };
    if !super::read(state)?.enabled {
        return Ok(Some(json!({"phase": "disabled"})));
    }
    match node::admit(State(state.clone()), Json(body)).await {
        Ok(record) => Ok(Some(json!({
            "phase": if bound { "joining-nonvoter" } else { "preparing" },
            "node_id": record.node_id,
        }))),
        Err((_, error)) => {
            attempts.insert(id.clone(), (Instant::now(), error.error.clone()));
            Ok(None)
        }
    }
}
