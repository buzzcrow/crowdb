// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Application readiness remains observable while initialization is unavailable.

use crate::{
    error::{err_409, ErrorBody},
    state::AppState,
};
use axum::{extract::State, http::StatusCode, Json};
use crowdb_console_shared::{
    ops::{cluster::readiness, OpContext},
    ConsoleConfig,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::mgmt::{
    cluster::{ClusterStatus, DependencyKind, DependencyObservation, DependencyState, ReadinessReason},
    node::observation::NodeRuntimeObservation,
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

type Failure = (StatusCode, Json<ErrorBody>);

pub(crate) async fn read(State(state): State<AppState>) -> Result<Json<ClusterStatus>, Failure> {
    if state.node_monitor_url.is_none() {
        return Err(err_409("Cluster observation requires an owning node monitor"));
    }
    let (local, failure) = match super::observation::read(State(state.clone())).await {
        Ok(Json(local)) => (local, None),
        Err(error) if error.0 == StatusCode::BAD_GATEWAY => (retained(&state)?, Some(error.1 .0.error)),
        Err(error) => return Err(error),
    };
    let seeds = local
        .binding
        .as_ref()
        .map_or_else(Vec::new, |binding| binding.management_seeds.clone());
    let client = Arc::new(CrowdbKvClient::new(ClientConfig::new(seeds.clone())));
    let context = OpContext::with_shared_client_preserving_hint(client, &seeds, ConsoleConfig::default());
    let id = local.binding.as_ref().map_or_else(
        || "local-node/observation".into(),
        |binding| format!("node/{}/observation", binding.node_id),
    );
    let mut report = readiness::collect(&context, local)
        .await
        .map_err(|_| err_409("Cluster observation identity or policy is invalid"))?;
    if let Some(failure) = failure {
        let detail = format!("Local monitor/runtime observation failed: {failure}; retained identity is available, but current execution and protocol health could not be observed");
        report.observation.dependencies.push(DependencyObservation {
            id: id.clone(),
            kind: DependencyKind::Member,
            required: true,
            state: DependencyState::Unknown,
            detail: detail.clone(),
            observed_at_unix_ms: report.observation.observed_at_unix_ms,
            max_age_ms: 3000,
            revision: None,
            operation_id: None,
            current_operation_id: None,
            reason: Some(ReadinessReason {
                dependency: id,
                code: "node-observation-unavailable".into(),
                detail,
            }),
        });
        let observed_at = report.observation.observed_at_unix_ms;
        report = readiness::reduce(report.observation, observed_at);
    }
    Ok(Json(report))
}

fn retained(state: &AppState) -> Result<NodeRuntimeObservation, Failure> {
    let root = state
        .runtime_root
        .parent()
        .ok_or_else(|| err_409("Node state root is absent"))?;
    let identity = crowdb_monitor::NodeIdentity::load_existing(root)
        .map_err(|_| err_409("Retained node identity is unavailable"))?;
    let policy = crowdb_monitor::read_node_policy(root)
        .map_err(|_| err_409("Retained node policy is invalid"))?
        .ok_or_else(|| err_409("Retained node policy is absent"))?;
    Ok(NodeRuntimeObservation {
        version: 1,
        discovery_id: identity.uuid().to_string(),
        policy,
        binding: super::cluster_binding(state)?,
        setup: None,
        observed_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |now| u64::try_from(now.as_millis()).unwrap_or(u64::MAX)),
        services: Vec::new(),
        protocols: Vec::new(),
    })
}
