// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed plan operations yield to every later explicit service command.

use super::{journal, AcceptedSetup, Result};
use crate::DeploymentProfile;
use crowdb_console_shared::deployment::services;
use crowdb_kv_client::CrowdbKvClient;
use crowdb_protocol::mgmt::node::{
    setup::{SetupServiceKind, SetupStepPhase},
    NodeBinding, NodeControl, NodeServiceAction,
};
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

pub(super) async fn advance(
    profile: &DeploymentProfile,
    state: &mut AcceptedSetup,
    binding: &NodeBinding,
    kv: &CrowdbKvClient,
) -> Result<bool> {
    if state.status.services.iter().all(|step| {
        matches!(
            step.phase,
            SetupStepPhase::Disabled | SetupStepPhase::Ready | SetupStepPhase::Warning
        )
    }) {
        return Ok(true);
    }
    let intents = services::list(kv).await?;
    let processes = super::super::super::control_node(
        &profile.paths.run_root.join("node-control.sock"),
        &NodeControl::Processes {
            cluster_id: binding.bootstrap.cluster_id.clone(),
            node_id: binding.node_id,
        },
    )
    .await?;
    for kind in SetupServiceKind::ALL {
        let index = state
            .status
            .services
            .iter()
            .position(|step| step.kind == kind)
            .ok_or("setup service step missing")?;
        let step = &mut state.status.services[index];
        if matches!(
            step.phase,
            SetupStepPhase::Disabled | SetupStepPhase::Ready | SetupStepPhase::Warning
        ) {
            continue;
        }
        if kind == SetupServiceKind::PaxosKv {
            step.phase = if processes["processes"]["kv"].is_number() {
                SetupStepPhase::Ready
            } else {
                SetupStepPhase::Waiting
            };
            step.detail = Some("Use the existing admitted KV process".into());
            if step.phase != SetupStepPhase::Ready {
                return Ok(false);
            }
            continue;
        }
        let name = name(kind);
        if let Some(intent) = intents.iter().find(|intent| {
            intent.node_id == binding.node_id
                && intent.cluster_id == binding.bootstrap.cluster_id
                && intent.kind == name
        }) {
            if !observe(profile, step, intent, &processes).await? {
                return Ok(false);
            }
            continue;
        }
        if step.operation_id.is_none() {
            step.operation_id = Some(uuid::Uuid::new_v4().to_string());
        }
        step.phase = SetupStepPhase::Preparing;
        journal::save(&profile.paths.data_root, state)?;
        let (success, value) = prepare(binding, state, kind).await?;
        let step = &mut state.status.services[index];
        step.phase = if success {
            SetupStepPhase::Accepted
        } else {
            SetupStepPhase::Waiting
        };
        step.detail = Some(journal::bounded(
            value["error"].as_str().unwrap_or(if success {
                "Service intent accepted"
            } else {
                "Service prerequisites unavailable"
            }),
            128,
        ));
        return Ok(false);
    }
    Ok(true)
}

fn name(kind: SetupServiceKind) -> &'static str {
    match kind {
        SetupServiceKind::PaxosKv => "kv",
        SetupServiceKind::Diskdb => "diskdb",
        SetupServiceKind::Diskio => "diskio",
        SetupServiceKind::Chunkdb => "chunkdb",
        SetupServiceKind::ChunkKv => "chunk-kv",
        SetupServiceKind::AccessServer => "access",
    }
}

async fn prepare(
    binding: &NodeBinding,
    state: &AcceptedSetup,
    kind: SetupServiceKind,
) -> Result<(bool, serde_json::Value)> {
    let mut response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?
        .post(format!(
            "http://127.0.0.1:9090/api/nodes/{}/setup/prepare-service",
            binding.node_id
        ))
        .json(&json!({"operation_id":state.request.operation_id,"kind":kind}))
        .send()
        .await?;
    let success = response.status().is_success();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > 65536 {
            return Err("prepared service reply exceeds its bound".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    Ok((success, value))
}

async fn observe(
    profile: &DeploymentProfile,
    step: &mut crowdb_protocol::mgmt::node::setup::SetupServiceStatus,
    intent: &crowdb_protocol::mgmt::node::NodeServiceIntent,
    processes: &serde_json::Value,
) -> Result<bool> {
    if intent.action != NodeServiceAction::Start
        || step
            .operation_id
            .as_ref()
            .is_some_and(|id| *id != intent.operation_id)
    {
        step.phase = SetupStepPhase::Warning;
        step.detail = Some("Existing operator command takes precedence; inspect service controls".into());
        return Ok(true);
    }
    let service = super::super::super::probes::for_intent(profile, intent)?;
    let healthy = processes["processes"][&intent.service_id].is_number()
        && crate::ProbeExecutor::new(intent.kind == "diskio")?
            .probe_service(&service, &BTreeMap::new())
            .await
            .is_ok();
    step.phase = if healthy {
        SetupStepPhase::Ready
    } else {
        SetupStepPhase::Accepted
    };
    step.detail = Some(
        if healthy {
            "Service is ready"
        } else {
            "Monitor is applying accepted service intent"
        }
        .into(),
    );
    Ok(healthy)
}
