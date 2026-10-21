// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pair health with its executed operation before exposing local evidence.

use super::{durable, Execution, Result};
use crate::{DeploymentProfile, MonitorStatus};
use crowdb_protocol::mgmt::{
    cluster::DependencyState,
    node::{observation::NodeServiceObservation, NodeServiceAction},
};
use std::collections::BTreeMap;

pub(in crate::node::runtime) fn observe(
    profile: &DeploymentProfile,
    live: &BTreeMap<String, u32>,
    health: Option<&MonitorStatus>,
    now: u64,
) -> Result<Vec<NodeServiceObservation>> {
    let mut observed = Vec::new();
    let directory = profile.paths.data_root.join("services");
    if directory.try_exists()? {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Some(execution) = durable::read::<Execution>(&entry.path().join("execution.json"))? {
                observed.push(executed(&execution, live, health, now));
            }
            if observed.len() > 64 {
                return Err("local service observation exceeds bound".into());
            }
        }
    }
    if !observed.iter().any(|service| service.kind == "kv") {
        let status = health.and_then(|health| health.services.get("kv"));
        observed.push(NodeServiceObservation {
            service_id: "kv".into(),
            kind: "kv".into(),
            operation_id: None,
            state: if status.is_some_and(|status| {
                status.healthy && status.pid == live.get("kv").copied() && status.pid.is_some()
            }) {
                DependencyState::Ready
            } else {
                DependencyState::Unknown
            },
            observed_at_unix_ms: health.map_or(now, |health| health.updated_at_ms),
            max_age_ms: 3000,
            reason_code: None,
            detail: "KV health must match the currently supervised process and observation timestamp".into(),
        });
    }
    observed.sort_by(|left, right| left.service_id.cmp(&right.service_id));
    Ok(observed)
}

fn executed(
    execution: &Execution,
    live: &BTreeMap<String, u32>,
    health: Option<&MonitorStatus>,
    now: u64,
) -> NodeServiceObservation {
    let intent = &execution.intent;
    let process = if intent.kind == "kv" {
        "kv"
    } else {
        &intent.service_id
    };
    let current = health
        .and_then(|health| health.services.get(process))
        .filter(|status| {
            status.operation_id.as_ref() == Some(&intent.operation_id)
                && status.pid.is_some()
                && status.pid == live.get(process).copied()
        });
    let state = if execution.complete
        && matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete)
    {
        DependencyState::Stopped
    } else if !execution.complete
        && execution.attempts >= 5
        && execution.error.is_some()
        && !live.contains_key(process)
    {
        DependencyState::Failed
    } else if execution.complete && current.is_some_and(|status| status.healthy) {
        DependencyState::Ready
    } else {
        DependencyState::Initializing
    };
    NodeServiceObservation {
        detail: format!(
            "Service {}: {:?}; executed operation {}; complete {}; launches {}; live process {}",
            intent.service_id,
            state,
            intent.operation_id,
            execution.complete,
            execution.attempts,
            live.contains_key(process)
        ),
        service_id: intent.service_id.clone(),
        kind: intent.kind.clone(),
        operation_id: Some(intent.operation_id.clone()),
        state,
        observed_at_unix_ms: if state == DependencyState::Ready {
            health.map_or(now, |health| health.updated_at_ms)
        } else {
            now
        },
        max_age_ms: 3000,
        reason_code: match state {
            DependencyState::Failed => Some("service-restart-budget-exhausted".into()),
            DependencyState::Stopped => Some("operator-stopped-service".into()),
            DependencyState::Ready => None,
            _ => Some("service-health-not-current".into()),
        },
    }
}
