// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{durable, service, Result};
use crate::{DeploymentProfile, ProcessManager};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceAction, NodeServiceIntent};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
pub(super) struct Execution {
    pub(super) intent: NodeServiceIntent,
    complete: bool,
    pid: Option<u32>,
    #[serde(default)]
    attempts: u32,
    #[serde(default)]
    error: Option<String>,
}

#[cfg(feature = "test-util")]
mod test_support;
#[cfg(feature = "test-util")]
pub use test_support::NodeServiceForTests;
mod execution;
mod policy;
pub(super) use execution::execute;

pub(super) fn retained(profile: &DeploymentProfile) -> Result<Vec<NodeServiceIntent>> {
    let directory = profile.paths.data_root.join("services");
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut intents = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(execution) = durable::read::<Execution>(&entry.path().join("execution.json"))? {
                intents.push(execution.intent);
            }
        }
        if intents.len() > 4096 {
            return Err("retained services exceed bound".into());
        }
    }
    ordered(profile, intents)
}

fn ordered(
    profile: &DeploymentProfile,
    mut intents: Vec<NodeServiceIntent>,
) -> Result<Vec<NodeServiceIntent>> {
    let ranks: std::collections::BTreeMap<_, _> = profile
        .services_in_start_order()?
        .into_iter()
        .enumerate()
        .map(|(rank, service)| (service.id.as_str(), rank))
        .collect();
    intents.sort_by(|left, right| {
        ranks
            .get(left.kind.as_str())
            .unwrap_or(&usize::MAX)
            .cmp(ranks.get(right.kind.as_str()).unwrap_or(&usize::MAX))
            .then_with(|| left.service_id.cmp(&right.service_id))
    });
    Ok(intents)
}

pub(super) fn kv_paused(profile: &DeploymentProfile) -> Result<bool> {
    Ok(retained(profile)?.iter().any(|intent| {
        intent.kind == "kv" && matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete)
    }))
}

pub(super) async fn desired(profile: &DeploymentProfile) -> Result<Vec<NodeServiceIntent>> {
    let binding: NodeBinding =
        durable::read(&profile.paths.data_root.join("node-binding.json"))?.ok_or("node unbound")?;
    let client =
        crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(binding.management_seeds));
    let records = tokio::time::timeout(
        Duration::from_secs(3),
        client.scan_bounded(0, 0, b"/deployment/services/", b"", b"", 4096, false, None),
    )
    .await??;
    if records.truncated || records.timed_out {
        return Err("deployment view exceeds bound".into());
    }
    let records = records
        .items
        .into_iter()
        .map(|(_, value)| serde_json::from_slice::<NodeServiceIntent>(&value))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ordered(
        profile,
        records
            .into_iter()
            .filter(|intent| {
                intent.node_id == binding.node_id && intent.cluster_id == binding.bootstrap.cluster_id
            })
            .collect(),
    )
}

pub(super) fn pending(
    profile: &DeploymentProfile,
    intent: &NodeServiceIntent,
    processes: &mut ProcessManager,
) -> Result<bool> {
    let path = profile
        .paths
        .data_root
        .join("services")
        .join(&intent.service_id)
        .join("execution.json");
    let Some(previous) = durable::read::<Execution>(&path)? else {
        return Ok(true);
    };
    if previous.intent != *intent {
        return Ok(true);
    }
    if previous.attempts >= 5
        && matches!(
            intent.action,
            NodeServiceAction::Start | NodeServiceAction::Restart
        )
    {
        return Ok(false);
    }
    if !previous.complete {
        return Ok(true);
    }
    let id = if intent.kind == "kv" {
        "kv"
    } else {
        &intent.service_id
    };
    Ok(matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    ) && (!processes.owns(id) || !processes.alive(id)?))
}

fn bounded_error(error: &str) -> String {
    error.chars().take(128).collect()
}

pub(super) fn failures(root: &std::path::Path) -> Result<Value> {
    let directory = root.join("services");
    let mut failures = serde_json::Map::new();
    if !directory.exists() {
        return Ok(Value::Object(failures));
    }
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(execution) = durable::read::<Execution>(&entry.path().join("execution.json"))? {
            if let Some(error) = execution.error {
                if failures.len() >= 64 {
                    return Err("service failure observation exceeds bound".into());
                }
                failures.insert(
                    execution.intent.service_id,
                    serde_json::json!({
                        "operation_id":execution.intent.operation_id, "attempts":execution.attempts,
                        "error":bounded_error(&error), "exhausted":execution.attempts >= 5,
                    }),
                );
            }
        }
    }
    Ok(Value::Object(failures))
}
