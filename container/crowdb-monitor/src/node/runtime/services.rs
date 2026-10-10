// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{durable, service, verify_binding, Result};
use crate::{DeploymentProfile, ProcessManager};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceAction, NodeServiceIntent};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
pub(super) struct Execution {
    pub(super) intent: NodeServiceIntent,
    complete: bool,
    pid: Option<u32>,
}

pub(super) async fn execute(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    intent: NodeServiceIntent,
) -> Result<Value> {
    validate(&intent)?;
    let binding: NodeBinding =
        durable::read(&profile.paths.data_root.join("node-binding.json"))?.ok_or("node is unbound")?;
    if binding.node_id != intent.node_id || binding.bootstrap.cluster_id != intent.cluster_id {
        return Err("service intent belongs to another node or cluster".into());
    }
    tokio::time::timeout(Duration::from_secs(3), verify_binding(&binding)).await??;
    verify_intent(&binding, &intent).await?;
    let process_id = if intent.kind == "kv" {
        "kv"
    } else {
        &intent.service_id
    };
    let directory = profile.paths.data_root.join("services").join(&intent.service_id);
    std::fs::create_dir_all(&directory)?;
    let state = directory.join("execution.json");
    if let Some(previous) = durable::read::<Execution>(&state)? {
        if previous.intent.operation_id == intent.operation_id {
            if previous.intent != intent {
                return Err("operation identity reused with other inputs".into());
            }
            if previous.complete
                && (previous.pid.is_none() || (processes.owns(process_id) && processes.alive(process_id)?))
            {
                return Ok(json!({"pid": processes.pid(process_id), "operation_id": intent.operation_id}));
            }
        }
    }
    durable::write(
        &state,
        &Execution {
            intent: intent.clone(),
            complete: false,
            pid: None,
        },
    )?;
    if processes.owns(process_id) {
        processes.stop(process_id, Duration::from_secs(10)).await?;
    }
    verify_intent(&binding, &intent).await?;
    let pid = if matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    ) {
        if intent.kind == "kv" {
            let accepted =
                durable::read::<super::AcceptedNode>(&profile.paths.data_root.join("accepted-node.json"))?
                    .ok_or("KV acceptance is absent")?;
            super::start_kv(profile, processes, &accepted).await?;
            processes.pid("kv")
        } else {
            let mut service = service(profile, &intent.kind)?;
            service.id.clone_from(&intent.service_id);
            let config = directory.join("service.toml");
            durable::write_bytes(&config, intent.configuration.as_bytes())?;
            service.args = vec!["--config".into(), config.to_string_lossy().into_owned()];
            service.env = intent.environment.clone();
            if intent.kind == "access" {
                let credentials = crate::ServerCredentials::load_existing(&profile.paths.data_root)?;
                for line in credentials.server_env().lines() {
                    if let Some((name, value)) = line.split_once('=') {
                        service.env.insert(name.into(), value.into());
                    }
                }
            }
            Some(
                processes
                    .start(&service, &std::collections::BTreeMap::new())
                    .await?,
            )
        }
    } else {
        None
    };
    durable::write(
        &state,
        &Execution {
            intent: intent.clone(),
            complete: true,
            pid,
        },
    )?;
    Ok(json!({"pid": pid, "operation_id": intent.operation_id}))
}

async fn verify_intent(binding: &NodeBinding, intent: &NodeServiceIntent) -> Result<()> {
    let client = crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        binding.management_seeds.clone(),
    ));
    let key = format!("/deployment/services/{}/{}", intent.node_id, intent.service_id);
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        client.get(
            0,
            0,
            key.as_bytes(),
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        ),
    )
    .await??;
    let crowdb_kv_client::GetOutcome::Found { value, .. } = outcome else {
        return Err("service intent is not committed".into());
    };
    let current: NodeServiceIntent = serde_json::from_slice(&value)?;
    if &current != intent {
        return Err("service intent was superseded".into());
    }
    Ok(())
}

fn validate(intent: &NodeServiceIntent) -> Result<()> {
    if uuid::Uuid::parse_str(&intent.operation_id)?.is_nil()
        || intent.service_id.is_empty()
        || intent.service_id.len() > 80
        || intent
            .service_id
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b"-_".contains(&byte))
        || !["kv", "diskdb", "diskio", "chunkdb", "chunk-kv", "access"].contains(&intent.kind.as_str())
        || intent.configuration.len() > 32768
    {
        return Err("invalid managed service intent".into());
    }
    if intent.environment.iter().any(|(name, value)| {
        ![
            "CROWDB_CHUNKDB_OWNERSHIP_POLICY",
            "CROWDB_S3_PUBLIC_URI",
            "CROWDB_ICEBERG_PUBLIC_URI",
            "CROWDB_ACCESS_HEALTH_LISTEN",
        ]
        .contains(&name.as_str())
            || value.len() > 4096
    }) {
        return Err("service environment contains unsupported or private settings".into());
    }
    if matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    ) {
        let _: toml::Value = toml::from_str(&intent.configuration)?;
    }
    Ok(())
}

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
    Ok(records
        .into_iter()
        .filter(|intent| {
            intent.node_id == binding.node_id && intent.cluster_id == binding.bootstrap.cluster_id
        })
        .collect())
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
    if previous.intent != *intent || !previous.complete {
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
