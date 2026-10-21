// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{bounded_error, durable, service, Execution, Result};
use crate::{DeploymentProfile, ProcessManager};
use crowdb_protocol::mgmt::node::{NodeBinding, NodeServiceAction, NodeServiceIntent};
use serde_json::{json, Value};
use std::time::Duration;

pub(in crate::node::runtime) async fn execute(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    intent: NodeServiceIntent,
) -> Result<Value> {
    validate(&intent)?;
    super::policy::validate(&profile.paths.data_root, &intent)?;
    let binding: NodeBinding =
        durable::read(&profile.paths.data_root.join("node-binding.json"))?.ok_or("node is unbound")?;
    if binding.node_id != intent.node_id || binding.bootstrap.cluster_id != intent.cluster_id {
        return Err("service intent belongs to another node or cluster".into());
    }
    tokio::time::timeout(Duration::from_secs(3), super::super::verify_binding(&binding)).await??;
    verify_intent(&binding, &intent).await?;
    let process_id = if intent.kind == "kv" {
        "kv"
    } else {
        &intent.service_id
    };
    let directory = profile.paths.data_root.join("services").join(&intent.service_id);
    std::fs::create_dir_all(&directory)?;
    let state = directory.join("execution.json");
    let mut attempts = 0;
    if let Some(previous) = durable::read::<Execution>(&state)? {
        if previous.intent.operation_id == intent.operation_id {
            if previous.intent != intent {
                return Err("operation identity reused with other inputs".into());
            }
            attempts = previous.attempts;
            if previous.complete
                && (previous.pid.is_none() || (processes.owns(process_id) && processes.alive(process_id)?))
            {
                return Ok(json!({"pid": processes.pid(process_id), "operation_id": intent.operation_id}));
            }
        }
    }
    if matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    ) {
        if attempts >= 5 {
            return Err(
                "service operation exceeded its durable restart budget; use explicit service controls".into(),
            );
        }
        attempts += 1;
    }
    durable::write(
        &state,
        &Execution {
            intent: intent.clone(),
            complete: false,
            pid: None,
            attempts,
            error: None,
        },
    )?;
    let result = apply(profile, processes, &binding, &intent, process_id, &directory).await;
    let (complete, pid, error) = match &result {
        Ok(pid) => (true, *pid, None),
        Err(error) => (false, None, Some(bounded_error(&error.to_string()))),
    };
    durable::write(
        &state,
        &Execution {
            intent: intent.clone(),
            complete,
            pid,
            attempts: if complete { 0 } else { attempts },
            error,
        },
    )?;
    result?;
    Ok(json!({"pid": pid, "operation_id": intent.operation_id}))
}

async fn apply(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    binding: &NodeBinding,
    intent: &NodeServiceIntent,
    process_id: &str,
    directory: &std::path::Path,
) -> Result<Option<u32>> {
    // Fence the admitted operation immediately before disrupting its service.
    // KV may provide the authority being read, so verify while it is still live.
    verify_intent(binding, intent).await?;
    if processes.owns(process_id) {
        processes.stop(process_id, Duration::from_secs(10)).await?;
    }
    let pid = if matches!(
        intent.action,
        NodeServiceAction::Start | NodeServiceAction::Restart
    ) {
        if intent.kind == "kv" {
            let accepted = durable::read::<super::super::AcceptedNode>(
                &profile.paths.data_root.join("accepted-node.json"),
            )?
            .ok_or("KV acceptance is absent")?;
            super::super::start_kv(profile, processes, &accepted).await?;
            processes.pid("kv")
        } else {
            let mut service = service(profile, &intent.kind)?;
            let physical_host = service.env.get("CROWDB_PHYSICAL_HOST_ID").cloned();
            service.id.clone_from(&intent.service_id);
            let config = directory.join("service.toml");
            durable::write_bytes(&config, intent.configuration.as_bytes())?;
            service.args = vec!["--config".into(), config.to_string_lossy().into_owned()];
            service.env = intent.environment.clone();
            if let Some(host) = physical_host {
                service.env.insert("CROWDB_PHYSICAL_HOST_ID".into(), host);
            }
            let logs = profile.paths.log_root.join(&intent.service_id);
            if matches!(intent.kind.as_str(), "diskdb" | "chunkdb" | "chunk-kv") {
                service
                    .args
                    .extend(["--log-dir".into(), logs.to_string_lossy().into_owned()]);
            }
            if intent.kind == "access" {
                service.env.insert(
                    "CROWDB_ACCESS_LOG_DIR".into(),
                    logs.to_string_lossy().into_owned(),
                );
                let credentials = crate::ServerCredentials::load_existing(&profile.paths.data_root)?;
                for line in credentials.server_env().lines() {
                    if let Some((name, value)) = line.split_once('=') {
                        service.env.insert(name.into(), value.into());
                    }
                }
            }
            let pid = processes
                .start(&service, &std::collections::BTreeMap::new())
                .await?;
            wait_ready(profile, processes, intent).await?;
            Some(pid)
        }
    } else {
        None
    };
    Ok(pid)
}

async fn wait_ready(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    intent: &NodeServiceIntent,
) -> Result<()> {
    let service = super::super::probes::for_intent(profile, intent)?;
    let probes = crate::ProbeExecutor::new(intent.kind == "diskio")?;
    let environment = std::collections::BTreeMap::new();
    let mut last_error = crate::ProbeError::Unavailable;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !processes.alive(&intent.service_id)? {
                let status = processes.exit_status(&intent.service_id)?;
                let code = status.and_then(|status| status.code());
                return Err(format!(
                    "managed service exited before readiness: node={} service={} exit_code={} exit_status={}",
                    intent.node_id,
                    intent.service_id,
                    code.map_or_else(|| "unknown".into(), |code| code.to_string()),
                    status.map_or_else(|| "unknown".into(), |status| status.to_string())
                )
                .into());
            }
            match probes.probe_service(&service, &environment).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = error,
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        format!(
            "managed service {} readiness failed: {last_error}",
            intent.service_id
        )
    })?
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
            "CROWDB_DATASET_PUBLIC_URI",
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
