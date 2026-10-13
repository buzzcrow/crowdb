// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::durable;
use super::{start_kv, AcceptedNode, Result};
use crate::{DeploymentProfile, ProcessManager};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn execute(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    bootstrap: SystemBootstrapIdentity,
    confirm_delete_system_store: bool,
) -> Result<Value> {
    let root = &profile.paths.data_root;
    if !confirm_delete_system_store {
        return Err("explicit system-store deletion confirmation required".into());
    }
    let retired = root.join(format!(
        "retired-bootstrap-{}.json",
        uuid::Uuid::parse_str(&bootstrap.operation_id)?
    ));
    let accepted: Option<AcceptedNode> = durable::read(&root.join("accepted-node.json"))?;
    let Some(accepted) = accepted else {
        durable::write(&retired, &bootstrap)?;
        return Ok(json!({}));
    };
    if accepted.bootstrap != bootstrap {
        return if retired.exists() {
            Ok(json!({}))
        } else {
            Err("cleanup belongs to another operation".into())
        };
    }
    durable::write(&retired, &accepted)?;
    for intent in super::services::retained(profile)? {
        if processes.owns(&intent.service_id) {
            processes
                .stop(&intent.service_id, Duration::from_secs(10))
                .await?;
        }
    }
    start_kv(profile, processes, &accepted).await?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let response = client
        .post("http://127.0.0.1:10000/system/cleanup")
        .json(&json!({"bootstrap": bootstrap, "confirm_delete_system_store": true}))
        .send()
        .await;
    processes.stop("kv", Duration::from_secs(10)).await?;
    response?.error_for_status()?;
    for name in [
        "node-binding.json",
        "accepted-node.json",
        "prepared-bootstrap.json",
    ] {
        match std::fs::remove_file(root.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    std::fs::File::open(root)?.sync_all()?;
    if root.join("secrets").exists() {
        std::fs::rename(
            root.join("secrets"),
            root.join(format!("retired-secrets-{}", bootstrap.operation_id)),
        )?;
        std::fs::File::open(root)?.sync_all()?;
    }
    durable::write(&root.join("node-cleaned.json"), &bootstrap)?;
    Ok(json!({}))
}
