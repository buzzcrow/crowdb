// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{durable, service, AcceptedNode, Result};
use crate::{DeploymentProfile, ProcessManager};
use crowdb_protocol::mgmt::node::NodeBinding;
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

pub(super) fn resume_allowed(profile: &DeploymentProfile, accepted: &AcceptedNode) -> Result<bool> {
    let root = &profile.paths.data_root;
    Ok(accepted.prepared
        && !super::admission::is_cancelled(root, accepted)
        && !root
            .join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ))
            .exists()
        && !super::services::kv_paused(profile)?
        && super::recovery::ready(root, accepted)?)
}

pub(super) async fn resume(profile: &DeploymentProfile, processes: &mut ProcessManager) -> Result<()> {
    if let Some(accepted) = durable::read(&profile.paths.data_root.join("accepted-node.json"))? {
        if resume_allowed(profile, &accepted)? {
            start_kv(profile, processes, &accepted).await?;
        }
    }
    Ok(())
}

pub(super) fn node_profile(config: &super::NodeRuntimeConfig) -> Result<DeploymentProfile> {
    let mut profile = DeploymentProfile::load(&config.profile)?;
    profile.validate_node_catalog()?;
    for service in &mut profile.services {
        service
            .env
            .insert("CROWDB_PHYSICAL_HOST_ID".into(), config.physical_host_id.clone());
    }
    let address = config
        .discovery
        .addresses
        .iter()
        .find(|address| address.is_ipv4() && !address.is_loopback())
        .or_else(|| config.discovery.addresses.first())
        .ok_or("node requires a management advertisement address")?;
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "kv")
        .ok_or("node profile requires KV service")?
        .env
        .insert("CROWDB_KV_MANAGEMENT_ADVERTISE_ADDR".into(), address.to_string());
    Ok(profile)
}

pub(super) async fn bind(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    binding: NodeBinding,
) -> Result<Value> {
    let root = &profile.paths.data_root;
    if root
        .join(format!(
            "retired-bootstrap-{}.json",
            uuid::Uuid::parse_str(&binding.bootstrap.operation_id)?
        ))
        .exists()
    {
        return Err("bootstrap operation is retired".into());
    }
    let accepted: AcceptedNode =
        durable::read(&root.join("accepted-node.json"))?.ok_or("node not prepared")?;
    if !accepted.prepared
        || accepted.node_id != binding.node_id
        || accepted.bootstrap != binding.bootstrap
        || super::admission::is_cancelled(root, &accepted)
    {
        return Err("binding differs from accepted bootstrap".into());
    }
    super::verify_binding(&binding).await?;
    let first_binding = !root.join("node-binding.json").exists();
    durable::write(&root.join("node-binding.json"), &binding)?;
    if first_binding
        && !root
            .join(format!("kv/node-{}/conf/system-bootstrap.json", accepted.node_id))
            .exists()
    {
        processes.stop("kv", Duration::from_secs(10)).await?;
        start_kv(profile, processes, &accepted).await?;
    }
    Ok(json!({}))
}

pub(super) async fn start_kv(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    accepted: &AcceptedNode,
) -> Result<()> {
    super::recovery::check(&profile.paths.data_root, accepted)?;
    if processes.owns("kv") && processes.alive("kv")? {
        return Ok(());
    }
    if processes.owns("kv") {
        processes.stop("kv", Duration::from_secs(5)).await?;
    }
    let mut kv = service(profile, "kv")?;
    let root = profile
        .paths
        .data_root
        .join("kv")
        .join(format!("node-{}", accepted.node_id));
    std::fs::create_dir_all(&root)?;
    kv.args = vec![
        "--root".into(),
        root.to_string_lossy().into_owned(),
        "--management-port".into(),
        "10000".into(),
        "--ports".into(),
        "10100,10101".into(),
        "--instance-id".into(),
        accepted.node_id.to_string(),
        "--node-id".into(),
        accepted.node_id.to_string(),
        "--binding-monitor-interval".into(),
        "1".into(),
        "--log-dir".into(),
        profile.paths.log_root.join("kv").to_string_lossy().into_owned(),
        "--log".into(),
    ];
    if let Some(binding) = durable::read::<NodeBinding>(&profile.paths.data_root.join("node-binding.json"))? {
        for seed in binding.management_seeds {
            kv.args.extend(["--group0-management-seed".into(), seed]);
        }
    }
    processes.start(&kv, &BTreeMap::new()).await?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .get("http://127.0.0.1:10000/health")
            .send()
            .await
            .is_ok_and(|reply| reply.status().is_success())
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline || !processes.alive("kv")? {
            return Err("KV server did not become ready".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
pub(super) async fn ensure_start_allowed(profile: &crate::DeploymentProfile) -> super::Result<()> {
    use crowdb_protocol::mgmt::node::NodeServiceAction;
    if profile.paths.data_root.join("node-binding.json").exists()
        && super::services::desired(profile).await?.iter().any(|intent| {
            intent.kind == "kv"
                && matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete)
        })
    {
        return Err("KV is stopped by the current cluster intent".into());
    }
    Ok(())
}
