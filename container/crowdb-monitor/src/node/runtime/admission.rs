// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fence cancellation by the current Group 0 admission operation.

use super::{durable, AcceptedNode, Result};
use crate::{DeploymentProfile, NodeIdentity, ProcessManager};
use crowdb_console_shared::deployment::registry;
use crowdb_protocol::mgmt::{node::NodeAdmissionGrant, SystemBootstrapIdentity};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};

pub(super) async fn validate(
    root: &Path,
    bootstrap: &SystemBootstrapIdentity,
    node_id: u64,
    grant: &NodeAdmissionGrant,
    cancelled: bool,
) -> Result<()> {
    let operation = uuid::Uuid::parse_str(&grant.operation_id)?;
    if operation.is_nil() || grant.management_seeds.is_empty() || grant.management_seeds.len() > 16 {
        return Err("invalid admission grant".into());
    }
    if !cancelled && root.join(format!("ssh/cancelled-{operation}.json")).exists() {
        return Err("admission operation cancelled".into());
    }
    let discovery_id = NodeIdentity::load_or_create(root)?.uuid().to_string();
    let client = crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        grant.management_seeds.clone(),
    ));
    let publication = tokio::time::timeout(
        Duration::from_secs(3),
        client.get(
            0,
            0,
            crowdb_console_shared::deployment::CLUSTER_KEY,
            crowdb_kv_client::ReadMode::Linearizable,
            None,
        ),
    )
    .await??;
    let crowdb_kv_client::GetOutcome::Found { value, .. } = publication else {
        return Err("cluster publication unavailable".into());
    };
    let publication: crowdb_console_shared::deployment::PreparedBootstrap = serde_json::from_slice(&value)?;
    if publication.identity != *bootstrap {
        return Err("admission cluster authority differs".into());
    }
    let (registry, _) = tokio::time::timeout(Duration::from_secs(3), registry::read(&client))
        .await??
        .ok_or("admission mapping unavailable")?;
    if registry.cluster_id != bootstrap.cluster_id
        || !registry.nodes.iter().any(|node| {
            node.discovery_id == discovery_id
                && node.node_id == node_id
                && node.operation_id == grant.operation_id
                && node.cancelled == cancelled
                && (!cancelled || !node.confirmed)
        })
    {
        return Err("admission grant differs from current authority".into());
    }
    Ok(())
}

pub(super) async fn cancel(
    profile: &DeploymentProfile,
    processes: &mut ProcessManager,
    bootstrap: SystemBootstrapIdentity,
    node_id: u64,
    grant: NodeAdmissionGrant,
) -> Result<Value> {
    let root = &profile.paths.data_root;
    validate(root, &bootstrap, node_id, &grant, true).await?;
    let accepted: Option<AcceptedNode> = durable::read(&root.join("accepted-node.json"))?;
    if let Some(accepted) = &accepted {
        if accepted.bootstrap != bootstrap
            || accepted.node_id != node_id
            || accepted.admission.as_ref() != Some(&grant)
            || root.join("node-binding.json").exists()
            || root
                .join(format!("kv/node-{node_id}/conf/system-bootstrap.json"))
                .exists()
        {
            return Err("cannot cancel a bound node or another preparation".into());
        }
    }
    // Persist the fence before touching the child or credentials. Restart must
    // not resurrect a pending replica after an interrupted cancellation.
    durable::write(
        &root.join(format!("ssh/cancelled-{}.json", grant.operation_id)),
        &grant,
    )?;
    if accepted.is_some() {
        if processes.owns("kv") {
            processes.stop("kv", Duration::from_secs(10)).await?;
        }
        if root.join("secrets").exists() {
            std::fs::rename(
                root.join("secrets"),
                root.join(format!("cancelled-secrets-{}", grant.operation_id)),
            )?;
        }
        std::fs::remove_file(root.join("accepted-node.json"))?;
        std::fs::File::open(root)?.sync_all()?;
    }
    Ok(json!({}))
}

pub(super) fn is_cancelled(root: &Path, accepted: &AcceptedNode) -> bool {
    accepted.admission.as_ref().is_some_and(|grant| {
        root.join(format!("ssh/cancelled-{}.json", grant.operation_id))
            .exists()
    })
}
