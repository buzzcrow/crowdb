// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovered authority commits address changes through the existing operation.

use super::super::{durable, Result};
use crowdb_console_shared::{
    deployment::{node_update, recovery::rebind_service, registry, services, PreparedBootstrap},
    ops::OpContext,
    ConsoleConfig,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::mgmt::node::{recovery::MemberEndpoints, NodeServiceAction};
use std::{path::Path, sync::Arc};

pub(super) async fn reconcile(
    root: &Path,
    operation: &PreparedBootstrap,
    endpoints: &MemberEndpoints,
    topology: &ConsoleConfig,
) -> Result<()> {
    let local = super::identity(root)?.endpoint;
    let mut config = topology.clone();
    let mut seeds = Vec::new();
    for node in &mut config.nodes {
        let Some(peer) = endpoints.members.iter().find(|peer| peer.node_id == node.id) else {
            continue;
        };
        let address = peer
            .addresses
            .iter()
            .find(|address| address.is_ipv4())
            .or_else(|| peer.addresses.first())
            .ok_or("member address missing")?;
        node.host = address.to_string();
        seeds.push(format!("http://{}", std::net::SocketAddr::new(*address, 10000)));
        for server in config
            .servers
            .iter_mut()
            .filter(|server| server.node_id == Some(node.id))
        {
            server.url = format!("http://{}", std::net::SocketAddr::new(*address, 10000));
            server.rpc_url = Some(std::net::SocketAddr::new(*address, 10100).to_string());
        }
    }
    let client = Arc::new(CrowdbKvClient::new(ClientConfig::new(seeds.clone())));
    let GetOutcome::Found { value, .. } = client
        .get(
            0,
            0,
            crowdb_console_shared::deployment::CLUSTER_KEY,
            ReadMode::Linearizable,
            None,
        )
        .await?
    else {
        return Err("cluster authority is absent".into());
    };
    let confirmed: PreparedBootstrap = serde_json::from_slice(&value)?;
    confirmed.validate()?;
    if confirmed.identity != operation.identity {
        return Err("recovered authority belongs to another bootstrap".into());
    }
    let ctx = OpContext::with_shared_client_preserving_hint(Arc::clone(&client), &seeds, config.clone());
    let (registry, _) = registry::read(&client).await?.ok_or("node registry is absent")?;
    if registry.cluster_id != operation.identity.cluster_id {
        return Err("recovered registry belongs to another cluster".into());
    }
    let mut record = registry
        .nodes
        .into_iter()
        .find(|node| {
            node.node_id == local.node_id
                && node.discovery_id == local.discovery_id
                && node.confirmed
                && !node.cancelled
        })
        .ok_or("confirmed local identity is absent")?;
    let current = config
        .node(local.node_id)
        .ok_or("local node topology missing")?
        .host
        .clone();
    if record.host != current {
        record.host.clone_from(&current);
        node_update::apply(&ctx, &operation.identity.cluster_id, record).await?;
    }
    super::owners::capture(root, &ctx, &operation.identity, local.node_id).await?;
    let complete = config
        .nodes
        .iter()
        .all(|node| endpoints.members.iter().any(|member| member.node_id == node.id));
    if !rebind(
        root,
        &client,
        operation,
        local.node_id,
        &current,
        &seeds,
        complete,
    )
    .await?
    {
        return Ok(());
    }
    if let Some(mut binding) =
        durable::read::<crowdb_protocol::mgmt::node::NodeBinding>(&root.join("node-binding.json"))?
    {
        if binding.bootstrap != operation.identity || binding.node_id != local.node_id {
            return Err("local binding changed during address publication".into());
        }
        if binding.management_seeds != seeds {
            binding.management_seeds = seeds;
            durable::write(&root.join("node-binding.json"), &binding)?;
        }
    }
    Ok(())
}

async fn rebind(
    root: &Path,
    client: &CrowdbKvClient,
    operation: &PreparedBootstrap,
    node: u64,
    current: &str,
    seeds: &[String],
    complete: bool,
) -> Result<bool> {
    let path = root.join("service-address.json");
    let old: String = match durable::read(&path)? {
        Some(address) => address,
        None => operation
            .intent
            .to_config()
            .node(node)
            .ok_or("original node missing")?
            .host
            .clone(),
    };
    let binding: Option<crowdb_protocol::mgmt::node::NodeBinding> =
        durable::read(&root.join("node-binding.json"))?;
    // A partial discovery round is not an address change. In particular,
    // never disrupt fresh services merely because one SSH proof is pending.
    // Own-address changes can publish a reachable subset; later complete
    // evidence refreshes the full contacts without oscillating on lost probes.
    if old == current
        && (!complete
            || binding
                .as_ref()
                .is_some_and(|binding| binding.management_seeds == seeds))
    {
        return Ok(false);
    }
    // New confirmed contacts leave all existing service seeds usable. Update
    // the monitor binding without replacing healthy service launch operations.
    if old == current
        && binding.as_ref().is_some_and(|binding| {
            !binding.management_seeds.is_empty()
                && binding.management_seeds.iter().all(|seed| seeds.contains(seed))
        })
    {
        durable::write(&path, &current)?;
        return Ok(true);
    }
    for intent in services::list(client).await? {
        if intent.node_id != node
            || intent.cluster_id != operation.identity.cluster_id
            || intent.kind == "kv"
            || !matches!(
                intent.action,
                NodeServiceAction::Start | NodeServiceAction::Restart
            )
        {
            continue;
        }
        let configuration = rebind_service(&intent.configuration, old.parse()?, current.parse()?, seeds)?;
        if configuration == intent.configuration {
            continue;
        }
        let key = format!("/deployment/services/{node}/{}", intent.service_id);
        let GetOutcome::Found { value, revision } = client
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await?
        else {
            continue;
        };
        let current_intent: crowdb_protocol::mgmt::node::NodeServiceIntent = serde_json::from_slice(&value)?;
        if current_intent != intent {
            return Err("service intent changed during address publication".into());
        }
        let mut rebound = intent;
        rebound.configuration = configuration;
        rebound.operation_id = uuid::Uuid::new_v4().to_string();
        client
            .put_cas(0, 0, key.as_bytes(), &serde_json::to_vec(&rebound)?, revision)
            .await?;
    }
    durable::write(&path, &current)?;
    Ok(true)
}

#[cfg(feature = "test-util")]
/// Exercise contact publication with no reachable authority.
///
/// # Errors
/// Propagates any attempted authority access or retained-state failure.
pub async fn rebind_recovery_services_for_tests(
    root: &Path,
    operation: &PreparedBootstrap,
    node: u64,
    current: &str,
    seeds: &[String],
    complete: bool,
) -> Result<bool> {
    let client = CrowdbKvClient::new(ClientConfig::new(vec!["http://127.0.0.1:9".into()]));
    rebind(root, &client, operation, node, current, seeds, complete).await
}
