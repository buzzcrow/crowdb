// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Initialize system-group processes and publish bootstrap metadata.

use super::server_client;
use crate::config::{ReplicaEntry, ServiceType};
use crate::error::{Error, Result};
use crate::ops::OpContext;
use std::collections::{HashMap, HashSet};

mod leader;
mod nodes;
mod publication;

/// Summary of a completed cluster init.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InitSummary {
    pub store_id: u64,
    pub group_id: u64,
    pub nodes: Vec<(u64, u64)>,
}

/// Initialize the cluster by bootstrapping group 0 on the listed nodes.
///
/// # Errors
/// Returns [`Error::Validation`] if `nodes` is empty;
/// [`Error::NodeUnreachable`] if a node is not reachable;
/// [`Error::UpstreamRpc`] if `system/init` fails on a node.
pub async fn init(ctx: &OpContext, nodes: &[u64]) -> Result<InitSummary> {
    if nodes.is_empty() {
        return Err(Error::Validation {
            field: "nodes".into(),
            message: "nodes list must not be empty".into(),
        });
    }

    let mut seen = HashSet::new();
    let mut target_nodes = nodes.to_vec();
    target_nodes.retain(|nid| seen.insert(*nid));
    let single_node = target_nodes.len() == 1;

    let succeeded = nodes::initialize(ctx, &target_nodes, single_node).await?;
    nodes::wire(ctx, &succeeded).await?;

    let store_nodes: Vec<u64> = succeeded.iter().map(|(n, _)| *n).collect();

    // Phase 4: seed the KV client with the group-0 leader endpoint so
    // `write_topology_to_sysdata` (which uses `ctx.sysmd()`) can reach
    // group 0. Without this, the shared `CrowdbKvClient` may have a
    // stale or dummy leader hint from before init.
    let mgmt_seeds: Vec<String> = succeeded
        .iter()
        .filter_map(|(nid, _)| ctx.node_mgmt_url(*nid).ok())
        .collect();
    ctx.kv().set_mgmt_seeds(mgmt_seeds.clone());

    // For multi-node init, the election driver starts after remotes are
    // wired (Phase 2) but the leader isn't elected yet. Wait for the
    // leader before seeding + writing sysdata — otherwise sysdata writes
    // fail with "not leader" and callers see spurious errors.
    leader::seed_leader_after_init(ctx, single_node, &succeeded, &mgmt_seeds).await;

    // Phase 5: write hardware + KV-cluster topology into group-0 sysdata.
    publication::write_topology_to_sysdata(ctx, &store_nodes, &succeeded).await?;

    let replicas: Vec<ReplicaEntry> = succeeded
        .iter()
        .map(|(nid, rid)| ReplicaEntry {
            replica_id: *rid,
            node_id: *nid,
        })
        .collect();
    {
        let mut cfg = ctx.config_mut();
        cfg.record_store(0, store_nodes.clone());
        cfg.record_group(0, 0, replicas);
    }

    wait_for_live_registration(ctx, &store_nodes).await?;
    propagate_discovery(ctx, &store_nodes, &mgmt_seeds).await?;

    Ok(InitSummary {
        store_id: 0,
        group_id: 0,
        nodes: succeeded,
    })
}

async fn propagate_discovery(ctx: &OpContext, members: &[u64], seeds: &[String]) -> Result<()> {
    let nonmembers: Vec<_> = ctx
        .config()
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Kv)
        .filter_map(|server| server.node_id)
        .filter(|node| !members.contains(node))
        .collect();
    for node in &nonmembers {
        server_client(ctx, *node)?
            .set_group0_discovery(seeds.to_vec())
            .await?;
    }
    if !nonmembers.is_empty() {
        wait_for_live_registration(ctx, &nonmembers).await?;
    }
    Ok(())
}

async fn wait_for_live_registration(ctx: &OpContext, nodes: &[u64]) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Ok(instances) = ctx.sysmd().read_all_kv_server_instances().await {
            let mut counts = HashMap::new();
            for (_, instance) in instances {
                let node_id = instance
                    .extra
                    .as_ref()
                    .and_then(|extra| extra.kv_server.as_ref())
                    .and_then(|identity| identity.node_id);
                if let Some(node_id) = node_id.filter(|_| !instance.rpc_endpoint.is_empty()) {
                    *counts.entry(node_id).or_insert(0usize) += 1;
                }
            }
            if nodes.iter().all(|node| counts.get(node) == Some(&1)) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::NodeUnreachable {
                node_id: format!("{nodes:?}"),
                reason: "Group 0 does not show exactly one live KV registration per node".into(),
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
