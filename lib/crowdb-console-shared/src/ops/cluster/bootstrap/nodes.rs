// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Identity-checked initialization; failed attempts preserve groups for resume.

use crowdb_protocol::mgmt::{
    RemoteReplicaInfo, SystemBootstrapIdentity, SystemInitRequest, SystemPrepareRequest,
};

use super::super::server_client;
use crate::error::{Error, Result};
use crate::ops::OpContext;

pub(super) async fn initialize(
    ctx: &OpContext,
    nodes: &[u64],
    single: bool,
    bootstrap: Option<&SystemBootstrapIdentity>,
) -> Result<Vec<(u64, u64)>> {
    let results = futures::future::join_all(
        nodes
            .iter()
            .enumerate()
            .map(|(index, node)| initialize_node(ctx, *node, index as u64 + 1, single, bootstrap)),
    )
    .await;
    // A peer may already have committed or elected a leader. Removing Group 0
    // here can destroy existing authority; leave each process available for
    // an identity-checked retry instead.
    results.into_iter().collect()
}

async fn initialize_node(
    ctx: &OpContext,
    node: u64,
    replica: u64,
    single: bool,
    bootstrap: Option<&SystemBootstrapIdentity>,
) -> Result<(u64, u64)> {
    let client = server_client(ctx, node)?;
    client.health().await.map_err(|error| Error::NodeUnreachable {
        node_id: node.to_string(),
        reason: error.to_string(),
    })?;
    let result = client
        .system_init(&SystemInitRequest {
            replica_id: replica,
            start_election: single,
            bootstrap: bootstrap.cloned(),
        })
        .await;
    match result {
        Ok(reply) if reply.store_id == 0 && reply.group_id == 0 && reply.replica_id == replica => {
            Ok((node, replica))
        }
        Ok(_) => Err(identity_conflict(node, replica)),
        Err(error) if bootstrap.is_some() => Err(error),
        Err(error) => {
            // A conflict or lost reply is not proof of the intended identity.
            // Read the actual process topology before accepting the retry.
            let topology = client.topology().await?;
            let actual = topology
                .iter()
                .find(|store| store.store_id == 0)
                .and_then(|store| store.groups.iter().find(|group| group.group_id == 0));
            match actual {
                Some(group) if group.local_replica_id == replica => Ok((node, replica)),
                Some(_) => Err(identity_conflict(node, replica)),
                None => Err(error),
            }
        }
    }
}

fn identity_conflict(node: u64, replica: u64) -> Error {
    Error::Conflict {
        kind: "bootstrap replica identity".into(),
        id: format!("node {node}, expected replica {replica}"),
    }
}

pub(super) async fn wire(ctx: &OpContext, members: &[(u64, u64)]) -> Result<()> {
    if members.len() < 2 {
        return Ok(());
    }
    let mut endpoints = Vec::with_capacity(members.len());
    for (node, _) in members {
        let topology = server_client(ctx, *node)?.topology().await?;
        let endpoint = topology
            .into_iter()
            .find(|store| store.store_id == 0)
            .and_then(|store| store.listen_addr)
            .filter(|endpoint| !endpoint.is_empty())
            .ok_or_else(|| Error::NodeUnreachable {
                node_id: node.to_string(),
                reason: "system store RPC endpoint is missing".into(),
            })?;
        let endpoint = endpoint
            .strip_prefix("http://")
            .or_else(|| endpoint.strip_prefix("https://"))
            .unwrap_or(&endpoint);
        let resolved = if let Some(port) = endpoint.strip_prefix("0.0.0.0:") {
            let management = reqwest::Url::parse(&ctx.node_mgmt_url(*node)?)
                .map_err(|error| Error::Config(error.to_string()))?;
            let host = management
                .host_str()
                .ok_or_else(|| Error::Config("management URL has no host".into()))?;
            format!("{host}:{port}")
        } else {
            endpoint.to_owned()
        };
        endpoints.push(resolved);
    }
    for (index, (node, _)) in members.iter().enumerate() {
        let remotes: Vec<_> = members
            .iter()
            .enumerate()
            .filter(|(peer, _)| *peer != index)
            .map(|(peer, (_, replica))| RemoteReplicaInfo {
                replica_id: *replica,
                endpoint: endpoints[peer].clone(),
                voting: true,
            })
            .collect();
        server_client(ctx, *node)?
            .add_remote_replicas(0, 0, &remotes)
            .await?;
    }
    Ok(())
}

pub(super) async fn prepare(
    ctx: &OpContext,
    nodes: &[u64],
    identity: &SystemBootstrapIdentity,
) -> Result<()> {
    let results = futures::future::join_all(nodes.iter().enumerate().map(|(index, node)| async move {
        let request = SystemPrepareRequest {
            replica_id: index as u64 + 1,
            bootstrap: identity.clone(),
        };
        let accepted = server_client(ctx, *node)?.system_prepare(&request).await?;
        if accepted.replica_id != request.replica_id || accepted.bootstrap != request.bootstrap {
            return Err(identity_conflict(*node, request.replica_id));
        }
        Ok(())
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(())
}
