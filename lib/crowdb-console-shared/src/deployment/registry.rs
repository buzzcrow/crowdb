// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional UUID allocation and public admission state in Group 0.

use crate::error::{Error, Result};
use crowdb_kv_client::{CrowdbKvClient, Error as KvError, GetOutcome, ReadMode};
use serde::{Deserialize, Serialize};

pub const NODES_KEY: &[u8] = b"/deployment/nodes";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub discovery_id: String,
    pub node_id: u64,
    pub physical_host_id: String,
    pub rack_id: u64,
    pub host: String,
    pub ssh_port: u16,
    pub ssh_user: String,
    pub operation_id: String,
    pub confirmed: bool,
    #[serde(default)]
    pub cancelled: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeRegistry {
    pub cluster_id: String,
    pub nodes: Vec<NodeRecord>,
}

/// # Errors
/// Rejects conflicting bootstrap mapping or unavailable quorum.
pub async fn publish(kv: &CrowdbKvClient, registry: &NodeRegistry) -> Result<()> {
    let bytes = serde_json::to_vec(registry).map_err(config_error)?;
    match kv.put_cas(0, 0, NODES_KEY, &bytes, 0).await {
        Ok(_) => Ok(()),
        Err(error) => match read(kv).await? {
            Some((actual, _))
                if actual.cluster_id == registry.cluster_id
                    && registry.nodes.iter().all(|initial| {
                        actual.nodes.iter().any(|node| {
                            node.discovery_id == initial.discovery_id
                                && node.node_id == initial.node_id
                                && node.confirmed
                                && !node.cancelled
                        })
                    }) =>
            {
                Ok(())
            }
            _ => Err(error.into()),
        },
    }
}

/// Reserve or confirm a stable numeric ID using one conditional Group 0 record.
///
/// # Errors
/// Rejects duplicate UUID endpoints, changed clusters and unavailable authority.
pub async fn admit(kv: &CrowdbKvClient, cluster: &str, mut node: NodeRecord) -> Result<NodeRecord> {
    super::node_update::check_admission(kv, &node.discovery_id).await?;
    for attempt in 0..32 {
        let (mut registry, revision) = read(kv)
            .await?
            .ok_or_else(|| Error::Config("cluster mapping is not published".into()))?;
        if registry.cluster_id != cluster {
            return Err(Error::Config("cluster identity differs".into()));
        }
        if let Some(existing) = registry
            .nodes
            .iter_mut()
            .find(|entry| entry.discovery_id == node.discovery_id)
        {
            if existing.cancelled && existing.operation_id == node.operation_id {
                return Err(Error::Config("admission operation was cancelled".into()));
            }
            if existing.cancelled && !cancellation_complete(kv, cluster, existing).await? {
                return Err(Error::Config(
                    "admission cancellation cleanup is incomplete".into(),
                ));
            }
            if existing.host != node.host
                || existing.ssh_port != node.ssh_port
                || existing.physical_host_id != node.physical_host_id
            {
                return Err(Error::Conflict {
                    kind: "discovery UUID".into(),
                    id: node.discovery_id,
                });
            }
            node.node_id = existing.node_id;
            if !existing.cancelled {
                node.operation_id = existing.operation_id.clone();
            }
            if existing.confirmed {
                if existing.rack_id != node.rack_id || existing.ssh_user != node.ssh_user {
                    return Err(Error::Config(
                        "confirmed node changes require authenticated update".into(),
                    ));
                }
                return Ok(existing.clone());
            }
            *existing = node.clone();
        } else {
            node.node_id = registry
                .nodes
                .iter()
                .map(|entry| entry.node_id)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| Error::Config("node IDs exhausted".into()))?;
            registry.nodes.push(node.clone());
        }
        let bytes = serde_json::to_vec(&registry).map_err(config_error)?;
        match kv.put_cas(0, 0, NODES_KEY, &bytes, revision).await {
            Ok(_) => return Ok(node),
            Err(KvError::CasFailed { .. } | KvError::OutcomeUnknown | KvError::CasBusy) => {
                tokio::time::sleep(std::time::Duration::from_millis(5 + attempt * 2)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::Config(
        "concurrent admission did not converge; retry".into(),
    ))
}

fn cancellation_key(operation: &str) -> Vec<u8> {
    format!("/deployment/cancelled-admissions/{operation}").into_bytes()
}

async fn cancellation_complete(kv: &CrowdbKvClient, cluster: &str, node: &NodeRecord) -> Result<bool> {
    let expected =
        serde_json::to_vec(&(cluster, &node.discovery_id, &node.operation_id)).map_err(config_error)?;
    Ok(
        matches!(kv.get(0, 0, &cancellation_key(&node.operation_id), ReadMode::Linearizable, None).await?,
        GetOutcome::Found { value, .. } if value.as_ref() == expected.as_slice()),
    )
}

/// Record completion only after target preparation and every SSH entry are cleaned.
///
/// # Errors
/// Rejects replaced operations, confirmed nodes and unavailable authority.
pub async fn complete_cancellation(kv: &CrowdbKvClient, cluster: &str, node: &NodeRecord) -> Result<()> {
    let (registry, _) = read(kv)
        .await?
        .ok_or_else(|| Error::Config("cluster mapping absent".into()))?;
    if registry.cluster_id != cluster
        || !registry.nodes.iter().any(|actual| {
            actual.discovery_id == node.discovery_id
                && actual.operation_id == node.operation_id
                && actual.cancelled
                && !actual.confirmed
        })
    {
        return Err(Error::Config("cancellation operation no longer current".into()));
    }
    let value =
        serde_json::to_vec(&(cluster, &node.discovery_id, &node.operation_id)).map_err(config_error)?;
    match kv
        .put_cas(0, 0, &cancellation_key(&node.operation_id), &value, 0)
        .await
    {
        Ok(_) => Ok(()),
        Err(_) if cancellation_complete(kv, cluster, node).await? => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Retain the numeric allocation while retiring only a still-pending operation.
///
/// # Errors
/// Rejects committed nodes, changed operation identity and unavailable authority.
pub async fn cancel(
    kv: &CrowdbKvClient,
    cluster: &str,
    discovery_id: &str,
    operation_id: &str,
) -> Result<()> {
    for attempt in 0..32 {
        let (mut registry, revision) = read(kv)
            .await?
            .ok_or_else(|| Error::Config("cluster mapping absent".into()))?;
        if registry.cluster_id != cluster {
            return Err(Error::Config("cluster identity differs".into()));
        }
        let node = registry
            .nodes
            .iter_mut()
            .find(|node| node.discovery_id == discovery_id)
            .ok_or_else(|| Error::Config("admission absent".into()))?;
        if node.confirmed || node.operation_id != operation_id {
            return Err(Error::Config("admission already confirmed or replaced".into()));
        }
        node.cancelled = true;
        let bytes = serde_json::to_vec(&registry).map_err(config_error)?;
        match kv.put_cas(0, 0, NODES_KEY, &bytes, revision).await {
            Ok(_) => return Ok(()),
            Err(KvError::CasFailed { .. } | KvError::OutcomeUnknown | KvError::CasBusy) => {
                tokio::time::sleep(std::time::Duration::from_millis(5 + attempt * 2)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::Config(
        "concurrent cancellation did not converge; retry".into(),
    ))
}

/// # Errors
/// Returns an explicit authority error rather than an empty registry on failure.
pub async fn read(kv: &CrowdbKvClient) -> Result<Option<(NodeRegistry, u64)>> {
    match kv.get(0, 0, NODES_KEY, ReadMode::Linearizable, None).await? {
        GetOutcome::Found { value, revision, .. } => Ok(Some((
            serde_json::from_slice(&value).map_err(config_error)?,
            revision,
        ))),
        GetOutcome::NotFound => Ok(None),
    }
}

fn config_error(error: impl std::fmt::Display) -> Error {
    Error::Config(error.to_string())
}
