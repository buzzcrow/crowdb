// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV-cluster logical plane: store/group/replica orchestration.
//!
//! Mirrors the web handlers' fan-out + rollback logic, but calls the
//! kv-server mgmt endpoints directly via [`ServerClient`] and reads
//! topology from group-0 sysdata via [`CrowdbSysmdClient`] instead of
//! the monitor cache.

use std::collections::HashSet;

use crowdb_protocol::key::{KvGroupKey, KvReplicaKey, KvStoreKey};
use crowdb_protocol::mgmt::{
    AddGroupInitialRole, AddGroupRequest, AddStoreRequest, RemoteReplicaInfo, StepDownRequest,
};

use crate::clients::http::ServerClient;
use crate::error::{Error, Result};
use crate::ops::OpContext;

mod group_wiring;
mod publication;
mod replica_rollback;

use replica_rollback::ReplicaRollback;

/// Build a client from a node's live management registration.
async fn server_client(ctx: &OpContext, node_id: u64) -> Result<ServerClient> {
    let url = if ctx.is_test_scenario() {
        ctx.node_mgmt_url(node_id)?
    } else {
        ctx.live_node_mgmt_url(node_id).await?
    };
    ServerClient::new(&url).map_err(|e| Error::UpstreamRpc {
        node_id: url,
        status: format!("client build: {e}"),
    })
}

fn already_absent(error: &Error) -> bool {
    matches!(error, Error::UpstreamRpc { status, .. } if status.contains("HTTP 404"))
}

/// Resolve the crowdb-rpc endpoint for a store on a node by calling
/// the node's `/topology` endpoint. Returns `None` if the store is not
/// hosted on the node or has no `listen_addr`.
async fn rpc_endpoint_for_store(ctx: &OpContext, node_id: u64, store_id: u64) -> Option<String> {
    let client = server_client(ctx, node_id).await.ok()?;
    let stores = client.topology().await.ok()?;
    for s in &stores {
        if s.store_id == store_id {
            if let Some(addr) = &s.listen_addr {
                return Some(strip_scheme(&remap_zero_host(addr)));
            }
        }
    }
    None
}

fn strip_scheme(s: &str) -> String {
    s.strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .unwrap_or(s)
        .to_string()
}

fn remap_zero_host(addr: &str) -> String {
    addr.strip_prefix("0.0.0.0:")
        .map_or_else(|| addr.to_string(), |port| format!("127.0.0.1:{port}"))
}

// ── store ───────────────────────────────────────────────────────

/// Create an empty store across the listed nodes. Fans out `add_store`
/// to each node, rolls back on partial failure, and records the store
/// in group-0 sysdata.
///
/// If `nodes` is empty, picks the first node with a deployed server.
///
/// # Errors
/// Returns an error if no nodes are available or any upstream RPC fails.
pub async fn add_store(ctx: &OpContext, store_id: u64, nodes: &[u64]) -> Result<Vec<u64>> {
    if !ctx.is_test_scenario() && ctx.sysmd().get_store(store_id).await?.is_some() {
        return Err(Error::Conflict {
            kind: "store".into(),
            id: store_id.to_string(),
        });
    }
    let mut target_nodes = if nodes.is_empty() {
        let first = if ctx.is_test_scenario() {
            ctx.config().servers.iter().find_map(|server| server.node_id)
        } else {
            ctx.sysmd()
                .read_all_kv_server_instances()
                .await?
                .into_iter()
                .filter_map(|(_, instance)| instance.extra?.kv_server?.node_id)
                .min()
        }
        .ok_or_else(|| Error::Validation {
            field: "nodes".into(),
            message: "no live nodes with deployed servers".into(),
        })?;
        vec![first]
    } else {
        nodes.to_vec()
    };

    let mut seen = HashSet::new();
    target_nodes.retain(|nid| seen.insert(*nid));

    // Health-check + add_store on each node concurrently. Collect
    // successes; on any failure, roll back and return the first error.
    let results: Vec<Result<u64>> = futures::future::join_all(target_nodes.iter().map(|nid| {
        let nid = *nid;
        async move {
            let client = server_client(ctx, nid).await?;
            client.health().await.map_err(|e| Error::NodeUnreachable {
                node_id: nid.to_string(),
                reason: e.to_string(),
            })?;
            let req = AddStoreRequest { store_id, port: None };
            client
                .add_store(&req)
                .await
                .map(|_| nid)
                .map_err(|e| Error::UpstreamRpc {
                    node_id: nid.to_string(),
                    status: format!("store create failed: {e}"),
                })
        }
    }))
    .await;

    let mut succeeded: Vec<u64> = Vec::new();
    let mut first_err: Option<Error> = None;
    for res in results {
        match res {
            Ok(nid) => succeeded.push(nid),
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }
    if let Some(e) = first_err {
        // Roll back successful creations (concurrently).
        futures::future::join_all(succeeded.iter().map(|ok_nid| async move {
            if let Ok(client) = server_client(ctx, *ok_nid).await {
                let _ = client.remove_store(store_id).await;
            }
        }))
        .await;
        return Err(e);
    }

    // Record in group-0 sysdata. The sysdata write must
    // succeed — add_group reads sysdata to find the store, and a missing
    // store record causes a spurious 404.
    publication::store(ctx, store_id, &succeeded).await?;
    Ok(succeeded)
}

/// Remove a store from every hosting node. Idempotent on per-node 404.
///
/// # Errors
/// Returns [`Error::Validation`] if `store_id` is 0 (the system store).
pub async fn remove_store(ctx: &OpContext, store_id: u64) -> Result<()> {
    if store_id == 0 {
        return Err(Error::Validation {
            field: "store_id".into(),
            message: "store 0 is the system store; use cluster destroy".into(),
        });
    }
    // Find hosting nodes from group-0 sysdata.
    let store = ctx
        .sysmd()
        .get_store(store_id)
        .await?
        .ok_or_else(|| Error::NotFound {
            kind: "store".into(),
            id: store_id.to_string(),
        })?;
    let groups = ctx.sysmd().list_groups_in_store(store_id).await?;
    let replicas = ctx.sysmd().list_replicas_in_store(store_id).await?;
    let mut node_ids = store.node_ids;
    node_ids.extend(replicas.iter().map(|replica| replica.node_id));
    node_ids.sort_unstable();
    node_ids.dedup();
    for nid in &node_ids {
        let client = server_client(ctx, *nid).await?;
        if let Err(error) = client.remove_store(store_id).await {
            if !already_absent(&error) {
                return Err(error);
            }
        }
    }
    // Keep parent records until all node deletions and child cleanup succeed,
    // so a failed cleanup can be retried using the remaining authority.
    for replica in replicas {
        publication::remove(
            ctx,
            KvReplicaKey {
                store_id,
                group_id: replica.group_id,
                replica_id: replica.replica_id,
            },
        )
        .await?;
    }
    for group in groups {
        publication::remove(
            ctx,
            KvGroupKey {
                store_id,
                group_id: group.group_id,
            },
        )
        .await?;
    }
    publication::remove(ctx, KvStoreKey { store_id }).await?;
    Ok(())
}

/// List stores from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn list_stores(ctx: &OpContext) -> Result<Vec<crowdb_protocol::common::StoreValue>> {
    ctx.sysmd().list_stores().await.map_err(Into::into)
}

// ── group ───────────────────────────────────────────────────────

/// Create a Paxos group across the listed nodes. Creates a local
/// `PxGroup` on each node, wires remote-replica entries, and records
/// the group in group-0 sysdata. Rolls back on
/// partial failure.
///
/// # Errors
/// Returns an error if `nodes` is empty or any upstream RPC fails.
#[allow(clippy::too_many_lines)]
pub async fn add_group(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    nodes: &[u64],
) -> Result<()> {
    if nodes.is_empty() {
        return Err(Error::Validation {
            field: "nodes".into(),
            message: "nodes list must not be empty".into(),
        });
    }
    if !ctx.is_test_scenario()
        && (ctx.sysmd().get_group(store_id, group_id).await?.is_some()
            || !ctx
                .sysmd()
                .list_replicas_in_group(store_id, group_id)
                .await?
                .is_empty())
    {
        return Err(Error::Conflict {
            kind: "group".into(),
            id: format!("{store_id}/{group_id}"),
        });
    }

    // Phase 1: create the group on each node concurrently.
    let results: Vec<Result<(u64, u64)>> =
        futures::future::join_all(nodes.iter().enumerate().map(|(i, nid)| {
            let nid = *nid;
            let rid = replica_id + i as u64;
            let single = nodes.len() <= 1;
            async move {
                let client = server_client(ctx, nid).await?;
                let req = AddGroupRequest {
                    group_id,
                    replica_id: rid,
                    initial_role: Some(if i == 0 {
                        AddGroupInitialRole::Leader
                    } else {
                        AddGroupInitialRole::Follower
                    }),
                    start_election: Some(single),
                };
                client
                    .add_group(store_id, &req)
                    .await
                    .map(|()| (nid, rid))
                    .map_err(|e| Error::UpstreamRpc {
                        node_id: nid.to_string(),
                        status: format!("group create failed: {e}"),
                    })
            }
        }))
        .await;

    let mut succeeded: Vec<(u64, u64)> = Vec::new();
    let mut first_err: Option<Error> = None;
    for res in results {
        match res {
            Ok(entry) => succeeded.push(entry),
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }
    if let Some(e) = first_err {
        return Err(group_wiring::rollback(ctx, store_id, group_id, &succeeded, e).await);
    }

    // Every peer must be wired before membership becomes authoritative.
    if let Err(error) = group_wiring::wire(ctx, store_id, group_id, &succeeded).await {
        return Err(group_wiring::rollback(ctx, store_id, group_id, &succeeded, error).await);
    }

    // Record in group-0 sysdata. The sysdata write must
    // succeed — add_replica reads sysdata to find existing replicas, and
    // a missing group record causes a spurious 404.
    publication::group(ctx, store_id, group_id, &succeeded).await?;
    Ok(())
}

/// Remove a Paxos group from every hosting node.
///
/// # Errors
/// Returns [`Error::Validation`] if removing group 0 in store 0.
pub async fn remove_group(ctx: &OpContext, store_id: u64, group_id: u64) -> Result<()> {
    if store_id == 0 && group_id == 0 {
        return Err(Error::Validation {
            field: "group_id".into(),
            message: "group 0 in store 0 is the system group; use cluster destroy".into(),
        });
    }
    // Find hosting nodes from group-0 sysdata.
    if ctx.sysmd().get_group(store_id, group_id).await?.is_none() {
        return Err(Error::NotFound {
            kind: "group".into(),
            id: format!("{store_id}/{group_id}"),
        });
    }
    let replicas = ctx.sysmd().list_replicas_in_group(store_id, group_id).await?;
    let node_ids: Vec<u64> = replicas.iter().map(|r| r.node_id).collect();
    for nid in &node_ids {
        let client = server_client(ctx, *nid).await?;
        if let Err(error) = client.remove_group(store_id, group_id).await {
            if !already_absent(&error) {
                return Err(error);
            }
        }
    }
    for replica in replicas {
        publication::remove(
            ctx,
            KvReplicaKey {
                store_id,
                group_id,
                replica_id: replica.replica_id,
            },
        )
        .await?;
    }
    publication::remove(ctx, KvGroupKey { store_id, group_id }).await?;
    Ok(())
}

/// List groups in a store from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn list_groups(ctx: &OpContext, store_id: u64) -> Result<Vec<crowdb_protocol::common::GroupValue>> {
    ctx.sysmd()
        .list_groups_in_store(store_id)
        .await
        .map_err(Into::into)
}

// ── replica ─────────────────────────────────────────────────────

async fn ensure_replica_store(ctx: &OpContext, store_id: u64, node_id: u64) -> Result<(ServerClient, bool)> {
    let client = server_client(ctx, node_id).await?;

    // Check if the target node already hosts this store via sysdata.
    let target_has_store = ctx
        .sysmd()
        .get_store(store_id)
        .await?
        .ok_or_else(|| Error::NotFound {
            kind: "store".into(),
            id: store_id.to_string(),
        })?
        .node_ids
        .contains(&node_id);

    let created_store_on_target = if target_has_store {
        false
    } else {
        // Create the store on the target node. If it already exists
        // (race with another concurrent add_replica), the 409 is
        // safe to ignore — the store is there either way.
        let store_req = AddStoreRequest { store_id, port: None };
        match client.add_store(&store_req).await {
            Ok(_) => true,
            Err(e) if e.to_string().contains("409") => false,
            Err(e) => {
                return Err(Error::UpstreamRpc {
                    node_id: node_id.to_string(),
                    status: format!("create local store: {e}"),
                });
            }
        }
    };
    Ok((client, created_store_on_target))
}

/// Add a replica to an existing group on a target node. Creates a local
/// `PxGroup` on the target, registers the new replica as a remote on
/// every existing peer, and registers every peer as a remote on the new
/// replica. Rolls back on partial failure.
///
/// # Errors
/// Returns an error if the group or node is not found, or any RPC fails.
#[allow(clippy::too_many_lines)]
pub async fn add_replica(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    node_id: u64,
    replica_id: Option<u64>,
) -> Result<u64> {
    // Resolve existing replicas from group-0 sysdata.
    let existing = ctx.sysmd().list_replicas_in_group(store_id, group_id).await?;
    if existing.is_empty() {
        return Err(Error::NotFound {
            kind: "group".into(),
            id: format!("{store_id}/{group_id}"),
        });
    }
    if existing.iter().any(|replica| replica.node_id == node_id) {
        return Err(Error::Conflict {
            kind: "replica on node".into(),
            id: node_id.to_string(),
        });
    }
    let new_rid = match replica_id {
        Some(id) => id,
        None => existing
            .iter()
            .map(|r| r.replica_id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Validation {
                field: "replica_id".into(),
                message: "replica identity space is exhausted".into(),
            })?,
    };
    if existing.iter().any(|r| r.replica_id == new_rid) {
        return Err(Error::Conflict {
            kind: "replica".into(),
            id: new_rid.to_string(),
        });
    }

    // Resolve every existing peer before changing the target's local state.
    let members: Vec<_> = existing.iter().map(|r| (r.node_id, r.replica_id)).collect();
    let peers = group_wiring::resolve(ctx, store_id, &members).await?;

    let (client, created_store_on_target) = ensure_replica_store(ctx, store_id, node_id).await?;

    let mut rollback = ReplicaRollback {
        ctx,
        store_id,
        group_id,
        replica_id: new_rid,
        target_node: node_id,
        remove_store: created_store_on_target,
        wired_peers: Vec::new(),
    };
    let req = AddGroupRequest {
        group_id,
        replica_id: new_rid,
        initial_role: Some(AddGroupInitialRole::Follower),
        start_election: Some(false),
    };
    if let Err(error) = client.add_group(store_id, &req).await {
        let original = Error::UpstreamRpc {
            node_id: node_id.to_string(),
            status: format!("create local group: {error}"),
        };
        // Only remove a store created by this operation. A failed request on
        // a pre-existing store may refer to a group we do not own.
        return Err(if created_store_on_target {
            rollback.fail(original).await
        } else {
            original
        });
    }

    // Step 2: Register the new replica as a remote on every existing peer.
    let Some(new_endpoint) = rpc_endpoint_for_store(ctx, node_id, store_id).await else {
        return Err(rollback
            .fail(Error::NodeUnreachable {
                node_id: node_id.to_string(),
                reason: "could not determine crowdb-rpc endpoint".into(),
            })
            .await);
    };
    let new_remote = RemoteReplicaInfo {
        replica_id: new_rid,
        endpoint: new_endpoint,
        voting: true,
    };
    for (existing_replica, (peer_client, _)) in existing.iter().zip(&peers) {
        // A lost response may still have applied the remote on this peer.
        rollback.wired_peers.push(existing_replica.node_id);
        if let Err(e) = peer_client
            .add_remote_replicas(store_id, group_id, std::slice::from_ref(&new_remote))
            .await
        {
            return Err(rollback
                .fail(Error::UpstreamRpc {
                    node_id: existing_replica.node_id.to_string(),
                    status: format!("wire new replica on peer: {e}"),
                })
                .await);
        }
    }

    // Step 3: Register every existing peer as a remote on the new replica.
    let existing_remotes: Vec<RemoteReplicaInfo> = existing
        .iter()
        .zip(&peers)
        .map(|(replica, (_, endpoint))| RemoteReplicaInfo {
            replica_id: replica.replica_id,
            endpoint: endpoint.clone(),
            voting: true,
        })
        .collect();
    if !existing_remotes.is_empty() {
        if let Err(e) = client
            .add_remote_replicas(store_id, group_id, &existing_remotes)
            .await
        {
            return Err(rollback
                .fail(Error::UpstreamRpc {
                    node_id: node_id.to_string(),
                    status: format!("wire existing peers on new replica: {e}"),
                })
                .await);
        }
    }

    // Record only after every existing peer and the new replica are wired.
    record_replica(ctx, store_id, group_id, new_rid, node_id).await?;
    Ok(new_rid)
}

/// Record a new replica in group-0 sysdata.
async fn record_replica(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    node_id: u64,
) -> Result<()> {
    let value = publication::replica_value(store_id, group_id, replica_id, node_id);
    publication::replica(ctx, &value).await?;
    Ok(())
}

/// Remove a replica: deregister from peers, delete local group, step
/// down if it was the leader.
///
/// # Errors
/// Returns [`Error::NotFound`] if the replica does not exist.
pub async fn remove_replica(ctx: &OpContext, store_id: u64, group_id: u64, replica_id: u64) -> Result<()> {
    let replicas = ctx.sysmd().list_replicas_in_group(store_id, group_id).await?;
    let target = replicas
        .iter()
        .find(|r| r.replica_id == replica_id)
        .ok_or_else(|| Error::NotFound {
            kind: "replica".into(),
            id: replica_id.to_string(),
        })?;
    let target_node = target.node_id;

    // Step 0: step down if this replica is the leader (best-effort).
    if let Ok(client) = server_client(ctx, target_node).await {
        let _ = client
            .step_down(
                store_id,
                group_id,
                &StepDownRequest {
                    reason: format!("replica {replica_id} removal"),
                },
            )
            .await;
    }

    // Step 1: Deregister from every peer.
    for peer in &replicas {
        if peer.replica_id == replica_id {
            continue;
        }
        let client = server_client(ctx, peer.node_id).await?;
        if let Err(error) = client.remove_remote_replica(store_id, group_id, replica_id).await {
            if !already_absent(&error) {
                return Err(error);
            }
        }
    }

    // Step 2: Delete the local group on the target node.
    let client = server_client(ctx, target_node).await?;
    if let Err(error) = client.remove_group(store_id, group_id).await {
        if !already_absent(&error) {
            return Err(error);
        }
    }

    publication::remove(
        ctx,
        KvReplicaKey {
            store_id,
            group_id,
            replica_id,
        },
    )
    .await?;
    Ok(())
}

/// List replicas in a group from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn list_replicas(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
) -> Result<Vec<crowdb_protocol::common::ReplicaValue>> {
    ctx.sysmd()
        .list_replicas_in_group(store_id, group_id)
        .await
        .map_err(Into::into)
}
