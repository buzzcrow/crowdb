// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology restore: startup three-way fallback + per-node restore.

use crate::mgmt::{
    build_server_client, mgmt_url_for_node, refresh_node_cache, rpc_endpoint_for_node, rpc_is_conflict,
    rpc_is_not_found,
};
use crate::state::AppState;
use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::cluster::NodeId;
use crowdb_console_shared::config::GroupEntry;
use crowdb_console_shared::mgmt::{AddGroupInitialRole, AddGroupRequest, AddStoreRequest};
use tracing::{info, warn};

/// Result of the three-way group 0 state check at console startup.
enum Group0State {
    /// No nodes deployed yet — first-run scenario.
    NoNodes,
    /// Group 0 not found on any reachable node — phase 1 (TOML mode).
    Missing,
    /// Group 0 exists — group 0 authoritative.
    Ready,
}

/// Check group 0 state across all deployed nodes to determine the
/// topology source at console startup.
async fn check_group0_state(state: &AppState) -> Group0State {
    let node_ids: Vec<NodeId> = {
        let cfg = state.config.read().unwrap();
        cfg.servers.iter().filter_map(|s| s.node_id).collect()
    };
    if node_ids.is_empty() {
        return Group0State::NoNodes;
    }
    for nid in &node_ids {
        let Ok(url) = mgmt_url_for_node(state, *nid) else {
            continue;
        };
        let Ok(client) = build_server_client(url) else {
            continue;
        };
        // Check if group 0 exists by listing stores.
        if let Ok(stores) = client.list_stores().await {
            if stores.iter().any(|s| s.store_id == 0) {
                return Group0State::Ready;
            }
        }
    }
    Group0State::Missing
}

/// Console startup three-way fallback. Checks group 0 state and picks
/// the right topology source before restoring.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn startup_topology_check(state: &AppState) {
    match check_group0_state(state).await {
        Group0State::NoNodes => {
            info!("no nodes deployed; first-run scenario, skipping topology restore");
        }
        Group0State::Missing => {
            warn!("group 0 could not be confirmed; local topology restore is forbidden");
        }
        Group0State::Ready => {
            info!("group 0 is ready; local topology restore is skipped");
        }
    }
}

/// Restores persisted topology (stores and groups) for a specific node.
///
/// This function ensures that all stores and groups configured for the given node
/// are properly set up on the node after a restart.
///
/// # Panics
/// Panics if the config read lock is poisoned (should not happen in normal operation).
///
/// # Errors
/// Returns an error if store or group restoration fails.
pub(crate) async fn restore_persisted_topology_for_node(
    state: &AppState,
    node_id: NodeId,
) -> Result<(), String> {
    let (stores, groups) = {
        let cfg = state.config.read().unwrap();
        (cfg.stores.clone(), cfg.groups.clone())
    };

    for store in stores
        .iter()
        .filter(|store| store.nodes.iter().any(|id| id == &node_id))
    {
        ensure_store_on_node(state, node_id, store.store_id).await?;
    }

    for group in groups
        .iter()
        .filter(|group| group.replicas.iter().any(|replica| replica.node_id == node_id))
    {
        let Some(local_replica) = group.replicas.iter().find(|replica| replica.node_id == node_id) else {
            continue;
        };
        ensure_group_local(
            state,
            node_id,
            group.store_id,
            group.group_id,
            local_replica.replica_id,
            AddGroupInitialRole::Follower,
            // Defer for multi-replica groups until remotes are wired.
            Some(group.replicas.len() <= 1),
        )
        .await?;
        if let Err(err) = ensure_group_remotes(state, group).await {
            warn!(
                store_id = group.store_id,
                group_id = group.group_id,
                node_id,
                error = %err,
                "failed to restore group remotes for restarted node"
            );
        }
    }

    refresh_node_cache(state, node_id).await;
    Ok(())
}

async fn ensure_store_on_node(state: &AppState, node_id: NodeId, store_id: u64) -> Result<(), String> {
    let url = mgmt_url_for_node(state, node_id).map_err(|(_, body)| body.0.error.clone())?;
    let client = ServerClient::new(url).map_err(|e| e.to_string())?;
    match client.get_store(store_id).await {
        Ok(_) => Ok(()),
        Err(err) if rpc_is_not_found(&err) => {
            client
                .add_store(&AddStoreRequest { store_id, port: None })
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) => Err(err.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn ensure_group_local(
    state: &AppState,
    node_id: NodeId,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    initial_role: AddGroupInitialRole,
    // `Some(false)` for multi-replica groups so the server does not self-elect
    // at `quorum == 1` before remotes are wired; the
    // following `ensure_group_remotes` rebuild starts the driver with a correct
    // quorum. `None`/`Some(true)` for single-replica groups (no remote-wiring
    // step to start the driver).
    start_election: Option<bool>,
) -> Result<(), String> {
    let url = mgmt_url_for_node(state, node_id).map_err(|(_, body)| body.0.error.clone())?;
    let client = ServerClient::new(url).map_err(|e| e.to_string())?;
    match client.list_groups(store_id).await {
        Ok(groups)
            if groups
                .iter()
                .any(|g| g.group_id == group_id && g.local_replica_id == replica_id) =>
        {
            Ok(())
        }
        Ok(_) => {
            client
                .add_group(
                    store_id,
                    &AddGroupRequest {
                        group_id,
                        replica_id,
                        initial_role: Some(initial_role),
                        start_election,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) if rpc_is_not_found(&err) => {
            ensure_store_on_node(state, node_id, store_id).await?;
            client
                .add_group(
                    store_id,
                    &AddGroupRequest {
                        group_id,
                        replica_id,
                        initial_role: Some(initial_role),
                        start_election,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, node_id).await;
            Ok(())
        }
        Err(err) if rpc_is_conflict(&err) => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

async fn ensure_group_remotes(state: &AppState, group: &GroupEntry) -> Result<(), String> {
    for replica in &group.replicas {
        refresh_node_cache(state, replica.node_id).await;
    }
    for replica in &group.replicas {
        let url = mgmt_url_for_node(state, replica.node_id).map_err(|(_, body)| body.0.error.clone())?;
        let client = ServerClient::new(url).map_err(|e| e.to_string())?;
        let existing = client
            .list_remote_replicas(group.store_id, group.group_id)
            .await
            .map_err(|e| e.to_string())?;
        let mut to_update = Vec::new();
        for peer in &group.replicas {
            if peer.replica_id == replica.replica_id {
                continue;
            }
            let Some(current_endpoint) = rpc_endpoint_for_node(state, peer.node_id, group.store_id).await
            else {
                // Peer's store is not up yet; skip rather than overwriting
                // the correct persisted-config endpoint with a stale one.
                continue;
            };
            let existing_entry = existing.iter().find(|r| r.replica_id == peer.replica_id);
            let needs_update = match existing_entry {
                None => true,
                Some(r) => r.endpoint != current_endpoint,
            };
            if needs_update {
                to_update.push(crowdb_console_shared::mgmt::RemoteReplicaInfo {
                    replica_id: peer.replica_id,
                    endpoint: current_endpoint,
                    voting: true,
                });
            }
        }
        if !to_update.is_empty() {
            client
                .add_remote_replicas(group.store_id, group.group_id, &to_update)
                .await
                .map_err(|e| e.to_string())?;
            refresh_node_cache(state, replica.node_id).await;
        }
    }
    Ok(())
}
