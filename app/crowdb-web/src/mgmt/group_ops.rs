// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A6: Logical group plane — writes delegate to `ops::kv_logical`,
//! reads Group 0 topology with live role/leader overlays.

use crate::error::{err_502, map_config_err, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::{cluster_initialized, refresh_node_cache};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::cluster::{
    GroupHealth, GroupSummary, GroupView, NodeGroup, NodeId, ReplicaRole, ReplicaState, ReplicaView,
};
use crowdb_console_shared::monitor::legacy_topology_to_node_stores;
use crowdb_console_shared::ops;
use crowdb_protocol::common::ReplicaValue;
use serde::Deserialize;
use std::collections::HashMap;

/// `GET /api/stores/:store_id/groups`. List Group 0 groups.
///
/// # Errors
/// Returns `404` if the store is not found, or `502` if Group 0 is unavailable.
pub(crate) async fn http_list_groups(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorBody>)> {
    let view = super::store_ops::store_view(&state, sid).await?;
    Ok(Json(view.groups))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateGroupBody {
    pub group_id: u64,
    pub replica_id: u64,
    pub nodes: Vec<NodeId>,
}

/// `POST /api/stores/:store_id/groups`. Create a group across the listed
/// nodes. Delegates to `ops::kv_logical::add_group` which handles
/// local group creation, remote wiring, sysdata recording, and rollback.
///
/// # Errors
/// Returns `409` if the cluster is not initialized (non-zero store),
/// `502` if any upstream RPC fails, `500` if config persistence fails.
pub(crate) async fn http_add_group(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Json(body): Json<CreateGroupBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if sid != 0 && !cluster_initialized(&state).await {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "cluster not initialized; call POST /api/cluster/init first".into(),
            }),
        ));
    }
    // Ensure the group-0 leader is available before the sysdata write
    // inside add_group. Right after a resetAll + cluster init, the
    // election may still be in progress and op_context would seed a
    // stale/non-leader endpoint, causing a 5s retry cycle.
    if sid != 0 {
        state.refresh_group0_leader(None).await;
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    ops::kv_logical::add_group(&ctx, sid, body.group_id, body.replica_id, &body.nodes)
        .await
        .map_err(map_config_err)?;

    // Refresh the monitor cache for all target nodes so health badges
    // and RPC endpoint resolution reflect the new group.
    futures::future::join_all(body.nodes.iter().map(|&nid| refresh_node_cache(&state, nid))).await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "store_id": sid,
            "group_id": body.group_id,
            "nodes": body.nodes,
        })),
    ))
}

/// `GET /api/stores/:store_id/groups/:group_id`. Group 0 membership with
/// observed per-replica runtime state.
///
/// # Errors
/// Returns `404` if the group is not found, or `502` if Group 0 is unavailable.
pub(crate) async fn http_get_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Recursive(_depth): Recursive,
) -> Result<Json<GroupView>, (StatusCode, Json<ErrorBody>)> {
    group_view(&state, sid, gid).await.map(Json)
}

pub(super) async fn group_view(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<GroupView, (StatusCode, Json<ErrorBody>)> {
    let ctx = state
        .op_context()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let group = ctx
        .sysmd()
        .get_group(sid, gid)
        .await
        .map_err(|error| err_502(format!("Group 0 group lookup failed: {error}")))?;
    if group.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found"),
            }),
        ));
    }
    let members = ctx
        .sysmd()
        .list_replicas_in_group(sid, gid)
        .await
        .map_err(|error| err_502(format!("Group 0 replica lookup failed: {error}")))?;
    let membership = ctx
        .membership()
        .read(sid, gid)
        .await
        .map_err(|error| err_502(format!("Group 0 membership authority lookup failed: {error}")))?;
    let instances = ctx
        .sysmd()
        .read_all_kv_server_instances()
        .await
        .map_err(|error| err_502(format!("Group 0 service lookup failed: {error}")))?;
    let mut registered = HashMap::<NodeId, Vec<String>>::new();
    for (_, instance) in instances {
        if let Some(node_id) = instance
            .extra
            .as_ref()
            .and_then(|extra| extra.kv_server.as_ref())
            .and_then(|extra| extra.node_id)
        {
            registered.entry(node_id).or_default().push(instance.rpc_endpoint);
        }
    }
    let reports = observe_replicas(sid, gid, &members, &registered).await;
    Ok(project_group(sid, gid, members, reports, membership.as_ref()))
}

async fn observe_replicas(
    sid: u64,
    gid: u64,
    members: &[ReplicaValue],
    registered: &HashMap<NodeId, Vec<String>>,
) -> Vec<Option<NodeGroup>> {
    futures::future::join_all(members.iter().map(|member| async {
        let endpoint = registered
            .get(&member.node_id)
            .filter(|endpoints| endpoints.len() == 1)?;
        let client = ServerClient::new(&endpoint[0]).ok()?;
        let stores = client.topology().await.ok()?;
        let stores = legacy_topology_to_node_stores(member.node_id, &stores);
        stores
            .get(&sid)?
            .groups
            .iter()
            .find(|group| group.group_id == gid && group.local.replica_id == member.replica_id)
            .cloned()
    }))
    .await
}

fn project_group(
    sid: u64,
    gid: u64,
    members: Vec<ReplicaValue>,
    reports: Vec<Option<NodeGroup>>,
    membership: Option<&crowdb_kv_client::GroupMembershipSnapshot>,
) -> GroupView {
    let mut replicas = Vec::with_capacity(members.len());
    let mut observed = 0usize;
    let mut read_state = None;
    let mut has_leader = false;
    for (member, local) in members.into_iter().zip(reports) {
        let replica = if let Some(local) = local {
            observed += 1;
            if local.local.role == ReplicaRole::Leader {
                has_leader = true;
                read_state = local.read_state;
            }
            ReplicaView {
                replica_id: member.replica_id,
                node_id: member.node_id,
                role: local.local.role,
                state: local.local.state,
                engine_healthy: local.local.engine_healthy,
                crowtree_stats: local.local.crowtree_stats,
                election: local.local.election,
            }
        } else {
            ReplicaView {
                replica_id: member.replica_id,
                node_id: member.node_id,
                role: ReplicaRole::Unknown,
                state: ReplicaState::Unknown,
                engine_healthy: false,
                crowtree_stats: None,
                election: None,
            }
        };
        replicas.push(replica);
    }
    let total = replicas.len();
    let state = if observed == 0 {
        GroupHealth::Unknown
    } else if !has_leader || observed < total / 2 + 1 {
        GroupHealth::Unavailable
    } else if observed == total {
        GroupHealth::Healthy
    } else {
        GroupHealth::Degraded
    };
    GroupView {
        store_id: sid,
        group_id: gid,
        replicas,
        state,
        read_state,
        membership_epoch: membership.as_ref().map(|value| value.record().epoch),
        membership_state: membership
            .as_ref()
            .map(|value| match &value.record().installation {
                crowdb_protocol::kv_membership::GroupMembershipState::Installing { .. } => {
                    "installing".to_string()
                }
                crowdb_protocol::kv_membership::GroupMembershipState::Ready => "ready".to_string(),
            }),
    }
}

/// `DELETE /api/stores/:store_id/groups/:group_id`. Delete the group
/// across every hosting node. Delegates to `ops::kv_logical::remove_group`
/// which handles fan-out + sysdata cleanup + config update.
///
/// # Errors
/// Returns `409` if removing group 0 in store 0,
/// `500` if config persistence fails.
pub(crate) async fn http_remove_group(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 && gid == 0 {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error:
                    "group 0 in store 0 is the system group; destroy and recreate the cluster to remove it"
                        .into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    // Resolve hosting nodes from group-0 sysdata before removal
    // so we can refresh their caches afterwards.
    let hosting_nodes: Vec<NodeId> = ctx
        .sysmd()
        .list_replicas_in_group(sid, gid)
        .await
        .map_err(|e| map_config_err(e.into()))?
        .iter()
        .map(|r| r.node_id)
        .collect();
    ops::kv_logical::remove_group(&ctx, sid, gid)
        .await
        .map_err(map_config_err)?;

    futures::future::join_all(hosting_nodes.iter().map(|&nid| refresh_node_cache(&state, nid))).await;
    Ok(StatusCode::NO_CONTENT)
}
