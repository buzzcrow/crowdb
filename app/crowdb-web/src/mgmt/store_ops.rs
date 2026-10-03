// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A5: Logical store plane — writes delegate to `ops::kv_logical`,
//! reads Group 0 topology with live leader hints from the monitor cache.

use crate::error::{err_502, map_config_err, ErrorBody};
use crate::expand::Recursive;
use crate::mgmt::{cluster_initialized, refresh_node_cache};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{GroupSummary, NodeId, StoreView};
use crowdb_console_shared::ops;
use crowdb_protocol::common::{GroupValue, ReplicaValue, StoreValue};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

/// `GET /api/stores`. List Group 0 stores with runtime leader hints.
///
/// # Errors
/// Returns `502` when Group 0 is unavailable.
pub(crate) async fn http_list_stores(
    State(state): State<AppState>,
    Recursive(_depth): Recursive,
) -> Result<Json<Vec<StoreView>>, (StatusCode, Json<ErrorBody>)> {
    let (has_servers, initialized) = {
        let config = state.config.read().unwrap();
        (
            config
                .servers
                .iter()
                .any(|server| server.service_type == crowdb_console_shared::config::ServiceType::Kv),
            config.group(0, 0).is_some(),
        )
    };
    if !state.managed_mode
        && !initialized
        && (!has_servers || !cluster_initialized(&state).await)
        && state
            .monitor_cache
            .snapshot()
            .await
            .values()
            .all(|node| node.stores.is_empty())
    {
        return Ok(Json(Vec::new()));
    }
    let ctx = state
        .op_context()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let (stores, groups, replicas) = tokio::try_join!(
        ctx.sysmd().list_stores(),
        ctx.sysmd().list_all_groups(),
        ctx.sysmd().list_all_replicas()
    )
    .map_err(|error| err_502(format!("Group 0 topology lookup failed: {error}")))?;
    let mut groups_by_store = BTreeMap::<u64, Vec<GroupValue>>::new();
    let mut replicas_by_store = BTreeMap::<u64, Vec<ReplicaValue>>::new();
    for group in groups {
        groups_by_store.entry(group.store_id).or_default().push(group);
    }
    for replica in replicas {
        replicas_by_store
            .entry(replica.store_id)
            .or_default()
            .push(replica);
    }
    let mut views = Vec::with_capacity(stores.len());
    for store in stores {
        let groups = groups_by_store.remove(&store.store_id).unwrap_or_default();
        let replicas = replicas_by_store.remove(&store.store_id).unwrap_or_default();
        views.push(project_store(&state, store, groups, replicas).await);
    }
    views.sort_by_key(|store| store.store_id);
    Ok(Json(views))
}

async fn project_store(
    state: &AppState,
    store: StoreValue,
    groups: Vec<GroupValue>,
    replicas: Vec<ReplicaValue>,
) -> StoreView {
    let mut replicas_by_group = HashMap::<u64, Vec<ReplicaValue>>::new();
    for replica in replicas {
        replicas_by_group
            .entry(replica.group_id)
            .or_default()
            .push(replica);
    }
    let mut summaries = Vec::with_capacity(groups.len());
    for group in groups {
        let members = replicas_by_group.remove(&group.group_id).unwrap_or_default();
        let leader = state
            .monitor_cache
            .resolve_group(store.store_id, group.group_id)
            .await
            .and_then(|view| {
                view.leader().and_then(|leader| {
                    members
                        .iter()
                        .any(|member| {
                            member.replica_id == leader.replica_id && member.node_id == leader.node_id
                        })
                        .then_some(leader.replica_id)
                })
            });
        summaries.push(GroupSummary {
            group_id: group.group_id,
            replica_count: members.len(),
            leader,
        });
    }
    summaries.sort_by_key(|group| group.group_id);
    let mut nodes = store.node_ids;
    nodes.sort_unstable();
    StoreView {
        store_id: store.store_id,
        name: None,
        nodes,
        groups: summaries,
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateStoreBody {
    pub store_id: u64,
    #[serde(default)]
    pub nodes: Vec<NodeId>,
}

/// `POST /api/stores`. Create an empty store across the listed nodes
/// (or the first node with a running server if `nodes` is empty).
/// Delegates to `ops::kv_logical::add_store` which handles fan-out +
/// rollback + sysdata recording.
///
/// # Errors
/// Returns `409` if the cluster is not initialized (non-zero store),
/// `502` if no nodes are available or any upstream RPC fails,
/// `500` if config persistence fails.
pub(crate) async fn http_add_store(
    State(state): State<AppState>,
    Json(body): Json<CreateStoreBody>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorBody>)> {
    if body.store_id != 0 && !cluster_initialized(&state).await {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "cluster not initialized; call POST /api/cluster/init first".into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let succeeded = ops::kv_logical::add_store(&ctx, body.store_id, &body.nodes)
        .await
        .map_err(map_config_err)?;

    // Refresh the monitor cache for affected nodes so health badges
    // and RPC endpoint resolution reflect the new store.
    futures::future::join_all(succeeded.iter().map(|&nid| refresh_node_cache(&state, nid))).await;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "store_id": body.store_id, "nodes": succeeded })),
    ))
}

/// `GET /api/stores/:store_id`. Store view from Group 0, with runtime hints.
///
/// # Errors
/// Returns `404` if the store is not found, or `502` if Group 0 is unavailable.
pub(crate) async fn http_get_store(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<StoreView>, (StatusCode, Json<ErrorBody>)> {
    store_view(&state, sid).await.map(Json)
}

pub(super) async fn store_view(
    state: &AppState,
    sid: u64,
) -> Result<StoreView, (StatusCode, Json<ErrorBody>)> {
    let ctx = state
        .op_context()
        .await
        .map_err(|error| err_502(error.to_string()))?;
    let store = ctx
        .sysmd()
        .get_store(sid)
        .await
        .map_err(|error| err_502(format!("Group 0 store lookup failed: {error}")))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("store {sid} not found"),
                }),
            )
        })?;
    let (groups, replicas) = tokio::try_join!(
        ctx.sysmd().list_groups_in_store(sid),
        ctx.sysmd().list_replicas_in_store(sid)
    )
    .map_err(|error| err_502(format!("Group 0 store topology lookup failed: {error}")))?;
    Ok(project_store(state, store, groups, replicas).await)
}

/// `DELETE /api/stores/:store_id`. Delete the store across every hosting
/// node. Delegates to `ops::kv_logical::remove_store` which handles
/// fan-out + sysdata cleanup + config update.
///
/// # Errors
/// Returns `409` if `store_id` is 0 (the system store),
/// `500` if config persistence fails.
pub(crate) async fn http_remove_store(
    State(state): State<AppState>,
    Path(sid): Path<u64>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if sid == 0 {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorBody {
                error: "store 0 is the system store; destroy and recreate the cluster to remove it".into(),
            }),
        ));
    }
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    // Resolve hosting nodes from group-0 sysdata before removal
    // so we can refresh their caches afterwards.
    let hosting_nodes = ctx
        .sysmd()
        .get_store(sid)
        .await
        .map_err(|e| map_config_err(e.into()))?
        .map(|s| s.node_ids)
        .unwrap_or_default();
    ops::kv_logical::remove_store(&ctx, sid)
        .await
        .map_err(map_config_err)?;

    futures::future::join_all(hosting_nodes.iter().map(|&nid| refresh_node_cache(&state, nid))).await;
    Ok(StatusCode::NO_CONTENT)
}
