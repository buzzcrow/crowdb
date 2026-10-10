// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cancellation-safe KV deployment and restart.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crowdb_console_shared::{
    cluster::{NodeHealth, NodeId},
    config::ServerEntry,
    ops,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{err_500, err_502, map_config_err, map_persist_err, ErrorBody},
    expand::Recursive,
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct DeployNodeServerBody {
    rest_port: u16,
    rpc_port: u16,
    #[serde(default)]
    binary: Option<String>,
    #[serde(default)]
    election_profile: Option<String>,
    /// `--kv-backend` value (e.g. `"file"`, `"block"`, `"mem-block"`).
    #[serde(default)]
    kv_backend: Option<String>,
    /// `--wal-backend` value (e.g. `"file"`, `"mem-block"`, `"block-device"`).
    #[serde(default)]
    wal_backend: Option<String>,
    /// Sets `--no-fsync` on the spawned server when `true`.
    #[serde(default)]
    no_fsync: bool,
    /// `--metrics-interval` value in seconds.
    #[serde(default)]
    metrics_interval: Option<u64>,
    /// `--max-inflight` value for the proposal admission window.
    #[serde(default)]
    max_inflight: Option<usize>,
    /// `--coalesce-max-keys` value for R45 proposal coalescing.
    #[serde(default)]
    coalesce_max_keys: Option<usize>,
    /// `--peer-pool-size` value for inter-server RPC connection pool.
    #[serde(default)]
    peer_pool_size: Option<usize>,
    /// `--enable-nagle` flag for RPC connections.
    #[serde(default)]
    enable_nagle: Option<bool>,
    /// `--quickack` flag for RPC connections (Linux only).
    #[serde(default)]
    quickack: Option<bool>,
    /// `--event-write` flag for RPC transports.
    #[serde(default)]
    event_write: Option<bool>,
    /// `--send-queue-capacity` value for per-connection send queue.
    #[serde(default)]
    send_queue_capacity: Option<u32>,
    /// Optional `--config` JSON path passed to the spawned `crowdb-kv-server`.
    #[serde(default)]
    config: Option<String>,
    /// `--rpc-workers` value for the spawned `crowdb-kv-server`.
    #[serde(default)]
    rpc_workers: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct DeployResult {
    node_id: NodeId,
    mgmt_url: String,
    rpc_url: String,
    pid: u32,
}

/// `GET /api/nodes/:node_id/server`. Runtime info; 404 if not deployed.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is deployed on this node.
pub async fn http_get_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Recursive(_depth): Recursive,
) -> Result<Json<ServerEntry>, (StatusCode, Json<ErrorBody>)> {
    let mut entry = {
        let cfg = state.config.read().unwrap();
        cfg.server_for_node(node_id).cloned().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorBody {
                    error: format!("no server deployed on node {node_id}"),
                }),
            )
        })?
    };
    entry.pid = state.runtime_pid(node_id);
    Ok(Json(entry))
}

/// `POST /api/nodes/:node_id/server/deploy`. Spawn `crowdb-kv-server` on
/// the node (local fork for `ssh_user=""`, SSH otherwise), wait for
/// health, persist the deployment record.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns an error if deployment, config persistence, or node lookup fails.
pub async fn http_deploy_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    Json(body): Json<DeployNodeServerBody>,
) -> Result<(StatusCode, Json<DeployResult>), (StatusCode, Json<ErrorBody>)> {
    let operation = crate::services::Operation::claim(&state, vec![format!("node/{node_id}")])?;
    tokio::spawn(async move {
        let _operation = operation;
        deploy_node_server(state, node_id, body).await
    })
    .await
    .map_err(|error| err_500(format!("KV lifecycle task failed: {error}")))?
}

async fn deploy_node_server(
    state: AppState,
    node_id: u64,
    body: DeployNodeServerBody,
) -> Result<(StatusCode, Json<DeployResult>), (StatusCode, Json<ErrorBody>)> {
    let _ports = crate::services::defaults::claim_ports(&state, &[body.rest_port, body.rpc_port])?;

    if state.node_monitor_url.is_some() {
        return Err(crate::error::err_409(
            "Node monitor starts its KV service during admission; use its retained restart operation",
        ));
    }
    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let req = deployment_request(&state, node_id, body);

    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let deployed = ops::kv_server::deploy(&ctx, &req, Some(&workspace_dir))
        .await
        .map_err(map_config_err)?;
    // Apply the server entry directly to state.config instead of
    // commit_op_context — the snapshot-replace pattern loses
    // concurrent updates (e.g. parallel deploys on different nodes).
    let entry = ctx
        .config()
        .server_for_node(node_id)
        .ok_or_else(|| err_500("deploy succeeded but server entry not in ctx"))?
        .clone();
    {
        let mut cfg = state.config.write().unwrap();
        cfg.add_server(entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state.set_runtime_pid(node_id, deployed.pid);
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    // A new server is now in the config — re-seed the shared kv_client
    // so topology refresh can reach this node.
    state.reseed_kv_client().await;
    Ok((
        StatusCode::CREATED,
        Json(DeployResult {
            node_id,
            mgmt_url: deployed.mgmt_url,
            rpc_url: deployed.rpc_url,
            pid: deployed.pid,
        }),
    ))
}

/// `POST /api/nodes/:node_id/server/restart`. Stop the tracked
/// `crowdb-kv-server` process on this node (if any) and immediately
/// re-deploy on the same ports recorded in the `ServerEntry`. The
/// binary path falls back to `CROWDB_KV_SERVER_BIN` / `"crowdb-kv-server"`
/// the same way the initial deploy does when no `binary` override
/// is supplied. Returns the new `DeployResult`.
///
/// Idempotent in the sense that calling it when no process is
/// currently running still performs a deploy (so an operator can
/// recover from an out-of-band crash).
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if no server is registered for this node, `502` if
/// the SSH/local restart cycle fails.
pub async fn http_restart_node_server(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
) -> Result<Json<DeployResult>, (StatusCode, Json<ErrorBody>)> {
    let operation = crate::services::Operation::claim(&state, vec![format!("node/{node_id}")])?;
    tokio::spawn(async move {
        let _operation = operation;
        restart_node_server(state, node_id).await
    })
    .await
    .map_err(|error| err_500(format!("KV lifecycle task failed: {error}")))?
}

async fn restart_node_server(
    state: AppState,
    node_id: u64,
) -> Result<Json<DeployResult>, (StatusCode, Json<ErrorBody>)> {
    if state.node_monitor_url.is_some() {
        let reply = crate::services::remote::kv_action(
            &state,
            node_id,
            crowdb_protocol::mgmt::node::NodeServiceAction::Restart,
        )
        .await?;
        let config = state.config.read().map_err(|error| err_502(error.to_string()))?;
        let entry = config
            .server_for_node(node_id)
            .ok_or_else(|| crate::error::err_404("KV service not found"))?;
        return Ok(Json(DeployResult {
            node_id,
            mgmt_url: entry.url.clone(),
            rpc_url: entry.rpc_url.clone().unwrap_or_default(),
            pid: reply["pid"]
                .as_u64()
                .and_then(|pid| u32::try_from(pid).ok())
                .ok_or_else(|| err_502("Monitor returned no KV PID"))?,
        }));
    }
    let workspace_dir = state
        .prepare_node_workspace(node_id)
        .map_err(|e| err_500(e.to_string()))?;
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    let deployed = ops::kv_server::restart(
        &ctx,
        node_id,
        Some(&workspace_dir),
        state.runtime_pid(node_id),
        &state.authority_seeds,
    )
    .await
    .map_err(map_config_err)?;
    // Apply the updated server entry directly to state.config (avoid
    // commit_op_context snapshot-replace race).
    let new_entry = ctx
        .config()
        .server_for_node(node_id)
        .ok_or_else(|| err_500("restart succeeded but server entry not in ctx"))?
        .clone();
    {
        let mut cfg = state.config.write().unwrap();
        let _ = cfg.remove_server_for_node(node_id);
        cfg.add_server(new_entry).map_err(map_config_err)?;
    }
    state.persist().map_err(map_persist_err)?;
    state.set_runtime_pid(node_id, deployed.pid);
    // Clear cached KV RPC connections so the next KV request reconnects
    // to the restarted server instead of reusing a stale TCP connection.
    if let Some(t) = state.kv_rpc_transport.read().await.as_ref() {
        t.clear_connections();
    }
    // Mark the node as recovering so the monitor cache preserves
    // previously-known stores until the server's /topology confirms
    // them (WAL replay may still be in progress when the first
    // topology fetch succeeds). Must be set BEFORE
    // restore_persisted_topology_for_node, which calls
    // refresh_node_cache internally.
    state.monitor_cache.mark_recovering(node_id).await;
    crate::mgmt::restore_persisted_topology_for_node(&state, node_id)
        .await
        .map_err(|e| err_502(format!("restore topology after restart: {e}")))?;
    // Refresh the monitor cache so health badges reflect the restarted
    // server. The process may not be listening yet, so retry a few
    // times with short delays until the probe succeeds.
    crate::mgmt::refresh_node_cache(&state, node_id).await;
    let state_clone = state.clone();
    tokio::spawn(async move {
        for _ in 0..10 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            crate::mgmt::refresh_node_cache(&state_clone, node_id).await;
            let snap = state_clone.monitor_cache.snapshot().await;
            if let Some(rec) = snap.get(&node_id) {
                if rec.health == NodeHealth::Up {
                    break;
                }
            }
        }
    });

    Ok(Json(DeployResult {
        node_id,
        mgmt_url: deployed.mgmt_url,
        rpc_url: deployed.rpc_url,
        pid: deployed.pid,
    }))
}

fn deployment_request(
    state: &AppState,
    node_id: u64,
    body: DeployNodeServerBody,
) -> crowdb_console_shared::lifecycle::DeployRequest {
    crowdb_console_shared::lifecycle::DeployRequest {
        server_id: node_id.to_string(),
        rest_port: body.rest_port,
        rpc_port: body.rpc_port,
        group0_management_seeds: (*state.authority_seeds).clone(),
        election_profile: body
            .election_profile
            .or_else(|| std::env::var("CROWDB_KV_SERVER_ELECTION_PROFILE").ok()),
        binary: body.binary.map(std::path::PathBuf::from),
        kv_backend: body.kv_backend,
        wal_backend: body.wal_backend,
        no_fsync: body.no_fsync,
        metrics_interval: body.metrics_interval,
        max_inflight: body.max_inflight,
        coalesce_max_keys: body.coalesce_max_keys,
        peer_pool_size: body.peer_pool_size,
        enable_nagle: body.enable_nagle,
        quickack: body.quickack,
        event_write: body.event_write,
        send_queue_capacity: body.send_queue_capacity,
        config: body.config.map(std::path::PathBuf::from),
        rpc_workers: body.rpc_workers,
    }
}
