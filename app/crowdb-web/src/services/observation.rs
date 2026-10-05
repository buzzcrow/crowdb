// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::state::AppState;
use axum::{extract::State, Json};
use crowdb_console_shared::{cluster::NodeHealth, config::ServiceType};
use serde::Serialize;

/// One row of `GET /api/servers`: a deployed `crowdb-kv-server` projected
/// from the persisted config plus the live monitor cache.
#[derive(Debug, Serialize)]
pub struct ServerSummary {
    pub id: String,
    /// Owning node id (`None` for plain externally-registered servers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
    /// KV management URL. Absent for `DiskDB`, which has no public
    /// management URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mgmt_url: Option<String>,
    /// Public service endpoint. Used by `DiskDB`; absent for KV when its
    /// endpoint is represented by `rpc_url`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,
    /// Live pid if the console currently tracks the process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Latest health from the monitor cache (`unknown` until probed).
    pub health: NodeHealth,
    /// Service type: "kv" (crowdb-kv-server) or "diskdb".
    pub service_type: String,
}

/// `GET /api/servers`. Cluster-wide list of deployed servers, one row
/// per `ServerEntry`, with health from the monitor cache and the live
/// pid when tracked. The CLI's `server list` renders this directly.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
pub async fn http_list_servers(State(state): State<AppState>) -> Json<Vec<ServerSummary>> {
    let snap = state.monitor_cache.snapshot().await;
    let cfg = state.config.read().unwrap();
    let rows = cfg
        .servers
        .iter()
        .map(|s| {
            let runtime_pid = match s.node_id {
                Some(node_id) if s.service_type == ServiceType::Diskdb => {
                    state.diskdb_runtime_pid(node_id.to_string())
                }
                Some(node_id) if s.service_type == ServiceType::PaxosKv => {
                    state.runtime_pid(node_id.to_string())
                }
                _ => None,
            };
            let pid = runtime_pid.or_else(|| {
                s.pid
                    .filter(|pid| crowdb_console_shared::lifecycle::process_is_alive(*pid))
            });
            // KV health comes from the monitor cache. The auxiliary services
            // do not all expose the same HTTP health contract (DiskDB and
            // DiskIO are RPC services), so a tracked live process is the
            // readiness signal for every deployed service.
            let health = if s.node_id.is_none() || !matches!(s.service_type, ServiceType::PaxosKv) {
                if pid.is_some() {
                    NodeHealth::Up
                } else {
                    NodeHealth::Down
                }
            } else if pid.is_some() {
                s.node_id
                    .and_then(|n| snap.get(&n))
                    .map_or(NodeHealth::Up, |rec| rec.health)
            } else {
                NodeHealth::Down
            };
            ServerSummary {
                id: s.id.clone(),
                node_id: s.node_id,
                mgmt_url: (s.url.starts_with("http://") || s.url.starts_with("https://"))
                    .then(|| s.url.clone()),
                endpoint: (s.service_type != ServiceType::PaxosKv)
                    .then(|| s.rpc_url.clone().unwrap_or_else(|| s.url.clone())),
                rpc_url: s.rpc_url.clone(),
                pid,
                health,
                service_type: match s.service_type {
                    crowdb_console_shared::config::ServiceType::PaxosKv => "paxos-kv",
                    crowdb_console_shared::config::ServiceType::Diskdb => "diskdb",
                    crowdb_console_shared::config::ServiceType::Chunkdb => "chunkdb",
                    crowdb_console_shared::config::ServiceType::Diskio => "diskio",
                    crowdb_console_shared::config::ServiceType::ChunkKv => "chunk-kv",
                    crowdb_console_shared::config::ServiceType::AccessServer => "access-server",
                    crowdb_console_shared::config::ServiceType::Rpc => "rpc",
                }
                .to_string(),
            }
        })
        .collect();
    Json(rows)
}
