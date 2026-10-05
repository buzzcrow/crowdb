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
    /// Canonical service kind, including "paxos-kv" and "diskdb".
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
    let cfg = state.config.read().unwrap().clone();
    let rows = futures::future::join_all(cfg.servers.iter().map(|s| {
        let state = &state;
        let cfg = &cfg;
        let snap = &snap;
        async move {
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
            let health = if s.service_type == ServiceType::AccessServer {
                if let Some(url) = cfg
                    .local_launches
                    .get(&s.id)
                    .and_then(crowdb_console_shared::config::LocalLaunchSpec::access_health_url)
                {
                    let healthy = reqwest::Client::new()
                        .get(url)
                        .timeout(std::time::Duration::from_secs(1))
                        .send()
                        .await
                        .is_ok_and(|response| response.status().is_success());
                    if healthy {
                        NodeHealth::Up
                    } else {
                        NodeHealth::Down
                    }
                } else {
                    NodeHealth::Unknown
                }
            } else if s.service_type == ServiceType::PaxosKv
                && pid.is_some()
                && s.node_id.is_some_and(|id| {
                    !cfg.stores.iter().any(|store| store.nodes.contains(&id))
                        && snap.get(&id).map_or(true, |record| record.stores.is_empty())
                })
            {
                // Before the first store exists, PKV reserves its RPC port but
                // does not bind it. Process liveness covers this startup phase.
                NodeHealth::Up
            } else if let Some(endpoint) = &s.rpc_url {
                if state.rpc_health.probe(endpoint).await {
                    NodeHealth::Up
                } else {
                    NodeHealth::Down
                }
            } else if s.service_type == ServiceType::PaxosKv {
                s.node_id.and_then(|id| snap.get(&id)).map_or(
                    if pid.is_some() {
                        NodeHealth::Up
                    } else {
                        NodeHealth::Unknown
                    },
                    |record| record.health,
                )
            } else if pid.is_some() {
                NodeHealth::Up
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
        }
    }))
    .await;
    Json(rows)
}
