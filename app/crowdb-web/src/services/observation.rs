// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod access;
mod cache;
mod mutation;
mod sampling;
mod source;
#[cfg(feature = "test-util")]
mod test_support;
mod worker;
#[cfg(feature = "test-util")]
pub use access::remote_access_health_for_tests;
pub(crate) use cache::ServiceObservations;
#[cfg(feature = "test-util")]
pub use source::ProcessObservation as ProcessObservationForTests;
#[cfg(feature = "test-util")]
pub use test_support::{invalidate_service_observation_for_tests, ServiceObservationMutationForTests};
pub use worker::{start_service_observer, ServiceObserver};
#[cfg(feature = "test-util")]
pub use worker::{
    start_service_observer_for_tests, ServiceObservationSource as ServiceObservationSourceForTests,
};

use crate::state::AppState;
use axum::{extract::State, Json};
use crowdb_console_shared::{cluster::NodeHealth, config::ServiceType};
use serde::Serialize;

/// One row of `GET /api/servers`: a deployed `crowdb-paxos-kv-server` projected
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
    /// Fresh accepted monitor evidence; absent for the legacy local inventory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_observed: Option<bool>,
    /// Accepted start awaits an owned PID observation and has not exhausted its launch budget.
    pub start_pending: bool,
    /// Node process namespace for a remote PID; never a Web-host PID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid_node_id: Option<u64>,
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
    let cfg = state.config.read().unwrap().clone();
    let remote = state.node_monitor_url.is_some();
    let binding = remote
        .then(|| crate::node::cluster_binding(&state).ok().flatten())
        .flatten();
    let observed = state.service_observations.latest.load_full();
    // Remote inventory readers never perform authority, SSH, or endpoint I/O.
    let snap = if remote {
        std::collections::BTreeMap::default()
    } else {
        state.monitor_cache.snapshot().await
    };
    let rows = futures::future::join_all(cfg.servers.iter().map(|s| {
        let state = &state;
        let cfg = &cfg;
        let snap = &snap;
        let observed = &observed;
        let binding = &binding;
        async move {
            let (pid, health, start_pending, process_observed) = if remote {
                let (pid, health, pending, observed) =
                    observed.read(binding.as_ref(), s.node_id.and_then(|id| cfg.node(id)), s);
                (pid, health, pending, Some(observed))
            } else {
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
                (
                    pid,
                    observe_health(state, cfg, s, pid, snap, &[]).await,
                    false,
                    None,
                )
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
                start_pending,
                process_observed,
                pid_node_id: if remote && pid.is_some() { s.node_id } else { None },
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

async fn observe_health(
    state: &AppState,
    cfg: &crowdb_console_shared::ConsoleConfig,
    s: &crowdb_console_shared::config::ServerEntry,
    pid: Option<u32>,
    snap: &std::collections::BTreeMap<u64, crowdb_console_shared::monitor::NodeRecord>,
    intents: &[crowdb_protocol::mgmt::node::NodeServiceIntent],
) -> NodeHealth {
    if s.service_type == ServiceType::AccessServer {
        access::observe(cfg, s, state.node_monitor_url.is_some(), intents).await
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
    }
}
