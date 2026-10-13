// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use axum::{extract::State, routing::get, Json, Router};
use crowdb_protocol::mgmt::node::{CandidateSnapshot, NodeBinding, NodeHandshake, NodeHardware};
use thiserror::Error;
use tokio::sync::watch;

use super::{DiscoveryConfig, DiscoveryError, NodeDiscovery, NodeIdentity, NodeIdentityError};

#[derive(Debug, Error)]
pub enum NodeManagementError {
    #[error(transparent)]
    Identity(#[from] NodeIdentityError),
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    #[error("node management I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("physical host identity must be explicitly configured")]
    PhysicalHost,
}

#[derive(Clone)]
struct ManagementState {
    handshake: watch::Receiver<NodeHandshake>,
    candidates: watch::Receiver<CandidateSnapshot>,
}

/// Run read-only discovery/handshake independently of initialized services.
///
/// # Errors
/// Rejects missing physical-host identity and failed listener/discovery setup.
pub async fn serve_node_management(
    root: &Path,
    bind: SocketAddr,
    config: &DiscoveryConfig,
    physical_host_id: String,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), NodeManagementError> {
    if physical_host_id.is_empty() || physical_host_id.len() > 256 {
        return Err(NodeManagementError::PhysicalHost);
    }
    let identity = NodeIdentity::load_or_create(root)?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    if listener.local_addr()?.port() != config.monitor_port {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "listener port differs from advertised monitor port",
        )
        .into());
    }
    validate_seeds(&config.seeds)?;
    let mut discovery = NodeDiscovery::start(identity, config)?;
    let seed_client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let (updates, candidates) = watch::channel(CandidateSnapshot::default());
    let (handshake_updates, handshake_state) =
        watch::channel(local_handshake(root, &discovery, physical_host_id));
    let state = ManagementState {
        handshake: handshake_state,
        candidates,
    };
    let router = Router::new()
        .route("/node", get(handshake))
        .route("/candidates", get(candidate_snapshot))
        .with_state(state);
    let (stop, stopped) = watch::channel(false);
    let server_stop = stop.clone();
    let mut server_stopped = stopped.clone();
    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        tokio::select! { () = shutdown => {}, _ = server_stopped.changed() => {} }
        server_stop.send_replace(true);
    });
    let browse = async {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let mut stopped = stopped;
        let mut diagnostics = Vec::new();
        let mut seeds = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = seeds.tick(), if !config.seeds.is_empty() => {
                    let mut probes = tokio::task::JoinSet::new();
                    for seed in &config.seeds {
                        let seed = seed.clone(); let client = seed_client.clone();
                        probes.spawn(async move {
                            let result = probe_seed(&client, &seed).await;
                            (seed, result)
                        });
                    }
                    while let Some(reply) = probes.join_next().await {
                        if let Ok((seed, observation)) = reply { discovery.observe_seed(&seed, observation.map(|handshake| handshake.advertisement)); }
                    }
                }
                _ = tick.tick() => {
                    let binding: Option<NodeBinding> = super::durable::read(&root.join("node-binding.json"))?;
                    let cluster_id = if let Some(binding) = binding { Some(binding.bootstrap.cluster_id) } else {
                        super::durable::read::<super::runtime::AcceptedNode>(&root.join("accepted-node.json"))?
                            .map(|accepted| accepted.bootstrap.cluster_id)
                    };
                    discovery.bind_cluster(cluster_id)?;
                    handshake_updates.send_modify(|handshake| handshake.advertisement = discovery.local().clone());
                    for diagnostic in discovery.diagnostics() {
                        if diagnostics.len() == 64 { diagnostics.remove(0); }
                        diagnostics.push(diagnostic);
                    }
                    updates.send_replace(CandidateSnapshot {
                        nodes: discovery.refresh(), discovery_diagnostics: diagnostics.clone(),
                    });
                }
                result = stopped.changed() => {
                    if result.is_err() || *stopped.borrow() { break; }
                }
            }
        }
        discovery.shutdown().await
    };
    let browse = async {
        let result = browse.await;
        if result.is_err() {
            stop.send_replace(true);
        }
        result
    };
    let (server, discovery) = tokio::join!(server, browse);
    server?;
    discovery?;
    Ok(())
}

fn validate_seeds(seeds: &[String]) -> Result<(), NodeManagementError> {
    if seeds.len() > 64 {
        return Err(NodeManagementError::PhysicalHost);
    }
    for seed in seeds {
        let url = reqwest::Url::parse(seed).map_err(|_| NodeManagementError::PhysicalHost)?;
        if url.scheme() != "http"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(NodeManagementError::PhysicalHost);
        }
    }
    Ok(())
}

fn local_handshake(root: &Path, discovery: &NodeDiscovery, physical_host_id: String) -> NodeHandshake {
    let hardware = NodeHardware {
        architecture: std::env::consts::ARCH.into(),
        logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
        memory_bytes: effective_memory(),
        data_root: root.to_string_lossy().into_owned(),
    };
    NodeHandshake {
        advertisement: discovery.local().clone(),
        physical_host_id,
        rack_hint: None,
        hardware,
    }
}

fn effective_memory() -> u64 {
    let host = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|number| number.parse::<u64>().ok())
        })
        .unwrap_or(0)
        * 1024;
    [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .filter_map(|path| std::fs::read_to_string(path).ok()?.trim().parse::<u64>().ok())
    .filter(|limit| *limit > 0)
    .fold(host, u64::min)
}

async fn probe_seed(client: &reqwest::Client, seed: &str) -> Option<NodeHandshake> {
    let mut reply = client
        .get(format!("{}/node", seed.trim_end_matches('/')))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = reply.chunk().await.ok()? {
        if bytes.len() + chunk.len() > 65536 {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}

async fn handshake(State(state): State<ManagementState>) -> Json<NodeHandshake> {
    Json(state.handshake.borrow().clone())
}

async fn candidate_snapshot(State(state): State<ManagementState>) -> Json<CandidateSnapshot> {
    Json(state.candidates.borrow().clone())
}
