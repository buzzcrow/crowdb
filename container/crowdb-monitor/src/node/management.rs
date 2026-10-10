// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use axum::{extract::State, routing::get, Json, Router};
use crowdb_protocol::mgmt::node::{CandidateSnapshot, NodeHandshake};
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
    handshake: NodeHandshake,
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
    let mut discovery = NodeDiscovery::start(identity, config)?;
    let (updates, candidates) = watch::channel(CandidateSnapshot::default());
    let state = ManagementState {
        handshake: NodeHandshake {
            advertisement: discovery.local().clone(),
            physical_host_id,
            rack_hint: None,
        },
        candidates,
    };
    let router = Router::new()
        .route("/node", get(handshake))
        .route("/candidates", get(candidate_snapshot))
        .with_state(state);
    let (stop, stopped) = watch::channel(false);
    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        shutdown.await;
        stop.send_replace(true);
    });
    let browse = async {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let mut stopped = stopped;
        let mut diagnostics = Vec::new();
        loop {
            tokio::select! {
                _ = tick.tick() => {
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
    let (server, discovery) = tokio::join!(server, browse);
    server?;
    discovery?;
    Ok(())
}

async fn handshake(State(state): State<ManagementState>) -> Json<NodeHandshake> {
    Json(state.handshake)
}

async fn candidate_snapshot(State(state): State<ManagementState>) -> Json<CandidateSnapshot> {
    Json(state.candidates.borrow().clone())
}
