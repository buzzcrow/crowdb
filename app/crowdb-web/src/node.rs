// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Local monitor observations, separate from confirmed cluster topology.

mod admission;
mod update;
pub(crate) use update::update;
mod authority;
mod preparation;
pub(crate) use admission::binding as cluster_binding;
pub(crate) use admission::{admissions, admit, control_socket, node_key_path, records, save};
pub(crate) use authority::{guard, status};

use axum::{extract::State, http::StatusCode, Json};
use crowdb_protocol::mgmt::node::{CandidateNode, CandidateSnapshot, CandidateState, NodeHandshake};
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;

use crate::state::AppState;

#[derive(Debug, Serialize)]
pub(crate) struct NodeCandidates {
    pub local: NodeHandshake,
    pub candidates: Vec<CandidateNode>,
    pub diagnostics: Vec<String>,
}

pub(crate) async fn candidates(State(state): State<AppState>) -> Result<Json<NodeCandidates>, StatusCode> {
    let base = state
        .node_monitor_url
        .as_deref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let node_url = format!("{base}/node");
    let candidates_url = format!("{base}/candidates");
    let (local, snapshot) = tokio::try_join!(
        read::<NodeHandshake>(&client, &node_url),
        read::<CandidateSnapshot>(&client, &candidates_url)
    )?;
    let mut candidates = snapshot.nodes;
    if !candidates
        .iter()
        .any(|peer| peer.advertisement.discovery_id == local.advertisement.discovery_id)
    {
        candidates.insert(
            0,
            CandidateNode {
                advertisement: local.advertisement.clone(),
                state: if local.advertisement.cluster_id.is_some() {
                    CandidateState::SameCluster
                } else {
                    CandidateState::Unbound
                },
            },
        );
    }
    Ok(Json(NodeCandidates {
        local,
        candidates,
        diagnostics: snapshot.discovery_diagnostics,
    }))
}

async fn read<T: DeserializeOwned>(client: &reqwest::Client, url: &str) -> Result<T, StatusCode> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?
        .error_for_status()
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| StatusCode::BAD_GATEWAY)? {
        if bytes.len() + chunk.len() > 256 * 1024 {
            return Err(StatusCode::BAD_GATEWAY);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| StatusCode::BAD_GATEWAY)
}

mod cleanup;
pub(crate) use cleanup::cleanup;

mod cancel;
pub(crate) use cancel::cancel;
