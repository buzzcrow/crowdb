// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Enrich supervised operation evidence with bounded local range diagnostics.

mod access;

use crate::state::AppState;
use crowdb_protocol::mgmt::node::{
    observation::NodeRuntimeObservation, NodeServiceAction, NodeServiceIntent,
};
use std::time::Duration;

pub(super) async fn observe(
    state: &AppState,
    node: &NodeRuntimeObservation,
    intents: Option<&[NodeServiceIntent]>,
) -> Vec<(String, String)> {
    let Some(binding) = &node.binding else {
        return Vec::new();
    };
    futures::future::join_all(
        intents
            .into_iter()
            .flatten()
            .filter(|intent| {
                matches!(intent.kind.as_str(), "chunk-kv" | "access")
                    && intent.node_id == binding.node_id
                    && intent.cluster_id == binding.bootstrap.cluster_id
                    && !matches!(intent.action, NodeServiceAction::Stop | NodeServiceAction::Delete)
            })
            .map(|intent| async move {
                let detail = read(state, intent).await.unwrap_or_else(|detail| detail);
                (intent.service_id.clone(), detail)
            }),
    )
    .await
}

pub(super) async fn read(state: &AppState, intent: &NodeServiceIntent) -> Result<String, String> {
    if intent.kind == "access" {
        return access::read(state, intent).await;
    }
    let invalid = || "Chunk-KV current intent has no valid local management listener".to_owned();
    let config: toml::Value = toml::from_str(&intent.configuration).map_err(|_| invalid())?;
    let address = config
        .get("http_listen_addr")
        .and_then(toml::Value::as_str)
        .ok_or_else(invalid)?;
    let origin = super::protocols::internal_origin(state, intent.node_id, address).ok_or_else(invalid)?;
    let expected = config
        .get("instance_id")
        .and_then(toml::Value::as_integer)
        .ok_or_else(invalid)?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(1))
        .build()
        .map_err(|_| invalid())?;
    let mut response = client.get(format!("{origin}/health")).send().await.map_err(|_| {
        format!("Chunk-KV health at {origin} is unreachable or exceeded its one-second deadline")
    })?;
    if !response.status().is_success() {
        return Err(format!(
            "Chunk-KV health at {origin} returned HTTP {}",
            response.status()
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "Chunk-KV health body is unavailable".to_owned())?
    {
        if bytes.len() + chunk.len() > 65_536 {
            return Err("Chunk-KV health body exceeds 65536 bytes".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let body: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "Chunk-KV health body is invalid JSON".to_owned())?;
    if body["instance_id"].as_i64() != Some(expected) {
        return Err("Chunk-KV health instance differs from current committed intent".into());
    }
    let detail = body["detail"]
        .as_str()
        .ok_or("Chunk-KV health has no detailed readiness evidence")?;
    let ranges = body["ranges"]
        .as_array()
        .ok_or("Chunk-KV health has no range ownership evidence")?;
    Ok(format!(
        "{detail}; ranges: {}",
        serde_json::to_string(ranges).map_err(|_| "Chunk-KV range evidence cannot be encoded")?
    ))
}
