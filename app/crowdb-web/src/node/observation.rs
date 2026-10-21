// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded local evidence for aggregate cluster observation.

mod protocols;

use crate::{
    error::{err_409, err_502},
    state::AppState,
};
use axum::{extract::State, Json};
use crowdb_console_shared::deployment::{admission::local_control, services};
use crowdb_protocol::mgmt::node::{observation::NodeRuntimeObservation, NodeControl};
use std::time::Duration;

type Failure = (axum::http::StatusCode, Json<crate::error::ErrorBody>);

pub(crate) async fn read(State(state): State<AppState>) -> Result<Json<NodeRuntimeObservation>, Failure> {
    if state.node_monitor_url.is_none() {
        return Err(err_409("Node observation requires an owning monitor"));
    }
    tokio::time::timeout(Duration::from_secs(3), observe(&state))
        .await
        .map_err(|_| err_502("Node observation deadline elapsed"))?
        .map(Json)
}

async fn observe(state: &AppState) -> Result<NodeRuntimeObservation, Failure> {
    let value = local_control(&super::control_socket(), &NodeControl::RuntimeObservation)
        .await
        .map_err(|_| err_502("Local monitor observation is unavailable"))?;
    let mut observation: NodeRuntimeObservation =
        serde_json::from_value(value).map_err(|_| err_502("Local monitor observation is invalid"))?;
    observation
        .policy
        .validate()
        .map_err(|_| err_409("Accepted node observation policy is invalid"))?;
    if observation.version != 1 || uuid::Uuid::parse_str(&observation.discovery_id).is_err() {
        return Err(err_409("Local monitor observation identity is invalid"));
    }
    if let Some(binding) = &observation.binding {
        if super::cluster_binding(state)?.as_ref() != Some(binding) {
            return Err(err_409("Bound node and monitor observation identities differ"));
        }
        let intents = tokio::time::timeout(Duration::from_secs(1), async {
            services::list(state.kv_client().await.as_ref()).await
        })
        .await
        .ok()
        .and_then(Result::ok);
        observation.protocols = protocols::observe(state, &observation, intents.as_deref()).await;
    }
    Ok(observation)
}

#[cfg(feature = "test-util")]
pub async fn protocol_probe_for_tests(protocol: &str, url: &str, token: Option<&str>) -> Option<bool> {
    protocols::request_probe(protocol, url, token).await
}
