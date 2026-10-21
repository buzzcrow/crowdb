// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded local evidence for aggregate cluster observation.

mod protocols;
mod services;
mod worker;
pub(crate) use worker::NodeObservations;
pub use worker::{start_node_observer, NodeObserver};

use crate::{
    error::{err_409, err_502},
    state::AppState,
};
use axum::{extract::State, Json};
use crowdb_console_shared::deployment::{admission::local_control, services as service_intents};
use crowdb_protocol::mgmt::node::{observation::NodeRuntimeObservation, NodeControl};
use std::time::Duration;

type Failure = (axum::http::StatusCode, Json<crate::error::ErrorBody>);

pub(crate) async fn read(State(state): State<AppState>) -> Result<Json<NodeRuntimeObservation>, Failure> {
    if state.node_monitor_url.is_none() {
        return Err(err_409("Node observation requires an owning monitor"));
    }
    let observed = state
        .node_observations
        .latest
        .load_full()
        .ok_or_else(|| err_502("Backend node observation is pending"))?;
    if observed.started.elapsed() > Duration::from_secs(3) {
        return Err(err_502(
            "Backend node observation expired; current health is unknown",
        ));
    }
    match &observed.result {
        Ok(value) => Ok(Json(value.clone())),
        Err((status, error)) => Err((*status, Json(crate::error::ErrorBody { error: error.clone() }))),
    }
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
        if !super::cluster_binding(state)?.as_ref().is_some_and(|current| {
            current.node_id == binding.node_id && current.bootstrap == binding.bootstrap
        }) {
            return Err(err_409("Bound node and monitor observation identities differ"));
        }
        let intents = tokio::time::timeout(Duration::from_secs(1), async {
            let kv = state.kv_client().await;
            kv.set_mgmt_seeds(binding.management_seeds.clone());
            service_intents::list(kv.as_ref()).await
        })
        .await
        .ok()
        .and_then(Result::ok);
        let (protocols, service_details) = tokio::join!(
            protocols::observe(state, &observation, intents.as_deref()),
            services::observe(state, &observation, intents.as_deref()),
        );
        observation.protocols = protocols;
        for (id, detail) in service_details {
            if let Some(service) = observation
                .services
                .iter_mut()
                .find(|service| service.service_id == id)
            {
                service.detail = format!("{}; {detail}", service.detail);
            }
        }
    }
    Ok(observation)
}

#[cfg(feature = "test-util")]
pub async fn protocol_probe_for_tests(protocol: &str, url: &str, token: Option<&str>) -> Option<bool> {
    protocols::request_probe(protocol, url, token).await
}

/// Execute the same signed local S3 probe used by node observation.
///
/// # Errors
/// Returns the credential, transport, HTTP or response diagnostic.
#[cfg(feature = "test-util")]
pub async fn s3_protocol_probe_for_tests(
    state: &AppState,
    url: &str,
) -> Result<String, (&'static str, String)> {
    protocols::signed_s3_probe(state, url).await
}

/// Read bounded local Chunk-KV range evidence using a committed service config.
///
/// # Errors
/// Returns a listener, identity, transport or response diagnostic.
#[cfg(feature = "test-util")]
pub async fn service_detail_for_tests(
    state: &AppState,
    intent: &crowdb_protocol::mgmt::node::NodeServiceIntent,
) -> Result<String, String> {
    services::read(state, intent).await
}

/// Start the production sampling loop with an injected observation source.
///
/// # Errors
/// Rejects a duplicate worker or a state without an owning node monitor.
#[cfg(feature = "test-util")]
pub fn start_node_observer_for_tests<F, Fut>(
    state: &AppState,
    source: F,
) -> Result<NodeObserver, &'static str>
where
    F: Fn() -> Fut + Send + 'static,
    Fut:
        std::future::Future<Output = Result<NodeRuntimeObservation, (axum::http::StatusCode, String)>> + Send,
{
    worker::start(state, source)
}

#[cfg(feature = "test-util")]
#[must_use]
pub fn node_observation_time_for_tests(state: &AppState) -> Option<u64> {
    state
        .node_observations
        .latest
        .load()
        .as_ref()
        .and_then(|observed| observed.result.as_ref().ok())
        .map(|value| value.observed_at_unix_ms)
}
