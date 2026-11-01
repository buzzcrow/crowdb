// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Authority and each node publish independently; reads never drive sampling.

use super::{
    cache::MAX_AGE,
    sampling::{sample_node, sample_service_health},
    source::Production,
    ServiceObservations,
};
use crate::state::AppState;
use crowdb_console_shared::config::{NodeEntry, ServerEntry};
use crowdb_protocol::mgmt::node::NodeBinding;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    task::{AbortHandle, JoinSet},
    time::{Instant, MissedTickBehavior},
};

pub use super::source::ServiceObservationSource;

/// Stops the coordinator and all its node samplers when the Web listener exits.
pub struct ServiceObserver(tokio::task::JoinHandle<()>);
impl Drop for ServiceObserver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct SamplingLease(Arc<ServiceObservations>);
impl Drop for SamplingLease {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
    }
}

/// Start independent service inventory observations for monitor-owned Web.
///
/// # Errors
/// Rejects an already running observer.
pub fn start_service_observer(state: &AppState) -> Result<Option<ServiceObserver>, &'static str> {
    if state.node_monitor_url.is_none() {
        return Ok(None);
    }
    start(state, Arc::new(Production)).map(Some)
}

/// Start the production sampler with deterministic observation transports.
///
/// # Errors
/// Rejects an unowned Web or a duplicate observer.
#[cfg(feature = "test-util")]
pub fn start_service_observer_for_tests(
    state: &AppState,
    source: Arc<dyn ServiceObservationSource>,
) -> Result<ServiceObserver, &'static str> {
    start(state, source)
}

fn start(
    state: &AppState,
    source: Arc<dyn ServiceObservationSource>,
) -> Result<ServiceObserver, &'static str> {
    if state.node_monitor_url.is_none() {
        return Err("Service sampling requires an owning monitor");
    }
    state
        .service_observations
        .running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| "Service observation sampler is already running")?;
    let state = state.clone();
    let lease = SamplingLease(Arc::clone(&state.service_observations));
    Ok(ServiceObserver(tokio::spawn(async move {
        let _lease = lease;
        coordinate(state, source).await;
    })))
}

async fn coordinate(state: AppState, source: Arc<dyn ServiceObservationSource>) {
    let mut children = JoinSet::new();
    let mut authority_task = children.spawn(authority(state.clone(), Arc::clone(&source)));
    let mut nodes: BTreeMap<u64, (NodeBinding, NodeEntry, AbortHandle)> = BTreeMap::new();
    let mut health: BTreeMap<(u64, String), (NodeBinding, NodeEntry, ServerEntry, AbortHandle)> =
        BTreeMap::new();
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if authority_task.is_finished() {
                    authority_task = children.spawn(authority(state.clone(), Arc::clone(&source)));
                }
                let binding = crate::node::cluster_binding(&state).ok().flatten();
                state.service_observations.synchronize(binding.as_ref());
                let config = state.config.read().unwrap().clone();
                nodes.retain(|id, (old_binding, old_node, task)| {
                    let retain = !task.is_finished() && binding.as_ref() == Some(old_binding) && config.node(*id) == Some(old_node);
                    if !retain { task.abort(); }
                    retain
                });
                health.retain(|(node, id), (old_binding, old_node, entry, task)| {
                    let retain = !task.is_finished() && binding.as_ref() == Some(old_binding)
                        && config.node(*node) == Some(old_node) && config.servers.iter().any(|current| current.id == *id && current == entry);
                    if !retain { task.abort(); }
                    retain
                });
                if let Some(binding) = binding {
                    for node in &config.nodes {
                        if nodes.contains_key(&node.id) { continue; }
                        let handle = children.spawn(sample_node(state.clone(), Arc::clone(&source), binding.clone(), node.clone()));
                        nodes.insert(node.id, (binding.clone(), node.clone(), handle));
                    }
                    for entry in &config.servers {
                        let Some(node) = entry.node_id.and_then(|id| config.node(id)) else { continue; };
                        let key = (node.id, entry.id.clone());
                        if health.contains_key(&key) { continue; }
                        let handle = children.spawn(sample_service_health(state.clone(), Arc::clone(&source), binding.clone(), node.clone(), entry.clone()));
                        health.insert(key, (binding.clone(), node.clone(), entry.clone(), handle));
                    }
                }
            }
            result = children.join_next(), if !children.is_empty() => {
                if let Some(Err(error)) = result {
                    if !error.is_cancelled() { tracing::error!(%error, "service observation worker stopped; unavailable observations expire"); }
                }
            }
        }
    }
}

async fn authority(state: AppState, source: Arc<dyn ServiceObservationSource>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = interval.tick() => {}, () = state.service_observations.changed.notified() => {} }
        let Some(binding) = crate::node::cluster_binding(&state).ok().flatten() else {
            continue;
        };
        state.service_observations.synchronize(Some(&binding));
        let epoch = state.service_observations.latest.load().authority_epoch;
        let started = Instant::now();
        let intents = match tokio::time::timeout(MAX_AGE, source.intents(&state)).await {
            Ok(Ok(intents)) => intents,
            Ok(Err(error)) => {
                phase_error(&state, binding.node_id, "authority", started, &error);
                continue;
            }
            Err(_) => {
                phase_error(
                    &state,
                    binding.node_id,
                    "authority",
                    started,
                    "three-second sampling deadline exceeded",
                );
                continue;
            }
        };
        tracing::debug!(
            phase = "authority",
            elapsed_ms = started.elapsed().as_millis(),
            "service inventory sampled"
        );
        if crate::node::cluster_binding(&state).ok().flatten().as_ref() != Some(&binding) {
            continue;
        }
        let intents: Vec<_> = intents
            .into_iter()
            .filter(|intent| intent.cluster_id == binding.bootstrap.cluster_id)
            .collect();
        if state
            .service_observations
            .authority(&binding, epoch, started, &intents)
        {
            crate::services::remote::project_intents(&state, &intents);
        }
    }
}

pub(super) fn phase_error(state: &AppState, node: u64, phase: &str, started: Instant, error: &str) {
    let diagnostic: String = error
        .chars()
        .filter(|character| !character.is_control())
        .take(256)
        .collect();
    if state.should_warn(
        &format!("service-observation/{node}/{phase}"),
        Duration::from_secs(30),
    ) {
        tracing::warn!(node_id = node, phase, elapsed_ms = started.elapsed().as_millis(), error = %diagnostic,
            "service observation unavailable; stale evidence expires and the next sample retries the read");
    }
}
