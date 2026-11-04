// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Each monitor and each endpoint has its own sampling cadence.

use super::{
    cache::{Sample, MAX_AGE},
    source::{ProcessObservation, ServiceObservationSource},
    worker::phase_error,
};
use crate::state::AppState;
use crowdb_console_shared::{
    cluster::NodeHealth,
    config::{NodeEntry, ServerEntry, ServiceType},
};
use crowdb_protocol::mgmt::node::NodeBinding;
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};

pub(super) async fn sample_node(
    state: AppState,
    source: Arc<dyn ServiceObservationSource>,
    binding: NodeBinding,
    node: NodeEntry,
) {
    let mut transport = match source.process_observer(&state, &binding, &node) {
        Ok(transport) => transport,
        Err(error) => {
            phase_error(&state, node.id, "processes", Instant::now(), &error);
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = interval.tick() => {}, () = state.service_observations.changed.notified() => {} }
        let config = state.config.read().unwrap().clone();
        if crate::node::cluster_binding(&state).ok().flatten().as_ref() != Some(&binding)
            || config.node(node.id) != Some(&node)
        {
            return;
        }
        let observed = state.service_observations.latest.load_full();
        let samples: Vec<_> = config
            .servers
            .iter()
            .filter(|entry| entry.node_id == Some(node.id))
            .map(|entry| observed.sample(&binding, &node, entry))
            .collect();
        sample_processes(&state, source.as_ref(), &mut transport, &binding, &node, &samples).await;
    }
}

async fn sample_processes(
    state: &AppState,
    source: &dyn ServiceObservationSource,
    observer: &mut Option<Box<dyn ProcessObservation>>,
    binding: &NodeBinding,
    node: &NodeEntry,
    samples: &[Sample],
) {
    let started = Instant::now();
    let read = async {
        match observer {
            Some(observer) => observer.read().await,
            None => source.processes(state, binding, node).await,
        }
    };
    let result = match tokio::time::timeout(MAX_AGE, read).await {
        Ok(Ok(value)) => Some(value),
        Ok(Err(error)) => {
            phase_error(state, node.id, "processes", started, &error);
            None
        }
        Err(_) => {
            phase_error(
                state,
                node.id,
                "processes",
                started,
                "three-second sampling deadline exceeded",
            );
            None
        }
    };
    let bootstrap = serde_json::to_value(&binding.bootstrap).expect("bootstrap serialization is infallible");
    let value = result.filter(|value| {
        value["node_id"] == node.id
            && value["cluster_id"] == binding.bootstrap.cluster_id
            && value["bootstrap"] == bootstrap
    });
    if value.is_none() {
        tracing::debug!(
            node_id = node.id,
            phase = "processes",
            "no current authenticated process observation"
        );
    }
    tracing::debug!(
        node_id = node.id,
        phase = "processes",
        elapsed_ms = started.elapsed().as_millis(),
        "service inventory sampled"
    );
    for sample in samples {
        let id = if sample.entry.service_type == ServiceType::PaxosKv {
            "kv"
        } else {
            &sample.entry.id
        };
        let owned = value.as_ref().filter(|value| {
            sample
                .intent
                .as_ref()
                .map_or(sample.entry.service_type == ServiceType::PaxosKv, |intent| {
                    value["process_operations"][id] == intent.operation_id
                })
        });
        let pid = owned
            .and_then(|value| value["processes"][id].as_u64())
            .and_then(|pid| u32::try_from(pid).ok())
            .filter(|pid| *pid > 0);
        let exhausted = value
            .as_ref()
            .zip(sample.intent.as_ref())
            .is_some_and(|(value, intent)| {
                value["service_failures"][&intent.service_id]["operation_id"] == intent.operation_id
                    && value["service_failures"][&intent.service_id]["exhausted"] == true
            });
        let has_process_evidence = value
            .as_ref()
            .and_then(|value| value["processes"].as_object())
            .is_some_and(|processes| match processes.get(id) {
                None => sample.intent.is_some() || sample.entry.service_type == ServiceType::PaxosKv,
                Some(value) => value
                    .as_u64()
                    .and_then(|pid| u32::try_from(pid).ok())
                    .is_some_and(|pid| pid > 0 && owned.is_some()),
            });
        state
            .service_observations
            .process(sample, started, pid, exhausted, has_process_evidence);
    }
}

pub(super) async fn sample_service_health(
    state: AppState,
    source: Arc<dyn ServiceObservationSource>,
    binding: NodeBinding,
    node: NodeEntry,
    entry: ServerEntry,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = interval.tick() => {}, () = state.service_observations.changed.notified() => {} }
        let config = state.config.read().unwrap().clone();
        if crate::node::cluster_binding(&state).ok().flatten().as_ref() != Some(&binding)
            || config.node(node.id) != Some(&node)
            || !config.servers.contains(&entry)
        {
            return;
        }
        let observed = state.service_observations.latest.load_full();
        let sample = observed.sample(&binding, &node, &entry);
        let intents = observed
            .authority
            .as_ref()
            .filter(|authority| authority.started.elapsed() <= MAX_AGE)
            .map_or(&[][..], |authority| authority.intents.as_slice());
        let started = Instant::now();
        let pid = observed
            .read(Some(&sample.binding), Some(&sample.node), &sample.entry)
            .0;
        let response = tokio::select! {
            biased;
            () = state.service_observations.changed.notified() => continue,
            result = tokio::time::timeout(MAX_AGE, source.health(&state, &config, &sample.entry, pid, intents)) => result,
        };
        let health = if let Ok(health) = response {
            health
        } else {
            phase_error(
                &state,
                sample.node.id,
                "health",
                started,
                "three-second sampling deadline exceeded",
            );
            NodeHealth::Unknown
        };
        tracing::debug!(node_id = sample.node.id, service_id = %sample.entry.id, phase = "health", elapsed_ms = started.elapsed().as_millis(), ?health, "service endpoint sampled");
        state.service_observations.health(&sample, started, health);
    }
}
