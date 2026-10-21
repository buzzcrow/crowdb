// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Node protocol and service probes run independently of status readers.

use crate::state::AppState;
use arc_swap::ArcSwapOption;
use axum::http::StatusCode;
use crowdb_protocol::mgmt::node::observation::NodeRuntimeObservation;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::{Instant, MissedTickBehavior};

type ObservationResult = Result<NodeRuntimeObservation, (StatusCode, String)>;

#[derive(Default)]
pub(crate) struct NodeObservations {
    pub(super) latest: ArcSwapOption<CachedObservation>,
    running: AtomicBool,
}

pub(super) struct CachedObservation {
    pub result: ObservationResult,
    pub started: Instant,
}

/// Cancels the node's background sampler when the Web listener exits.
pub struct NodeObserver(tokio::task::JoinHandle<()>);
impl Drop for NodeObserver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct SamplingLease(Arc<NodeObservations>);
impl Drop for SamplingLease {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
    }
}

/// Start independent protocol and service observations for a monitor-owned Web.
///
/// # Errors
/// Rejects another active sampling worker.
pub fn start_node_observer(state: &AppState) -> Result<Option<NodeObserver>, &'static str> {
    if state.node_monitor_url.is_none() {
        return Ok(None);
    }
    let sample_state = state.clone();
    start(state, move || {
        let state = sample_state.clone();
        async move {
            super::observe(&state)
                .await
                .map_err(|(status, body)| (status, body.0.error))
        }
    })
    .map(Some)
}

pub(super) fn start<F, Fut>(state: &AppState, source: F) -> Result<NodeObserver, &'static str>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = ObservationResult> + Send,
{
    if state.node_monitor_url.is_none() {
        return Err("Node sampling requires an owning monitor");
    }
    state
        .node_observations
        .running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| "Node observation sampler is already running")?;
    let observations = Arc::clone(&state.node_observations);
    let lease = SamplingLease(Arc::clone(&observations));
    Ok(NodeObserver(tokio::spawn(async move {
        let _lease = lease;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let started = Instant::now();
            let result = tokio::time::timeout(Duration::from_secs(3), source())
                .await
                .unwrap_or_else(|_| {
                    Err((
                        StatusCode::BAD_GATEWAY,
                        "Backend node observation exceeded its three-second deadline".into(),
                    ))
                });
            observations
                .latest
                .store(Some(Arc::new(CachedObservation { result, started })));
        }
    })))
}
