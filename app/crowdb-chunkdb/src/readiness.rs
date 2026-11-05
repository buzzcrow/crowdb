// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP readers observe bounded ownership evidence without driving authority reads.

use crate::range_guard::RangeGuard;
use arc_swap::ArcSwapOption;
use axum::{http::StatusCode, routing::get, Router};
use crowdb_paxos_kv_client::PaxosKvClient;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

const MAX_AGE: Duration = Duration::from_secs(3);

struct Observation {
    started: Instant,
    succeeded: bool,
}

pub struct RangeReadiness {
    guard: Arc<RangeGuard>,
    latest: ArcSwapOption<Observation>,
}

impl RangeReadiness {
    pub fn new(guard: Arc<RangeGuard>) -> Self {
        Self {
            guard,
            latest: ArcSwapOption::empty(),
        }
    }

    /// Use the same independently refreshed evidence for every HTTP reader.
    pub fn router(self: Arc<Self>) -> Router {
        Router::new().route(
            "/ready",
            get(move || {
                let readiness = Arc::clone(&self);
                async move { readiness.response() }
            }),
        )
    }

    fn response(&self) -> (StatusCode, &'static str) {
        let Some(observed) = self.latest.load_full() else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "range ownership observation pending",
            );
        };
        if observed.started.elapsed() > MAX_AGE {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "range ownership observation expired",
            );
        }
        if !observed.succeeded {
            return (StatusCode::SERVICE_UNAVAILABLE, "range ownership refresh failed");
        }
        if self.guard.is_ready() {
            (StatusCode::OK, "ok")
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, "range ownership pending")
        }
    }

    /// Own one bounded refresh at a time; callers never initiate extra reads.
    pub fn start(
        self: Arc<Self>,
        kv: Arc<PaxosKvClient>,
        instance_id: u64,
        stop: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let guard = Arc::clone(&self.guard);
        self.start_source(stop, move || {
            let guard = Arc::clone(&guard);
            let kv = Arc::clone(&kv);
            async move {
                guard
                    .load_from_group0(&kv, instance_id)
                    .await
                    .map_err(|error| error.to_string())
            }
        })
    }

    fn start_source<F, Fut>(self: Arc<Self>, mut stop: watch::Receiver<bool>, source: F) -> JoinHandle<()>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send,
    {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                if *stop.borrow() {
                    return;
                }
                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = stop.changed() => return,
                }
                tokio::select! {
                    () = self.sample(source()) => {},
                    _ = stop.changed() => return,
                }
            }
        })
    }

    async fn sample(&self, source: impl Future<Output = Result<(), String>>) {
        let started = Instant::now();
        let result = tokio::time::timeout(MAX_AGE, source)
            .await
            .unwrap_or_else(|_| Err("range ownership refresh exceeded its three-second deadline".into()));
        self.latest.store(Some(Arc::new(Observation {
            started,
            succeeded: result.is_ok(),
        })));
        if let Err(error) = result {
            tracing::warn!(%error, "periodic range guard refresh failed; readiness remains unavailable");
        }
    }

    /// Exercise production scheduling, expiry and cancellation with deterministic I/O.
    #[cfg(feature = "test-util")]
    pub fn start_source_for_tests<F, Fut>(
        self: Arc<Self>,
        stop: watch::Receiver<bool>,
        source: F,
    ) -> JoinHandle<()>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send,
    {
        self.start_source(stop, source)
    }
}
