// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP liveness, readiness, and metrics surface.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::{ChunkKvService, ServerLifecycle, ServerMetricsSnapshot};

#[derive(Clone)]
pub struct ManagementState {
    service: Arc<ChunkKvService>,
}

impl ManagementState {
    #[must_use]
    pub fn new(service: Arc<ChunkKvService>) -> Self {
        Self { service }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    instance_id: u64,
    lifecycle: &'static str,
    catalog_generation: u64,
    hosted_partitions: usize,
    serving_partitions: usize,
    ready: bool,
}

pub fn management_router(state: ManagementState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn health(State(state): State<ManagementState>) -> Json<HealthResponse> {
    Json(health_response(&state))
}

async fn ready(State(state): State<ManagementState>) -> (StatusCode, Json<HealthResponse>) {
    let response = health_response(&state);
    let status = if response.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(response))
}

async fn metrics(State(state): State<ManagementState>) -> Json<ServerMetricsSnapshot> {
    Json(state.service.metrics_snapshot())
}

fn health_response(state: &ManagementState) -> HealthResponse {
    let health = state.service.health(state.service.monotonic_ms());
    HealthResponse {
        instance_id: health.instance_id,
        lifecycle: lifecycle_name(health.lifecycle),
        catalog_generation: health.catalog_generation,
        hosted_partitions: health.partitions.len(),
        serving_partitions: health.serving_partitions,
        ready: health.lifecycle == ServerLifecycle::Serving,
    }
}

fn lifecycle_name(lifecycle: ServerLifecycle) -> &'static str {
    match lifecycle {
        ServerLifecycle::Prepared => "prepared",
        ServerLifecycle::Serving => "serving",
        ServerLifecycle::Draining => "draining",
    }
}
