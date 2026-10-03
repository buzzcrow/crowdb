// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP liveness, readiness, and metrics surface.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

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
        .route("/partitions/:id/observation", get(partition_observation))
        .with_state(state)
}

#[derive(Deserialize)]
struct ObservationQuery {
    generation: u64,
    epoch: u64,
}

async fn partition_observation(
    State(state): State<ManagementState>,
    Path(id): Path<String>,
    Query(query): Query<ObservationQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let failure = |status, error| (status, Json(serde_json::json!({ "error": error })));
    if id.len() != 32 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(failure(StatusCode::BAD_REQUEST, "invalid_partition_id"));
    }
    let id = crowdb_protocol::chunk_kv::Id128 {
        high: u64::from_str_radix(&id[..16], 16)
            .map_err(|_| failure(StatusCode::BAD_REQUEST, "invalid_partition_id"))?,
        low: u64::from_str_radix(&id[16..], 16)
            .map_err(|_| failure(StatusCode::BAD_REQUEST, "invalid_partition_id"))?,
    };
    state
        .service
        .observe_partition(id, query.generation, query.epoch)
        .map(Json)
        .map_err(|error| {
            failure(
                if matches!(error, "partition_not_found" | "partition_not_hosted") {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::CONFLICT
                },
                error,
            )
        })
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
