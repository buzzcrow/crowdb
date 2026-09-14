// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use crowdb_chunk_kv_server::{management_router, ChunkKvService, ManagementState};
use tower::ServiceExt;

fn app() -> axum::Router {
    let service = Arc::new(ChunkKvService::new(7, 8).unwrap());
    management_router(ManagementState::new(service))
}

#[tokio::test]
async fn health_is_live_while_unleased_server_is_not_ready() {
    let health = app()
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let body = to_bytes(health.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["instance_id"], 7);
    assert_eq!(body["lifecycle"], "prepared");
    assert_eq!(body["ready"], false);

    let ready = app()
        .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn metrics_exposes_lock_free_service_counters() {
    let response = app()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["requests"], 0);
    assert_eq!(body["successes"], 0);
}
