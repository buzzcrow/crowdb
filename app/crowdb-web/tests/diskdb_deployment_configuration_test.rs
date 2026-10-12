// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn submit(body: Value) -> (StatusCode, String) {
    let state = AppState::default();
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes/1/diskdb/deploy")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn invalid_heartbeat_cadence_is_rejected_before_node_execution() {
    let (status, body) = submit(json!({"rpc_port":7100,"keepalive_interval_secs":0})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(body.contains("nonzero"));
    for interval in [json!(-1), json!(4_294_967_296u64), json!("1")] {
        let (status, _) = submit(json!({"rpc_port":7100,"keepalive_interval_secs":interval})).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}

#[tokio::test]
async fn default_and_explicit_cadence_require_group0_before_starting_service() {
    for body in [
        json!({"rpc_port":7100}),
        json!({"rpc_port":7100,"keepalive_interval_secs":1}),
    ] {
        let (status, message) = submit(body).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(message.contains("Group 0 is not ready"));
    }
}
