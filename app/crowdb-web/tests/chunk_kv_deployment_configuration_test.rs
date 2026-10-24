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
    let response = router(AppState::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes/1/services/deploy")
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

fn body() -> Value {
    json!({"kind":"chunk-kv","instance_id":1,"http_port":7400,"rpc_port":7401,"metadata_store_id":0})
}

#[tokio::test]
async fn invalid_stream_geometry_is_rejected_before_execution() {
    for bytes in [0, 1024 * 1024 - 1, 256 * 1024 * 1024 + 1] {
        let mut request = body();
        request["stream_chunk_capacity_bytes"] = json!(bytes);
        let (status, message) = submit(request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.contains("Stream chunk capacity"));
    }
}

#[tokio::test]
async fn stream_geometry_is_scoped_to_chunk_kv_and_requires_authority() {
    for bytes in [None, Some(1024 * 1024), Some(256 * 1024 * 1024)] {
        let mut request = body();
        if let Some(bytes) = bytes {
            request["stream_chunk_capacity_bytes"] = json!(bytes);
        }
        let (status, message) = submit(request).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(message.contains("Group 0 is not ready"));
    }
    for field in ["stream_chunk_capacity_bytes"] {
        let mut request = json!({"kind":"chunkdb","instance_id":1,"http_port":7300,"rpc_port":7301});
        request[field] = if field == "stream_chunk_capacity_bytes" {
            json!(1024 * 1024)
        } else {
            json!(1)
        };
        let (status, message) = submit(request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.contains("selected service type"));
    }
}
