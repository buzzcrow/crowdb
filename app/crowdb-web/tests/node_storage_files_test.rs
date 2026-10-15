// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_web::{router, AppState};
use tower::ServiceExt;

#[tokio::test]
async fn readonly_standard_node_rejects_storage_and_bootstrap_before_authority_work() {
    let mut state = AppState::default()
        .with_node_monitor("http://127.0.0.1:1")
        .unwrap();
    state.readonly = true;
    let app = router(state);
    for (path, body) in [
        (
            "/api/nodes/1/disk-files",
            r#"{"operation_id":"12345678-1234-4234-8234-123456789abc","disk_group_id":1}"#,
        ),
        ("/api/racks", r#"{"id":1}"#),
        ("/api/cluster/init", r#"{"nodes":[1]}"#),
        (
            "/api/nodes/1/disk-groups/1/disks/0000000000000001-0000000000000001/offline",
            r#"{"expected_revision":"1"}"#,
        ),
        (
            "/api/nodes/1/disk-groups/1/disks/0000000000000001-0000000000000001/removal",
            r#"{"expected_revision":"1"}"#,
        ),
        (
            "/api/nodes/1/disk-groups/1/disks/0000000000000001-0000000000000001/rejoin",
            r#"{"removal_operation_id":"remove/disk/1"}"#,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
    }
    let response = app
        .oneshot(Request::builder().uri("/api/mode").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn file_creation_without_a_monitor_has_no_local_fallback() {
    let app = router(AppState::default());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes/1/disk-files")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"operation_id":"12345678-1234-4234-8234-123456789abc","disk_group_id":1}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/nodes/1/disk-files/not-a-uuid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
