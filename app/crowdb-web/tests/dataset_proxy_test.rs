// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::any,
};
use crowdb_console_shared::{
    config::{ServerEntry, ServiceType},
    ConsoleConfig,
};
use crowdb_web::{router, AppState};
use tower::ServiceExt;

async fn fixture(readonly: bool) -> (axum::Router, tokio::task::JoinHandle<()>) {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("dataset-proxy").keep();
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(&root).unwrap();
    let expected = format!("Bearer {}", credentials.dataset_token());
    let upstream = axum::Router::new().route(
        "/*path",
        any(move |request: Request<Body>| {
            let expected = expected.clone();
            async move {
                assert_eq!(request.headers()["authorization"], expected);
                assert!(request.uri().path().starts_with("/api/dataset/v2/"));
                if request.uri().query() == Some("large=1") {
                    return (StatusCode::OK, "x".repeat(1024 * 1024 + 1));
                }
                if request.uri().query() == Some("denied=1") {
                    return (StatusCode::UNAUTHORIZED, "denied".into());
                }
                (StatusCode::OK, r#"{"offset":"9007199254740993"}"#.into())
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("access-1", "http://127.0.0.1:1");
    entry.service_type = ServiceType::AccessServer;
    entry.dataset_origin = Some(origin);
    config.servers.push(entry);
    let mut state = AppState::with_runtime_root(config, root);
    state.readonly = readonly;
    (router(state), task)
}

#[tokio::test]
async fn private_origin_bearer_and_metadata_bounds_are_enforced() {
    let (app, task) = fixture(false).await;
    for (suffix, expected) in [("", 200), ("?denied=1", 401), ("?large=1", 502)] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/access/dataset/api/dataset/v2/capabilities{suffix}"))
                    .header("authorization", "Bearer browser-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("browser-secret"));
        if expected == 200 {
            assert!(String::from_utf8_lossy(&bytes).contains("9007199254740993"));
        }
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/access/dataset/other")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    task.abort();
}

#[tokio::test]
async fn readonly_allows_exact_matrix_but_rejects_all_mutations() {
    let (app, task) = fixture(true).await;
    let base = "/api/access/dataset/api/dataset/v2/namespaces/dataset-ns/datasets/example";
    let snapshot = "11111111111111111111111111111111";
    for (method, suffix, status) in [
        ("POST", format!("/snapshots/{snapshot}/matrix"), 200),
        ("GET", format!("/snapshots/{snapshot}/contains"), 200),
        ("POST", "/compositions".into(), 403),
        ("POST", format!("/snapshots/{snapshot}/reclaim"), 403),
        ("PUT", String::new(), 403),
        ("DELETE", "/uploads/123".into(), 403),
        ("POST", format!("/snapshots/{snapshot}/matrix/extra"), 403),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(format!("{base}{suffix}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status, "{method} {suffix}");
    }
    task.abort();
}
