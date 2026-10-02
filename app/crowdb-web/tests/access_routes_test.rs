// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::any;
use crowdb_console_shared::config::{ServerEntry, ServiceType};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use tower::ServiceExt;

fn configured(origin: &str) -> axum::Router {
    let mut config = ConsoleConfig::default();
    for protocol in ["s3", "iceberg"] {
        let mut entry = ServerEntry::new(format!("console-access-{protocol}"), origin.to_string());
        entry.service_type = ServiceType::AccessServer;
        entry.auto_start = false;
        config.servers.push(entry);
    }
    router(AppState::with_config(config, None))
}

#[tokio::test]
async fn s3_root_with_or_without_trailing_slash_reaches_native_service() {
    let upstream = axum::Router::new().route("/", any(|| async { "native-root" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    for path in ["/api/access/s3", "/api/access/s3/"] {
        let response = configured(&address)
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 100).await.unwrap(),
            "native-root"
        );
    }
    task.abort();
}

#[tokio::test]
async fn native_proxy_preserves_authentication_status_and_streamed_body() {
    let upstream = axum::Router::new().route(
        "/*path",
        any(|request: Request<Body>| async move {
            assert_eq!(request.uri().path(), "/bucket/a%20b");
            assert_eq!(request.uri().query(), Some("uploadId=a%2Fb"));
            assert_eq!(request.headers()["authorization"], "native-credential");
            assert_eq!(request.headers()["range"], "bytes=0-3");
            assert!(!request.headers().contains_key("x-console-secret"));
            (
                StatusCode::PARTIAL_CONTENT,
                [("etag", "native-etag"), ("content-range", "bytes 0-3/100")],
                "data",
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = configured(&format!("http://{}", listener.local_addr().unwrap()));
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/access/s3/bucket/a%20b?uploadId=a%2Fb")
                .header("authorization", "native-credential")
                .header("range", "bytes=0-3")
                .header("x-console-secret", "do-not-forward")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["etag"], "native-etag");
    assert_eq!(response.headers()["content-range"], "bytes 0-3/100");
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 100).await.unwrap(),
        "data"
    );
    task.abort();
}

#[tokio::test]
async fn proxy_rejects_unknown_protocol_and_non_catalog_paths_before_network() {
    for path in ["/api/access/other/x", "/api/access/iceberg/admin"] {
        let response = configured("http://127.0.0.1:1")
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
}

#[tokio::test]
async fn access_configuration_rejects_embedded_credentials_and_paths() {
    for origin in [
        "file:///tmp/secret",
        "http://user:secret@localhost",
        "http://localhost/admin",
    ] {
        let response = router(AppState::new(vec![]))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/access/connections")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"protocol":"s3", "origin":origin}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{origin}");
    }
}

#[tokio::test]
async fn group0_data_mutations_are_rejected_before_leader_resolution() {
    for operation in ["put", "delete"] {
        let response = router(AppState::new(vec![]))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/stores/0/groups/0/kv/{operation}"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"key":"key","value":"value"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn chunk_query_rejects_invalid_ids_and_unbounded_pages() {
    for path in [
        "/api/chunks?limit=257",
        "/api/chunks?prefix=not-hex",
        "/api/chunks?after=abc",
        "/api/chunks/abc",
    ] {
        let response = router(AppState::new(vec![]))
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
}
