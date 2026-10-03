// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::any;
use crowdb_console_shared::config::{ServerEntry, ServiceType};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use tower::ServiceExt;

#[tokio::test]
async fn native_deployment_resolves_cluster_origins_and_private_reader() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("native-access-origin");
    crowdb_monitor::ServerCredentials::load_or_create(root.path()).unwrap();
    let upstream = axum::Router::new().route(
        "/*path",
        any(|request: Request<Body>| async move {
            assert!(request.headers()["authorization"]
                .to_str()
                .unwrap()
                .starts_with("Bearer "));
            (StatusCode::OK, "configured-catalog")
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("access-server-1", origin.clone());
    entry.service_type = ServiceType::AccessServer;
    config.servers.push(entry);
    config.local_launches.insert(
        "access-server-1".into(),
        crowdb_console_shared::config::LocalLaunchSpec {
            program: "unused".into(),
            args: vec![],
            workdir: root.path().to_string_lossy().into_owned(),
            env: std::collections::BTreeMap::from([
                ("CROWDB_ICEBERG_PUBLIC_URI".into(), origin.clone()),
                ("CROWDB_S3_PUBLIC_URI".into(), origin.clone()),
            ]),
            env_file: Some(
                root.path()
                    .join("secrets/server.env")
                    .to_string_lossy()
                    .into_owned(),
            ),
            readiness_url: None,
        },
    );
    let app = router(AppState::with_runtime_root(config, root.path().to_owned()));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/access/connections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(metadata["iceberg"], origin);
    assert_eq!(metadata["s3"], origin);
    assert_eq!(metadata["iceberg_ready"], true);
    assert!(!String::from_utf8_lossy(&bytes).contains("TOKEN"));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/access/iceberg/v1/namespaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    task.abort();
}

fn configured(origin: &str) -> axum::Router {
    let mut config = ConsoleConfig::default();
    for protocol in ["s3", "iceberg"] {
        let mut entry = ServerEntry::new(format!("console-access-{protocol}"), origin.to_string());
        entry.service_type = ServiceType::AccessServer;
        entry.auto_start = false;
        config.servers.push(entry);
    }
    router(
        AppState::with_config(config, None)
            .with_iceberg_reader("r".repeat(32))
            .unwrap(),
    )
}

#[tokio::test]
async fn cluster_catalog_uses_server_reader_without_granting_mutation_access() {
    let upstream = axum::Router::new().route(
        "/*path",
        any(|request: Request<Body>| async move {
            if request.method() == "GET" {
                assert_eq!(
                    request.headers()["authorization"],
                    format!("Bearer {}", "r".repeat(32))
                );
                (StatusCode::OK, "catalog")
            } else {
                assert!(!request.headers().contains_key("authorization"));
                (StatusCode::UNAUTHORIZED, "authentication required")
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = configured(&format!("http://{}", listener.local_addr().unwrap()));
    let task = tokio::spawn(async move {
        axum::serve(listener, upstream).await.unwrap();
    });
    for (method, expected) in [("GET", StatusCode::OK), ("POST", StatusCode::UNAUTHORIZED)] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/api/access/iceberg/v1/namespaces")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/access/connections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(metadata["iceberg_ready"], true);
    assert!(!String::from_utf8_lossy(&bytes).contains(&"r".repeat(32)));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/access/connections")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"protocol":"iceberg","origin":"http://untrusted.example"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    task.abort();
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
