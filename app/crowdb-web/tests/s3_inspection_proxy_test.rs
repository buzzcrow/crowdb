// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::body::Body;
use axum::extract::Query;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use crowdb_console_shared::config::{ServerEntry, ServiceType};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use std::collections::HashMap;
use tower::ServiceExt;

async fn upstream(
    Query(query): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> (StatusCode, String) {
    let authorization = headers["authorization"].to_str().unwrap();
    assert!(authorization.starts_with("Bearer "));
    assert_ne!(authorization, "Bearer browser-token");
    assert_eq!(query["bucket"], "bucket");
    match query["key"].as_str() {
        "large" => (
            StatusCode::OK,
            format!("{{\"body\":\"{}\"}}", "x".repeat(1024 * 1024)),
        ),
        "corrupt" => (StatusCode::OK, "not JSON".into()),
        "stale" => (
            StatusCode::CONFLICT,
            r#"{"error":"Object generation changed"}"#.into(),
        ),
        "missing" => (StatusCode::NOT_FOUND, r#"{"error":"Object missing"}"#.into()),
        "folder/中文.bin" => {
            assert_eq!(query["limit"], "20");
            assert_eq!(query["cursor"], "opaque+/=");
            (StatusCode::OK, r#"{"offset":"9007199254740993"}"#.into())
        }
        _ => panic!("invalid inputs must not reach upstream"),
    }
}

#[tokio::test]
async fn inspection_proxy_uses_private_credentials_and_bounds_upstream_responses() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("s3-inspection-proxy");
    crowdb_monitor::ServerCredentials::load_or_create(root.path()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let service = axum::Router::new().route("/_crowdb/admin/object-locations", get(upstream));
    let server = tokio::spawn(async move { axum::serve(listener, service).await.unwrap() });
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("console-access-s3", origin);
    entry.service_type = ServiceType::AccessServer;
    entry.auto_start = false;
    config.servers.push(entry);
    let app = router(AppState::with_runtime_root(config, root.path().to_owned()));
    for (key, suffix, status, expected) in [
        (
            "folder%2F%E4%B8%AD%E6%96%87.bin",
            "&cursor=opaque%2B%2F%3D",
            StatusCode::OK,
            "9007199254740993",
        ),
        ("stale", "", StatusCode::CONFLICT, "Object generation changed"),
        ("missing", "", StatusCode::NOT_FOUND, "Object missing"),
        ("large", "", StatusCode::BAD_GATEWAY, "1 MiB limit"),
        (
            "corrupt",
            "",
            StatusCode::BAD_GATEWAY,
            "Invalid object inspection response",
        ),
        (
            "unreachable",
            "&limit=101",
            StatusCode::BAD_REQUEST,
            "Invalid object inspection scope",
        ),
        (
            "unreachable",
            "&origin=http://untrusted.example",
            StatusCode::BAD_REQUEST,
            "unknown field",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/access/s3-inspect/locations?bucket=bucket&key={key}{suffix}"
                    ))
                    .header("authorization", "Bearer browser-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{key}");
        let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains(expected), "{body}");
        assert!(!body.contains("Bearer "));
    }
    server.abort();
    let _ = server.await;
}
