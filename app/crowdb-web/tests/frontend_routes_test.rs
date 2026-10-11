// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Missing-UI routing remains a handler proof; shipped SPA routes use release acceptance.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn missing_ui_fallback_preserves_api_health_and_repository_boundaries() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("missing-console-ui");
    let mut state = crowdb_web::AppState::default();
    state.ui_root = std::sync::Arc::new(directory.path().to_owned());
    let app = crowdb_web::router(state);
    for path in ["/", "/some/spa/route", "/../Cargo.toml"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(response.status().is_success() || response.status() == StatusCode::BAD_REQUEST);
        let body = String::from_utf8(
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        if path != "/../Cargo.toml" {
            assert!(body.contains("UI not built"));
            assert!(body.contains("make ui-build"));
        }
        assert!(!body.contains("[package]") && !body.contains("[workspace]"));
    }
    let health = app
        .clone()
        .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(to_bytes(health.into_body(), 1024 * 1024).await.unwrap(), "ok");
    let racks = app
        .oneshot(Request::builder().uri("/api/racks").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(racks.status().is_success());
    assert!(racks.headers()[axum::http::header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("application/json"));
}
