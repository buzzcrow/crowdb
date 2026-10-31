// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use crowdb_console_shared::{deployment::registry::NodeRecord, ConsoleConfig};
use crowdb_web::{router, AppState};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use tower::ServiceExt;

#[tokio::test]
async fn a_preflight_review_is_not_admission_and_discard_is_operation_fenced_and_readonly_guarded() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-preflight-api");
    let root = directory.path().join("console");
    std::fs::create_dir(&root).unwrap();
    let discovery = uuid::Uuid::new_v4().to_string();
    let operation = uuid::Uuid::new_v4().to_string();
    let record = NodeRecord {
        web_port: 9090,
        discovery_id: discovery.clone(),
        operation_id: operation.clone(),
        node_id: 1,
        physical_host_id: "owned-host".into(),
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 2222,
        ssh_user: "crowdb".into(),
        confirmed: false,
        host_key: None,
        cancelled: false,
    };
    let path = root.join(format!("node-preflight-{discovery}.json"));
    let bytes = serde_json::to_vec(&record).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let state = AppState::with_runtime_root(ConsoleConfig::default(), root)
        .with_node_monitor("http://127.0.0.1:1")
        .unwrap();
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/node/admissions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        json!([])
    );
    let uri = format!("/api/node/preflight/{discovery}");
    let discard = |operation: &str| {
        Request::builder()
            .method(Method::DELETE)
            .uri(&uri)
            .header("content-type", "application/json")
            .body(Body::from(json!({"operation_id":operation}).to_string()))
            .unwrap()
    };
    let response = app
        .clone()
        .oneshot(discard(&uuid::Uuid::new_v4().to_string()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let mut readonly = state;
    readonly.readonly = true;
    let response = router(readonly).oneshot(discard(&operation)).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    for _ in 0..2 {
        let response = app.clone().oneshot(discard(&operation)).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(!path.exists());
    }
    assert!(app
        .oneshot(
            Request::builder()
                .uri("/api/node/admissions")
                .body(Body::empty())
                .unwrap()
        )
        .await
        .unwrap()
        .status()
        .is_success());
    assert!(!directory.path().join("disks").exists());
    assert!(!directory.path().join("node-setup.json").exists());
}
