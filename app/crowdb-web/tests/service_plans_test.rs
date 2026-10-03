// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_console_shared::{
    config::{NodeEntry, RackEntry},
    ConsoleConfig,
};
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn request(app: &axum::Router, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn config() -> ConsoleConfig {
    let mut config = ConsoleConfig::default();
    config
        .add_rack(RackEntry {
            id: 1,
            name: "Rack".into(),
        })
        .unwrap();
    config
        .add_node(NodeEntry {
            id: 1,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
            ssh_credential_ref: None,
        })
        .unwrap();
    config
}

#[tokio::test]
async fn durable_plan_survives_state_recreation_and_rejects_competing_writers() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("service-plan");
    let state = AppState::with_runtime_root(config(), root.path().to_owned());
    let app = router(state.clone());
    let kinds = ["kv", "diskdb", "chunkdb", "diskio", "chunk-kv", "access-server"];
    let steps: serde_json::Map<_, _> = kinds
        .iter()
        .map(|kind| ((*kind).to_string(), json!({"state":"waiting"})))
        .collect();
    let body = json!({"revision":0,"steps":steps});
    let (status, saved) = request(&app, "PUT", "/api/nodes/1/service-plan", body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["revision"], 1);
    let (status, _) = request(&app, "PUT", "/api/nodes/1/service-plan", body.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let recovered = router(AppState::with_runtime_root(config(), root.path().to_owned()));
    let (status, plans) = request(&recovered, "GET", "/api/service-plans", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(plans["1"], saved);
    let mut changed = saved;
    changed["steps"]["chunkdb"]["state"] = json!("deploying");
    let (status, saved) = request(&recovered, "PUT", "/api/nodes/1/service-plan", changed).await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["revision"], 2);
    let (status, _) = request(&app, "PUT", "/api/nodes/2/service-plan", body.clone()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let mut invalid = body;
    invalid["revision"] = json!(2);
    invalid["steps"]["kv"]["detail"] = json!("x".repeat(4097));
    let (status, _) = request(&app, "PUT", "/api/nodes/1/service-plan", invalid).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    state.clear_workspaces().unwrap();
    let (_, plans) = request(&app, "GET", "/api/service-plans", Value::Null).await;
    assert_eq!(plans, json!({}));
}
