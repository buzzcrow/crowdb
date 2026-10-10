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
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({"error": String::from_utf8_lossy(&bytes)})),
    )
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
    let kinds = [
        "paxos-kv",
        "diskdb",
        "chunkdb",
        "diskio",
        "chunk-kv",
        "access-server",
    ];
    let steps: serde_json::Map<_, _> = kinds
        .iter()
        .map(|kind| ((*kind).to_string(), json!({"state":"waiting"})))
        .collect();
    let body = json!({"revision":0,"steps":steps,"overrides":{"access-server":{"http_port":9092,"s3_port":9091,"health_port":9094},"paxos-kv":{"http_port":19910,"rpc_port":19920}}});
    let (status, saved) = request(&app, "PUT", "/api/nodes/1/service-plan", body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["revision"], 1);
    let (_, defaults) = request(&app, "GET", "/api/deployment-defaults", Value::Null).await;
    for value in defaults.as_object().unwrap().values() {
        for field in ["http_port", "rpc_port", "s3_port", "health_port"] {
            if let Some(port) = value[field].as_u64() {
                assert!(![9091, 9092, 9094, 19910, 19920].contains(&port), "{defaults}");
            }
        }
    }
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
    invalid["steps"]["paxos-kv"]["detail"] = json!("x".repeat(4097));
    let (status, _) = request(&app, "PUT", "/api/nodes/1/service-plan", invalid).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    state.clear_workspaces().unwrap();
    let (_, plans) = request(&app, "GET", "/api/service-plans", Value::Null).await;
    assert_eq!(plans, json!({}));
}

#[tokio::test]
async fn plan_rejects_legacy_kinds_and_invalid_listener_configuration() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("service-plan-validation");
    let app = router(AppState::with_runtime_root(config(), root.path().to_owned()));
    let steps: serde_json::Map<_, _> = [
        "access-server",
        "chunk-kv",
        "chunkdb",
        "diskdb",
        "diskio",
        "paxos-kv",
    ]
    .into_iter()
    .map(|kind| (kind.into(), json!({"state":"disabled"})))
    .collect();
    for overrides in [
        json!({"access-server":{"s3_port":9091,"health_port":9091}}),
        json!({"access-server":{"health_port":0}}),
        json!({"diskio":{"http_port":13010}}),
        json!({"diskdb":{"rpc_port":65534}}),
        json!({"diskdb":{"rpc_port":12000},"diskio":{"rpc_port":12001}}),
        json!({"chunkdb":{"health_port":9093}}),
        json!({"kv":{"rpc_port":19920}}),
        json!({"access-server":{"instance_id":"1"}}),
    ] {
        let (status, response) = request(
            &app,
            "PUT",
            "/api/nodes/1/service-plan",
            json!({"revision":0,"steps":steps,"overrides":overrides}),
        )
        .await;
        assert!(status.is_client_error(), "{response}");
    }
    let mut legacy = steps;
    let step = legacy.remove("paxos-kv").unwrap();
    legacy.insert("kv".into(), step);
    let (status, _) = request(
        &app,
        "PUT",
        "/api/nodes/1/service-plan",
        json!({"revision":0,"steps":legacy}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
