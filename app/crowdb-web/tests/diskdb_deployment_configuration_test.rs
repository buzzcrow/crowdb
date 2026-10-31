// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

#[test]
fn explicit_diskdb_ranges_keep_adjacent_host_instances_disjoint_and_preserve_legacy_inputs() {
    use crowdb_console_shared::config::NodeEntry;
    use crowdb_web::diskdb::{configuration_for_tests, DeployDiskdbBody};
    let node: NodeEntry = serde_json::from_value(json!({
        "id":1,"rack_id":1,"host":"127.0.0.1","ssh_port":2222,"ssh_user":"crowdb"
    }))
    .unwrap();
    let mut used = std::collections::BTreeSet::new();
    for offset in 0..3 {
        let body: DeployDiskdbBody = serde_json::from_value(json!({
            "listen_port":11000+offset,"http_port":11100+offset,
            "rpc_port":11200+offset,"rpc_listen_port":11200+offset
        }))
        .unwrap();
        let config = configuration_for_tests(&node, &["http://127.0.0.1:10000".into()], &body)
            .unwrap_or_else(|(_, body)| panic!("{}", body.error));
        let config: toml::Value = toml::from_str(&config).unwrap();
        for (field, base) in [
            ("listen_addr", 11000),
            ("http_listen_addr", 11100),
            ("rpc_listen_addr", 11200),
        ] {
            let address = config["server"][field].as_str().unwrap();
            assert_eq!(address, format!("127.0.0.1:{}", base + offset));
            assert!(used.insert(address.to_owned()));
        }
    }
    let legacy: DeployDiskdbBody = serde_json::from_value(json!({"rpc_port":7100})).unwrap();
    let config =
        configuration_for_tests(&node, &[], &legacy).unwrap_or_else(|(_, body)| panic!("{}", body.error));
    let config: toml::Value = toml::from_str(&config).unwrap();
    assert_eq!(
        config["server"]["rpc_listen_addr"].as_str(),
        Some("127.0.0.1:7102")
    );
}

async fn submit(body: Value) -> (StatusCode, String) {
    let state = AppState::default();
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nodes/1/diskdb/deploy")
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

#[tokio::test]
async fn invalid_heartbeat_cadence_is_rejected_before_node_execution() {
    let (status, body) = submit(json!({"rpc_port":7100,"keepalive_interval_secs":0})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(body.contains("nonzero"));
    for interval in [json!(-1), json!(4_294_967_296u64), json!("1")] {
        let (status, _) = submit(json!({"rpc_port":7100,"keepalive_interval_secs":interval})).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}

#[tokio::test]
async fn default_and_explicit_cadence_require_group0_before_starting_service() {
    for body in [
        json!({"rpc_port":7100}),
        json!({"rpc_port":7100,"keepalive_interval_secs":1}),
    ] {
        let (status, message) = submit(body).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(message.contains("Group 0 is not ready"));
    }
}
