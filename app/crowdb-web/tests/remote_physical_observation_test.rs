// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Remote process observations never enter the console's local PID namespace.

use axum::{body::Body, http::Request, routing::get, Json};
use crowdb_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry, ServerEntry};
use crowdb_web::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn read(app: &axum::Router, path: &str) -> Value {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn remote_topology_supplies_store_and_process_observations_without_local_pid() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/topology",
                get(|| async { Json(json!({"stores": [{"store_id": 7, "groups": []}]})) }),
            ),
        )
        .await
        .unwrap();
    });
    let mut config = ConsoleConfig::default();
    config
        .add_rack(RackEntry {
            id: 1,
            name: "remote".into(),
        })
        .unwrap();
    config
        .add_node(NodeEntry {
            id: 1,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 2222,
            ssh_user: "crowdb".into(),
            ssh_key: None,
            ssh_password: None,
            ssh_credential_ref: None,
        })
        .unwrap();
    let mut server = ServerEntry::new("kv-1", &origin);
    server.node_id = Some(1);
    config.add_server(server).unwrap();
    let state = AppState::with_config(config, None)
        .with_node_monitor(&origin)
        .unwrap();
    let app = router(state.clone());
    let stores = read(&app, "/api/nodes/1/stores").await;
    assert_eq!(stores[0]["store_id"], 7);
    let node = read(&app, "/api/nodes/1").await;
    assert_eq!(node["kv_server"]["state"], "running");
    assert_eq!(node["kv_server"]["health"], "up");
    assert!(node["kv_server"].get("pid").is_none());
    let racks = read(&app, "/api/racks?recursive=1").await;
    assert_eq!(racks["items"][0]["nodes"][0]["kv_server"]["state"], "running");
    assert!(state.kv_pid_snapshot().is_empty());

    upstream.abort();
    let _ = upstream.await;
    let node = read(&app, "/api/nodes/1").await;
    assert_eq!(node["kv_server"]["health"], "down");
    assert_eq!(node["kv_server"]["state"], "failed");
    assert!(state.kv_pid_snapshot().is_empty());
}
