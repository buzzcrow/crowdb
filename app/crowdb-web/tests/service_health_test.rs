// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{body::Body, http::Request};
use crowdb_console_shared::{
    config::{LocalLaunchSpec, ServerEntry, ServiceType},
    ConsoleConfig,
};
use crowdb_rpc_ffi::RpcServer;
use crowdb_web::{router, AppState};
use serde_json::Value;
use tower::ServiceExt;

async fn servers(app: &axum::Router) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/servers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 65536).await.unwrap()).unwrap()
}

#[tokio::test]
async fn internal_health_uses_rpc_and_disk_failures_do_not_change_paxos_health() {
    let mut config = ConsoleConfig::default();
    let mut listeners = Vec::new();
    for (id, kind) in [
        ("paxos-kv-1", ServiceType::PaxosKv),
        ("diskdb-1", ServiceType::Diskdb),
        ("diskio-1", ServiceType::Diskio),
    ] {
        let rpc = RpcServer::with_engines(None, 1, 1);
        rpc.listen("127.0.0.1", 0).unwrap();
        rpc.start();
        let mut entry = ServerEntry::new(id, "http://127.0.0.1:1");
        entry.service_type = kind;
        entry.node_id = Some(1);
        entry.pid = Some(std::process::id());
        entry.rpc_url = Some(format!("127.0.0.1:{}", rpc.port()));
        config.servers.push(entry);
        listeners.push(rpc);
    }
    config.stores.push(crowdb_console_shared::config::StoreEntry {
        store_id: 0,
        nodes: vec![1],
    });
    let app = router(AppState::with_config(config, None));
    let initial = servers(&app).await;
    for row in initial.as_array().unwrap() {
        assert_eq!(row["health"], "up", "{row}");
    }
    listeners[1].stop();
    listeners[2].stop();
    let stopped = servers(&app).await;
    assert_eq!(stopped[0]["health"], "up");
    assert_eq!(stopped[1]["health"], "down");
    assert_eq!(stopped[2]["health"], "down");
    // The PIDs are still alive; they cannot override failed RPC probes.
    assert!(stopped[1]["pid"].is_number());
    listeners[0].stop();
    assert_eq!(servers(&app).await[0]["health"], "down");
}

#[tokio::test]
async fn access_health_probes_the_dedicated_listener_and_marks_legacy_launch_unknown() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new().route("/_crowdb/health/ready", axum::routing::get(|| async { "ready" }));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("access-server-1", "http://127.0.0.1:1");
    entry.service_type = ServiceType::AccessServer;
    entry.pid = Some(std::process::id());
    config.servers.push(entry);
    config.local_launches.insert(
        "access-server-1".into(),
        LocalLaunchSpec {
            env: [
                ("CROWDB_ACCESS_HEALTH_LISTEN".into(), address.to_string()),
                ("CROWDB_S3_PUBLIC_URI".into(), "http://127.0.0.1:1".into()),
            ]
            .into(),
            readiness_url: Some(format!("http://{address}/_crowdb/health/ready")),
            ..Default::default()
        },
    );
    let state = AppState::with_config(config, None);
    let app = router(state.clone());
    assert_eq!(servers(&app).await[0]["health"], "up");
    task.abort();
    let _ = task.await;
    assert_eq!(servers(&app).await[0]["health"], "down");
    state
        .config
        .write()
        .unwrap()
        .local_launches
        .get_mut("access-server-1")
        .unwrap()
        .env
        .remove("CROWDB_ACCESS_HEALTH_LISTEN");
    assert_eq!(servers(&app).await[0]["health"], "unknown");
}

#[tokio::test]
async fn prebootstrap_paxos_health_uses_liveness_only_until_a_store_exists() {
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("paxos-kv-1", "http://127.0.0.1:1");
    entry.node_id = Some(1);
    entry.pid = Some(std::process::id());
    entry.rpc_url = Some("127.0.0.1:1".into());
    config.servers.push(entry);
    let state = AppState::with_config(config, None);
    let app = router(state.clone());
    assert_eq!(servers(&app).await[0]["health"], "up");
    state
        .config
        .write()
        .unwrap()
        .stores
        .push(crowdb_console_shared::config::StoreEntry {
            store_id: 0,
            nodes: vec![1],
        });
    assert_eq!(servers(&app).await[0]["health"], "down");
}
