// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use axum::{body::Body, http::Request};
use crowdb_console_shared::{
    config::{ServerEntry, ServiceType},
    ConsoleConfig,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient};
use crowdb_protocol::diskdb::rpc::DiskGroupValue;
use crowdb_web::{router, AppState};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

async fn readiness(app: &axum::Router) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/chunk-storage-readiness")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());
    serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 65536).await.unwrap()).unwrap()
}

#[tokio::test]
async fn chunk_storage_requires_live_complete_ownership_even_when_diskio_pid_is_alive() {
    let cluster = crowdb_test_harness::cluster::KvCluster::start().await;
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let hardware = HardwareClient::from_shared(kv.clone());
    for id in [11, 12] {
        hardware
            .add_disk_group(1, 1, id, &DiskGroupValue::default())
            .await
            .unwrap();
    }
    let registry = ServiceRegistryClient::from_shared(kv.clone());
    let mut config = ConsoleConfig::default();
    let endpoint = "127.0.0.1:13010";
    let mut server = ServerEntry::new("diskio-1", "http://127.0.0.1:1");
    server.service_type = ServiceType::Diskio;
    server.node_id = Some(1);
    server.pid = Some(std::process::id());
    server.rpc_url = Some(format!("http://{endpoint}"));
    config.servers.push(server);
    let state = AppState::with_config(config, None);
    *state.kv_client.write().await = Some(kv);
    let app = router(state);
    assert_eq!(readiness(&app).await["ready"], false);
    registry.heartbeat_diskio(1, endpoint, &[11], &[]).await.unwrap();
    assert_eq!(readiness(&app).await["ready"], false);
    registry
        .heartbeat_diskio(1, endpoint, &[11, 12], &[])
        .await
        .unwrap();
    assert_eq!(readiness(&app).await["ready"], true);
    registry.unregister("diskio", 1).await.unwrap();
    assert_eq!(readiness(&app).await["ready"], false);
}
