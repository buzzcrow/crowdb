// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/memory_kv.rs"]
mod memory_kv;

use axum::{body::Body, http::Request};
use crowdb_console_shared::config::{ConsoleConfig, ServerEntry};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::mgmt::{
    node::{NodeBinding, NodeServiceAction, NodeServiceIntent},
    SystemBootstrapIdentity,
};
use crowdb_web::{router, AppState};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

#[tokio::test]
async fn kv_lifecycle_intents_preserve_admitted_endpoints_while_auxiliary_services_refresh() {
    let cluster = memory_kv::TestKvCluster::start().await;
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    )));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("remote-kv-projection");
    let binding = NodeBinding {
        node_id: 1,
        bootstrap: SystemBootstrapIdentity {
            cluster_id: "projection-cluster".into(),
            operation_id: "bootstrap".into(),
            configuration_digest: "a".repeat(64),
        },
        management_seeds: cluster.mgmt_endpoints.clone(),
    };
    std::fs::write(
        root.path().join("node-binding.json"),
        serde_json::to_vec(&binding).unwrap(),
    )
    .unwrap();
    let mut config = ConsoleConfig::default();
    let mut entry = ServerEntry::new("kv-1", &cluster.mgmt_endpoints[0]);
    entry.node_id = Some(1);
    config.add_server(entry).unwrap();
    publish_intents(&kv, &binding.bootstrap.cluster_id).await;
    let state = AppState::with_runtime_root(config, root.path().join("console"))
        .with_node_monitor("http://127.0.0.1:12345")
        .unwrap();
    *state.kv_client.write().await = Some(kv);
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/api/servers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
    let rows: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let kv = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "kv-1")
        .unwrap();
    assert_eq!(kv["service_type"], "paxos-kv");
    assert_eq!(kv["mgmt_url"], cluster.mgmt_endpoints[0]);
    assert!(rows
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["id"] == "chunkdb-1"));
}

async fn publish_intents(kv: &CrowdbKvClient, cluster_id: &str) {
    for (id, kind, configuration) in [
        ("kv-1", "kv", "node_id = 1\n"),
        (
            "chunkdb-1",
            "chunkdb",
            "[server]\nhttp_listen_addr = '127.0.0.1:1'\nrpc_listen_addr = '127.0.0.1:1'\n",
        ),
    ] {
        let intent = NodeServiceIntent {
            cluster_id: cluster_id.to_owned(),
            operation_id: "restart".into(),
            node_id: 1,
            service_id: id.into(),
            kind: kind.into(),
            action: NodeServiceAction::Restart,
            configuration: configuration.into(),
            environment: BTreeMap::new(),
        };
        kv.put_cas(
            0,
            0,
            format!("/deployment/services/1/{id}").as_bytes(),
            &serde_json::to_vec(&intent).unwrap(),
            0,
        )
        .await
        .unwrap();
    }
}
