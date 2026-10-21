// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_console_shared::{config::NodeEntry, ConsoleConfig};
use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};
use crowdb_web::{service_detail_for_tests, AppState};

#[tokio::test]
async fn service_evidence_identifies_the_current_instance_and_owned_range() {
    let config = ConsoleConfig {
        nodes: vec![NodeEntry {
            id: 1,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: "crowdb".into(),
            ssh_credential_ref: None,
            ssh_key: None,
            ssh_password: None,
        }],
        ..Default::default()
    };
    let state = AppState::with_runtime_root(config, std::path::PathBuf::from("/unused"));
    for instance in [7, 8] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/health", get(move || async move { Json(serde_json::json!({
            "instance_id": instance,
            "detail": "Instance Serving; catalog generation 9; 1 owned tree range, 0 ready",
            "ranges": [{"partition_id":{"high":1,"low":2},"owner_epoch":4,"ready":false,"detail":"Owned range writer is Prepared; waiting for recovery and activation"}]
        })) }));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = NodeServiceIntent {
            cluster_id: uuid::Uuid::new_v4().to_string(),
            node_id: 1,
            service_id: "chunk-kv-1".into(),
            kind: "chunk-kv".into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            action: NodeServiceAction::Start,
            configuration: format!("instance_id = 7\nhttp_listen_addr = \"{address}\"\n"),
            environment: std::collections::BTreeMap::new(),
        };
        let result = service_detail_for_tests(&state, &intent).await;
        if instance == 7 {
            let detail = result.unwrap();
            assert!(detail.contains("catalog generation 9"));
            assert!(detail.contains("Prepared"));
            assert!(detail.contains("owner_epoch"));
        } else {
            assert!(result
                .unwrap_err()
                .contains("differs from current committed intent"));
        }
        task.abort();
    }
}
