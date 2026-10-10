// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use crowdb_console_shared::{
    config::ConsoleConfig,
    ops::{cluster::init_prepared, OpContext},
};
use crowdb_protocol::mgmt::{SystemBootstrapIdentity, SystemPrepareRequest};
use serde_json::json;

#[derive(Clone)]
struct TestState {
    reject: bool,
    prepares: Arc<AtomicUsize>,
    starts: Arc<AtomicUsize>,
}

async fn prepare(
    State(state): State<TestState>,
    Json(request): Json<SystemPrepareRequest>,
) -> Result<Json<SystemPrepareRequest>, StatusCode> {
    state.prepares.fetch_add(1, Ordering::Relaxed);
    if state.reject {
        Err(StatusCode::CONFLICT)
    } else {
        Ok(Json(request))
    }
}

async fn start(State(state): State<TestState>) -> StatusCode {
    state.starts.fetch_add(1, Ordering::Relaxed);
    StatusCode::INTERNAL_SERVER_ERROR
}

#[tokio::test]
async fn one_rejected_prepare_prevents_creation_on_every_selected_member() {
    let mut servers = Vec::new();
    let mut states = Vec::new();
    let mut config = ConsoleConfig::default();
    for index in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = TestState {
            reject: index == 1,
            prepares: Arc::default(),
            starts: Arc::default(),
        };
        let router = Router::new()
            .route("/system/prepare", post(prepare))
            .route("/system/init", post(start))
            .with_state(state.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
        states.push(state);
        config.nodes.push(
            serde_json::from_value(json!({
                "id": index + 1, "rack_id": 1, "host": "127.0.0.1"
            }))
            .unwrap(),
        );
        config.servers.push(
            serde_json::from_value(json!({
                "id": format!("node-{index}"), "node_id": index + 1, "url": endpoint
            }))
            .unwrap(),
        );
    }
    let ctx = OpContext::new("127.0.0.1:1".into(), vec![], config);
    let identity = SystemBootstrapIdentity {
        cluster_id: "12345678-1234-4234-8234-123456789abc".into(),
        operation_id: "22345678-1234-4234-8234-123456789abc".into(),
        configuration_digest: "a".repeat(64),
    };
    assert!(init_prepared(&ctx, &[1, 2], &identity).await.is_err());
    for state in &states {
        assert_eq!(state.prepares.load(Ordering::Relaxed), 1);
        assert_eq!(state.starts.load(Ordering::Relaxed), 0);
    }
    assert!(init_prepared(&ctx, &[1, 1], &identity).await.is_err());
    assert!(init_prepared(&ctx, &[], &identity).await.is_err());
    for state in &states {
        assert_eq!(state.prepares.load(Ordering::Relaxed), 1);
    }
    for server in servers {
        server.abort();
    }
}
