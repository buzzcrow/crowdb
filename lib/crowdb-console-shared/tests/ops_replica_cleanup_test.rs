// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use crowdb_console_shared::ops::{kv_logical, OpContext};
use crowdb_console_shared::ConsoleConfig;
use crowdb_protocol::common::{KvServerIdentity, ReplicaValue};
use crowdb_test_harness::cluster::KvCluster;
use serde_json::json;

#[derive(Clone)]
struct TestState {
    store: Arc<AtomicBool>,
    deletes: Arc<AtomicUsize>,
    reject_cleanup: bool,
}

struct TestNode {
    state: TestState,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn target(ctx: &OpContext, reject_cleanup: bool) -> TestNode {
    let state = TestState {
        store: Arc::new(AtomicBool::new(false)),
        deletes: Arc::new(AtomicUsize::new(0)),
        reject_cleanup,
    };
    let app = Router::new()
        .route(
            "/stores",
            post(|State(state): State<TestState>| async move {
                state.store.store(true, Ordering::SeqCst);
                (
                    StatusCode::CREATED,
                    Json(json!({"store_id": 77, "group_count": 0})),
                )
            }),
        )
        .route(
            "/stores/:sid",
            delete(|State(state): State<TestState>| async move {
                state.deletes.fetch_add(1, Ordering::SeqCst);
                if state.reject_cleanup {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    state.store.store(false, Ordering::SeqCst);
                    StatusCode::NO_CONTENT
                }
            }),
        )
        .route(
            "/stores/:sid/groups",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/topology",
            get(|| async {
                Json(json!({"stores": [{"store_id": 77, "listen_addr": "127.0.0.1:10100", "groups": []}]}))
            }),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for node_id in [701, 702] {
        ctx.sysmd()
            .register_kv_server(
                KvServerIdentity {
                    instance_id: node_id,
                    node_id: Some(node_id),
                },
                &endpoint,
                &[77],
                &[],
                "ok",
                "/test-node",
            )
            .await
            .unwrap();
    }
    TestNode { state, server }
}

async fn seed(ctx: &OpContext, replica_id: u64) {
    ctx.sysmd().add_store(77, &[701]).await.unwrap();
    ctx.sysmd().add_group(77, 7).await.unwrap();
    ctx.sysmd()
        .add_replica(&ReplicaValue {
            store_id: 77,
            group_id: 7,
            replica_id,
            node_id: 701,
            role: String::new(),
            voting: true,
            endpoint: String::new(),
        })
        .await
        .unwrap();
}

async fn verify_cleanup(reject_cleanup: bool) {
    let cluster = KvCluster::start().await;
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    let node = target(&ctx, reject_cleanup).await;
    seed(&ctx, 700).await;
    let error = kv_logical::add_replica(&ctx, 77, 7, 702, Some(701))
        .await
        .unwrap_err();
    assert_eq!(
        node.state.deletes.load(Ordering::SeqCst),
        1,
        "failed group creation must clean its new store: {error}"
    );
    assert_eq!(node.state.store.load(Ordering::SeqCst), reject_cleanup);
    if reject_cleanup {
        assert!(
            error.to_string().contains("rollback incomplete"),
            "cleanup failure must be visible: {error}"
        );
    }
    assert_eq!(ctx.sysmd().list_replicas_in_group(77, 7).await.unwrap().len(), 1);
}

#[tokio::test]
async fn failed_group_creation_removes_new_store() {
    verify_cleanup(false).await;
}

#[tokio::test]
async fn failed_rollback_is_reported() {
    verify_cleanup(true).await;
}

#[tokio::test]
async fn automatic_replica_identity_exhaustion_is_validation_error() {
    let cluster = KvCluster::start().await;
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    seed(&ctx, u64::MAX).await;
    let error = kv_logical::add_replica(&ctx, 77, 7, 702, None).await.unwrap_err();
    assert!(matches!(
        error,
        crowdb_console_shared::error::Error::Validation { .. }
    ));
}

#[tokio::test]
async fn existing_replica_host_is_rejected_before_local_mutation() {
    let cluster = KvCluster::start().await;
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    let node = target(&ctx, false).await;
    seed(&ctx, 700).await;
    let error = kv_logical::add_replica(&ctx, 77, 7, 701, Some(701))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crowdb_console_shared::error::Error::Conflict { .. }
    ));
    assert!(!node.state.store.load(Ordering::SeqCst));
    assert_eq!(node.state.deletes.load(Ordering::SeqCst), 0);
}
