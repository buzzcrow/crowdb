// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use crowdb_console_shared::ops::{kv_logical, OpContext};
use crowdb_console_shared::ConsoleConfig;
use crowdb_protocol::common::KvServerIdentity;
use crowdb_test_harness::cluster::KvCluster;
use serde_json::json;

#[derive(Clone)]
struct TestNodeState {
    created: Arc<AtomicBool>,
    reject_wiring: bool,
    omit_endpoint: bool,
}

struct TestNode {
    endpoint: String,
    state: TestNodeState,
    server: tokio::task::JoinHandle<()>,
}

impl TestNode {
    async fn start(reject_wiring: bool, omit_endpoint: bool) -> Self {
        let state = TestNodeState {
            created: Arc::new(AtomicBool::new(false)),
            reject_wiring,
            omit_endpoint,
        };
        let app = Router::new()
            .route(
                "/stores/:sid/groups",
                post(|State(state): State<TestNodeState>| async move {
                    state.created.store(true, Ordering::SeqCst);
                    StatusCode::CREATED
                }),
            )
            .route(
                "/stores/:sid/groups/:gid",
                delete(|State(state): State<TestNodeState>| async move {
                    state.created.store(false, Ordering::SeqCst);
                    StatusCode::NO_CONTENT
                }),
            )
            .route(
                "/stores/:sid/groups/:gid/remotes",
                post(|State(state): State<TestNodeState>| async move {
                    if state.reject_wiring {
                        StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        StatusCode::OK
                    }
                }),
            )
            .route(
                "/topology",
                get(|State(state): State<TestNodeState>| async move {
                    let endpoint = (!state.omit_endpoint).then_some("127.0.0.1:10100");
                    Json(json!({"stores": [{"store_id": 77, "listen_addr": endpoint, "groups": []}]}))
                }),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            endpoint,
            state,
            server,
        }
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn verify_rejected_wiring(reject_wiring: bool, omit_endpoint: bool) {
    let cluster = KvCluster::start().await;
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    let first = TestNode::start(false, false).await;
    let second = TestNode::start(reject_wiring, omit_endpoint).await;
    for (node, server) in [(701, &first), (702, &second)] {
        ctx.sysmd()
            .register_kv_server(
                KvServerIdentity {
                    instance_id: node,
                    node_id: Some(node),
                },
                &server.endpoint,
                &[77],
                &[],
                "ok",
                "/test-node",
            )
            .await
            .unwrap();
    }
    ctx.sysmd().add_store(77, &[701, 702]).await.unwrap();
    let result = kv_logical::add_group(&ctx, 77, 7, 700, &[701, 702]).await;
    assert!(result.is_err(), "incomplete peer wiring must not report success");
    assert!(!first.state.created.load(Ordering::SeqCst));
    assert!(!second.state.created.load(Ordering::SeqCst));
    assert!(ctx.sysmd().get_group(77, 7).await.unwrap().is_none());
    assert!(ctx
        .sysmd()
        .list_replicas_in_group(77, 7)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn peer_wiring_failure_rolls_back_without_publishing_membership() {
    verify_rejected_wiring(true, false).await;
}

#[tokio::test]
async fn missing_peer_endpoint_rolls_back_without_publishing_membership() {
    verify_rejected_wiring(false, true).await;
}

async fn verify_replica_discovery_failure(register_peer: bool, omit_peer: bool, omit_target: bool) {
    let cluster = KvCluster::start().await;
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    let peer = TestNode::start(false, omit_peer).await;
    let target = TestNode::start(false, omit_target).await;
    for (node, server) in [(701, &peer), (702, &target)] {
        if node == 701 && !register_peer {
            continue;
        }
        ctx.sysmd()
            .register_kv_server(
                KvServerIdentity {
                    instance_id: node,
                    node_id: Some(node),
                },
                &server.endpoint,
                &[77],
                &[],
                "ok",
                "/test-node",
            )
            .await
            .unwrap();
    }
    ctx.sysmd().add_store(77, &[701, 702]).await.unwrap();
    ctx.sysmd().add_group(77, 7).await.unwrap();
    ctx.sysmd()
        .add_replica(&crowdb_protocol::common::ReplicaValue {
            store_id: 77,
            group_id: 7,
            replica_id: 700,
            node_id: 701,
            role: String::new(),
            voting: true,
            endpoint: String::new(),
        })
        .await
        .unwrap();
    let result = kv_logical::add_replica(&ctx, 77, 7, 702, Some(701)).await;
    assert!(
        result.is_err(),
        "incomplete replica discovery must not report success"
    );
    assert!(
        !target.state.created.load(Ordering::SeqCst),
        "failed creation must not leave a local group"
    );
    let members = ctx.sysmd().list_replicas_in_group(77, 7).await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].replica_id, 700);
}

#[tokio::test]
async fn replica_creation_requires_every_existing_peer_registration() {
    verify_replica_discovery_failure(false, false, false).await;
}

#[tokio::test]
async fn replica_creation_requires_every_existing_peer_endpoint() {
    verify_replica_discovery_failure(true, true, false).await;
}

#[tokio::test]
async fn replica_creation_rolls_back_when_new_endpoint_is_missing() {
    verify_replica_discovery_failure(true, false, true).await;
}
