// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;
use std::sync::Arc;

#[path = "common/rpc_response_proxy.rs"]
mod rpc_response_proxy;

use axum::routing::{get, post};
use axum::{Json, Router};
use crowdb_console_shared::ops::{kv_logical, OpContext};
use crowdb_console_shared::ConsoleConfig;
use crowdb_protocol::common::KvServerIdentity;
use crowdb_test_harness::cluster::KvCluster;
use serde_json::json;

struct TestServer(tokio::task::JoinHandle<()>);

impl Drop for TestServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn verify_store_race(winner: u64) {
    let cluster = KvCluster::start().await;
    let ctx = Arc::new(OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    ));
    let competing = ctx.clone();
    let app = Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route(
            "/stores",
            post(move || {
                let competing = competing.clone();
                async move {
                    // Another console publishes after the initial absence check.
                    competing.sysmd().add_store(77, &[winner]).await.unwrap();
                    Json(json!({"store_id": 77, "group_count": 0}))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let _server = TestServer(tokio::spawn(
        async move { axum::serve(listener, app).await.unwrap() },
    ));
    ctx.sysmd()
        .register_kv_server(
            KvServerIdentity {
                instance_id: 701,
                node_id: Some(701),
            },
            &endpoint,
            &[],
            &[],
            "ok",
            "/test-node",
        )
        .await
        .unwrap();
    let result = kv_logical::add_store(&ctx, 77, &[701]).await;
    if winner == 701 {
        result.unwrap();
    } else {
        assert!(
            matches!(result, Err(crowdb_console_shared::error::Error::Conflict { .. })),
            "a concurrent winner must not be overwritten: {result:?}"
        );
    }
    assert_eq!(
        ctx.sysmd().get_store(77).await.unwrap().unwrap().node_ids,
        vec![winner]
    );
}

#[tokio::test]
async fn concurrent_conflicting_store_publication_is_preserved() {
    verify_store_race(999).await;
}

#[tokio::test]
async fn concurrent_matching_store_publication_is_confirmed() {
    verify_store_race(701).await;
}

#[tokio::test]
async fn committed_store_is_confirmed_after_its_write_response_is_lost() {
    let cluster = KvCluster::start().await;
    let proxy = rpc_response_proxy::TestResponseProxy::start(cluster.group0_leader_endpoint.clone()).await;
    let ctx = OpContext::new(
        proxy.endpoint.clone(),
        vec![proxy.management_endpoint.clone()],
        ConsoleConfig::default(),
    );
    let armed = proxy.armed.clone();
    let app = Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route(
            "/stores",
            post(move || {
                armed.store(true, Ordering::SeqCst);
                async { Json(json!({"store_id": 77, "group_count": 0})) }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let _server = TestServer(tokio::spawn(
        async move { axum::serve(listener, app).await.unwrap() },
    ));
    ctx.sysmd()
        .register_kv_server(
            KvServerIdentity {
                instance_id: 701,
                node_id: Some(701),
            },
            &endpoint,
            &[],
            &[],
            "ok",
            "/test-node",
        )
        .await
        .unwrap();
    assert_eq!(kv_logical::add_store(&ctx, 77, &[701]).await.unwrap(), vec![701]);
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        ctx.sysmd().get_store(77).await.unwrap().unwrap().node_ids,
        vec![701]
    );
}
