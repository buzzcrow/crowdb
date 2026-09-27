// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::delete;
use axum::Router;
use crowdb_console_shared::ops::{kv_logical, OpContext};
use crowdb_console_shared::ConsoleConfig;
use crowdb_protocol::common::{KvServerIdentity, ReplicaValue};
use crowdb_test_harness::cluster::KvCluster;

#[path = "common/rpc_response_proxy.rs"]
mod rpc_response_proxy;

struct TestNode {
    calls: Arc<AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn register_node(
    ctx: &OpContext,
    node_id: u64,
    status: StatusCode,
    arm: Option<Arc<AtomicBool>>,
) -> TestNode {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let handler = move || {
        handler_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(arm) = &arm {
            arm.store(true, Ordering::SeqCst);
        }
        async move { status }
    };
    let app = Router::new()
        .route("/stores/:sid", delete(handler.clone()))
        .route("/stores/:sid/groups/:gid", delete(handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
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
    TestNode { calls, server }
}

async fn seed(ctx: &OpContext) {
    // Node 702 acquired its store through a later replica addition.
    ctx.sysmd().add_store(77, &[701]).await.unwrap();
    for group_id in [7, 8] {
        ctx.sysmd().add_group(77, group_id).await.unwrap();
        for node_id in [701, 702] {
            ctx.sysmd()
                .add_replica(&ReplicaValue {
                    store_id: 77,
                    group_id,
                    replica_id: node_id,
                    node_id,
                    role: String::new(),
                    voting: true,
                    endpoint: String::new(),
                })
                .await
                .unwrap();
        }
    }
}

async fn verify_deletion(store: bool, failure: bool, drop_reply: bool) {
    let cluster = KvCluster::start().await;
    let proxy = if drop_reply {
        Some(rpc_response_proxy::TestResponseProxy::start(cluster.group0_leader_endpoint.clone()).await)
    } else {
        None
    };
    let ctx = OpContext::new(
        proxy.as_ref().map_or_else(
            || cluster.group0_leader_endpoint.clone(),
            |proxy| proxy.endpoint.clone(),
        ),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    // A retried deletion may find the first node already absent.
    let first = register_node(&ctx, 701, StatusCode::NOT_FOUND, None).await;
    let second = register_node(
        &ctx,
        702,
        if failure {
            StatusCode::INTERNAL_SERVER_ERROR
        } else {
            StatusCode::NO_CONTENT
        },
        proxy.as_ref().map(|proxy| proxy.armed.clone()),
    )
    .await;
    seed(&ctx).await;
    let result = if store {
        kv_logical::remove_store(&ctx, 77).await
    } else {
        kv_logical::remove_group(&ctx, 77, 7).await
    };
    assert_eq!(result.is_err(), failure, "unexpected deletion result: {result:?}");
    if let Some(proxy) = &proxy {
        assert_eq!(proxy.dropped.load(Ordering::SeqCst), 1);
    }
    assert_eq!(first.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        ctx.sysmd().get_store(77).await.unwrap().is_some(),
        failure || !store
    );
    for group_id in [7, 8] {
        let retained = failure || (!store && group_id == 8);
        assert_eq!(
            ctx.sysmd().get_group(77, group_id).await.unwrap().is_some(),
            retained
        );
        assert_eq!(
            ctx.sysmd()
                .list_replicas_in_group(77, group_id)
                .await
                .unwrap()
                .len(),
            if retained { 2 } else { 0 }
        );
    }
}

#[tokio::test]
async fn store_deletion_cleans_descendants_and_reaches_later_replica_hosts() {
    verify_deletion(true, false, false).await;
}

#[tokio::test]
async fn group_deletion_cleans_replicas_and_preserves_siblings() {
    verify_deletion(false, false, false).await;
}

#[tokio::test]
async fn failed_store_deletion_preserves_all_authority_records() {
    verify_deletion(true, true, false).await;
}

#[tokio::test]
async fn failed_group_deletion_preserves_all_authority_records() {
    verify_deletion(false, true, false).await;
}

#[tokio::test]
async fn store_deletion_reconciles_a_lost_metadata_response() {
    verify_deletion(true, false, true).await;
}

#[tokio::test]
async fn group_deletion_reconciles_a_lost_metadata_response() {
    verify_deletion(false, false, true).await;
}
