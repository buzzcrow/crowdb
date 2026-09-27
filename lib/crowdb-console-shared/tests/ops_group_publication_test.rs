// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;

#[path = "common/rpc_response_proxy.rs"]
mod rpc_response_proxy;

use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use crowdb_console_shared::ops::{kv_logical, OpContext};
use crowdb_console_shared::ConsoleConfig;
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::common::{KvServerIdentity, ReplicaValue};
use crowdb_protocol::key::{KvGroupKey, KvReplicaKey, TextKey};
use crowdb_test_harness::cluster::KvCluster;

struct TestServer(tokio::task::JoinHandle<()>);

impl Drop for TestServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn verify_group_publication(drop_reply: bool) {
    let cluster = KvCluster::start().await;
    let proxy = rpc_response_proxy::TestResponseProxy::start(cluster.group0_leader_endpoint.clone()).await;
    let ctx = OpContext::new(
        proxy.endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    let armed = proxy.armed.clone();
    let app = Router::new().route(
        "/stores/:sid/groups",
        post(move || {
            armed.store(drop_reply, Ordering::SeqCst);
            async { StatusCode::CREATED }
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
            &[77],
            &[],
            "ok",
            "/test-node",
        )
        .await
        .unwrap();
    ctx.sysmd().add_store(77, &[701]).await.unwrap();
    kv_logical::add_group(&ctx, 77, 7, 700, &[701]).await.unwrap();
    let mut revisions = Vec::new();
    for key in [
        KvGroupKey {
            store_id: 77,
            group_id: 7,
        }
        .to_path(),
        KvReplicaKey {
            store_id: 77,
            group_id: 7,
            replica_id: 700,
        }
        .to_path(),
    ] {
        match ctx
            .kv()
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await
            .unwrap()
        {
            GetOutcome::Found { revision, .. } => revisions.push(revision),
            GetOutcome::NotFound => panic!("missing committed membership: {key}"),
        }
    }
    assert_eq!(
        revisions[0], revisions[1],
        "group and its initial members must commit atomically"
    );
    assert_eq!(proxy.dropped.load(Ordering::SeqCst), usize::from(drop_reply));
}

#[tokio::test]
async fn group_and_initial_replica_share_one_committed_revision() {
    verify_group_publication(false).await;
}

#[tokio::test]
async fn lost_batch_response_confirms_complete_group_membership() {
    verify_group_publication(true).await;
}

#[tokio::test]
async fn orphan_membership_is_not_overwritten_by_group_creation() {
    let cluster = KvCluster::start().await;
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    ctx.sysmd().add_store(77, &[701]).await.unwrap();
    let orphan = ReplicaValue {
        store_id: 77,
        group_id: 7,
        replica_id: 700,
        node_id: 999,
        role: String::new(),
        voting: true,
        endpoint: String::new(),
    };
    ctx.sysmd().add_replica(&orphan).await.unwrap();
    let result = kv_logical::add_group(&ctx, 77, 7, 700, &[701]).await;
    assert!(matches!(
        result,
        Err(crowdb_console_shared::error::Error::Conflict { .. })
    ));
    assert!(ctx.sysmd().get_group(77, 7).await.unwrap().is_none());
    assert_eq!(ctx.sysmd().get_replica(77, 7, 700).await.unwrap(), Some(orphan));
}
