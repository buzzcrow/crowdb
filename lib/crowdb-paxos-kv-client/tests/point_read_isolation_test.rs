// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv::rpc::{KvErrorCode, KvResponse};
use crowdb_paxos_kv_client::{
    ClientConfig, Error, GetOutcome, PaxosKvClient, PaxosKvRpcTransport, ReadMode, Result,
};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::task::JoinHandle;

#[path = "common/point_read_peer.rs"]
mod point_read_peer;
use point_read_peer::{TestPointReadPeer, TestPointReply};

const BUDGET: Duration = Duration::from_millis(750);

async fn scan_value(transport: &PaxosKvRpcTransport, endpoint: &str) {
    let page = transport
        .send_scan(
            endpoint,
            b"/bulk/",
            b"",
            b"",
            1,
            1,
            0,
            1,
            ReadMode::Linearizable,
            0,
            false,
            false,
            0,
            false,
            0,
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].value.len(), 65536);
}

fn start_scan(transport: Arc<PaxosKvRpcTransport>, endpoint: String) -> JoinHandle<()> {
    tokio::spawn(async move { scan_value(&transport, &endpoint).await })
}

async fn finish_scan(peer: &TestPointReadPeer, scan: JoinHandle<()>) {
    peer.state.release_bulk.notify_one();
    tokio::time::timeout(BUDGET, scan).await.unwrap().unwrap();
}

async fn point(transport: &PaxosKvRpcTransport, endpoint: &str) -> Result<KvResponse> {
    transport
        .send_get(endpoint, b"/chunk/point", 2, 0, 1, ReadMode::Linearizable, 0)
        .await
}

#[tokio::test]
async fn point_read_finishes_while_a_scan_frame_is_incomplete() {
    let peer = TestPointReadPeer::start(TestPointReply::Ready).await;
    let client = Arc::new(PaxosKvClient::new_with_isolated_point_reads(ClientConfig::new(
        vec![],
    )));
    client.seed_leader(0, 1, peer.endpoint.clone());
    let scan = tokio::spawn({
        let client = client.clone();
        async move {
            let page = client
                .scan(
                    0,
                    1,
                    b"/bulk/",
                    b"",
                    b"",
                    1,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(page.items[0].1.len(), 65536);
        }
    });
    tokio::time::timeout(BUDGET, peer.state.bulk_started.notified())
        .await
        .unwrap();
    let get = tokio::time::timeout(
        BUDGET,
        client.get(0, 1, b"/chunk/point", ReadMode::Linearizable, None),
    )
    .await;
    let received = peer.state.gets.load(Ordering::Acquire);
    finish_scan(&peer, scan).await;
    assert_eq!(
        received, 1,
        "the peer received the point read while its scan response was held"
    );
    assert!(matches!(get, Ok(Ok(GetOutcome::NotFound))),
        "a real point read must finish within its original deadline despite an incomplete scan frame: {get:?}");
    assert_eq!(peer.state.accepted.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn ordinary_transport_keeps_the_shared_pool_without_opt_in() {
    let peer = TestPointReadPeer::start(TestPointReply::Ready).await;
    let transport = Arc::new(PaxosKvRpcTransport::new());
    let scan = start_scan(transport.clone(), peer.endpoint.clone());
    tokio::time::timeout(BUDGET, peer.state.bulk_started.notified())
        .await
        .unwrap();
    let get = tokio::time::timeout(BUDGET, point(&transport, &peer.endpoint)).await;
    finish_scan(&peer, scan).await;
    assert!(
        get.is_err(),
        "the control must retain shared-frame response ordering: {get:?}"
    );
    assert_eq!(peer.state.gets.load(Ordering::Acquire), 1);
    assert_eq!(peer.state.accepted.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn isolated_point_reads_preserve_rejection() {
    let peer = TestPointReadPeer::start(TestPointReply::Rejected).await;
    let transport = PaxosKvRpcTransport::new().with_isolated_point_reads();
    let result = tokio::time::timeout(BUDGET, point(&transport, &peer.endpoint))
        .await
        .unwrap()
        .unwrap();
    assert!(!result.ok);
    assert!(!result.not_found);
    assert_eq!(result.error_code, KvErrorCode::KvErrorUnavailable as i32);
    assert_eq!(peer.state.gets.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn isolated_point_reads_keep_the_original_deadline() {
    let peer = TestPointReadPeer::start(TestPointReply::Held).await;
    let transport = PaxosKvRpcTransport::new().with_isolated_point_reads();
    assert!(tokio::time::timeout(BUDGET, point(&transport, &peer.endpoint))
        .await
        .is_err());
    assert_eq!(peer.state.gets.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn failed_point_generation_does_not_evict_a_healthy_scan_pool() {
    let peer = TestPointReadPeer::start(TestPointReply::ClosedOnce).await;
    let transport = Arc::new(PaxosKvRpcTransport::new().with_isolated_point_reads());
    let scan = start_scan(transport.clone(), peer.endpoint.clone());
    tokio::time::timeout(BUDGET, peer.state.bulk_started.notified())
        .await
        .unwrap();
    let lost = tokio::time::timeout(BUDGET, point(&transport, &peer.endpoint)).await;
    let recovered = tokio::time::timeout(BUDGET, point(&transport, &peer.endpoint)).await;
    finish_scan(&peer, scan).await;
    assert!(matches!(lost, Ok(Err(Error::Transport { .. }))), "{lost:?}");
    assert!(recovered.unwrap().unwrap().not_found);
    scan_value(&transport, &peer.endpoint).await;
    assert_eq!(peer.state.accepted.load(Ordering::Acquire), 3);
    transport.clear_connections();
    assert!(point(&transport, &peer.endpoint).await.unwrap().not_found);
    scan_value(&transport, &peer.endpoint).await;
    assert_eq!(
        peer.state.accepted.load(Ordering::Acquire),
        5,
        "explicit clear must replace both pools; failed point reads must replace only their own generation"
    );
}
