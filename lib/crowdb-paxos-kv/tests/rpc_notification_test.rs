// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_paxos_kv::rpc::{PxRpcTransport, ReplicaDestination};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{flush_cpp_global_metrics, RpcServer};

/// A receiver deliberately sends no replies, as required for chosen notices.
/// Both notification variants must arrive without later timing out in the sender.
#[tokio::test]
async fn chosen_notifications_do_not_create_reply_timeouts() {
    let server = RpcServer::with_engines(None, 1, 1);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for kind in [
        FBMsgType::EChosenNotification,
        FBMsgType::EBatchChosenNotification,
    ] {
        let tx = tx.clone();
        let msg_type = u16::try_from(kind.0).unwrap();
        server.register_handler(msg_type, move |_| {
            tx.send(msg_type).unwrap();
        });
    }
    server.listen("127.0.0.1", 0).unwrap();
    server.start();
    let endpoint = format!("127.0.0.1:{}", server.port());
    let destination = ReplicaDestination {
        endpoint: &endpoint,
        replica_id: 2,
    };
    let transport = PxRpcTransport::new();
    for slot in 1..=32 {
        transport.send_chosen(destination, 1, slot, 1, 1, 1).unwrap();
        transport
            .send_batch_chosen(destination, 1, slot, slot + 1, 1, 1, 1)
            .unwrap();
    }
    let mut received = [0; 2];
    for _ in 0..64 {
        let kind = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("notification delivery timeout")
            .expect("notification receiver closed");
        let is_batch = kind == u16::try_from(FBMsgType::EBatchChosenNotification.0).unwrap();
        received[usize::from(is_batch)] += 1;
    }
    assert_eq!(received, [32, 32]);

    // The consensus client's reply deadline is 3s and its reaper scans every
    // 500ms. Keep it alive beyond both so the old request-response path fails.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let metrics =
        flush_cpp_global_metrics(4.0, "notification-test", "cpp-rpc", 60, 10, 10).unwrap_or_default();
    let reaped = metrics.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next()? == "rpc.request.reaped.c").then(|| fields.last().unwrap().parse::<u64>().unwrap())
    });
    server.stop();
    assert_eq!(
        reaped.unwrap_or(0),
        0,
        "one-way notices must not be reaped:\n{metrics}"
    );
}
