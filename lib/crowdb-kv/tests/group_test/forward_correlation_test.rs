// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_kv::cluster::KvServer;
use crowdb_kv::rpc::{KvJournalScanResponse, KvResponse, KvScanResponse};
use crowdb_kv_client::{KvRpcTransport, ReadMode};

async fn missing_get(transport: &KvRpcTransport, endpoint: &str, business_id: u64) -> KvResponse {
    tokio::time::timeout(
        Duration::from_secs(1),
        transport.send_get(endpoint, b"missing", business_id, 0, 1, ReadMode::Linearizable, 0),
    )
    .await
    .expect("Get must correlate without waiting for reaper")
    .unwrap()
}

async fn empty_scan(transport: &KvRpcTransport, endpoint: &str, business_id: u64) -> KvScanResponse {
    tokio::time::timeout(
        Duration::from_secs(1),
        transport.send_scan(
            endpoint,
            b"missing",
            b"",
            b"",
            20,
            business_id,
            0,
            1,
            ReadMode::Linearizable,
            0,
            false,
            false,
            0,
            false,
            0,
        ),
    )
    .await
    .expect("Scan must correlate without waiting for reaper")
    .unwrap()
}

async fn empty_journal(
    transport: &KvRpcTransport,
    endpoint: &str,
    business_id: u64,
) -> KvJournalScanResponse {
    tokio::time::timeout(
        Duration::from_secs(1),
        transport.send_journal_scan(
            endpoint,
            0,
            0,
            b"missing",
            20,
            business_id,
            0,
            1,
            ReadMode::Linearizable,
        ),
    )
    .await
    .expect("JournalScan must correlate without waiting for reaper")
    .unwrap()
}

#[tokio::test]
async fn forwarded_reads_and_failed_forward_keep_caller_rpc_identity() {
    let cluster = crate::common::cluster::start_cluster(&[1, 2], 1).await;
    let leader = cluster.leader();
    let follower = cluster.followers()[0];
    let leader_endpoint = leader.listen_addr().unwrap().to_string();
    let follower_endpoint = follower.listen_addr().unwrap().to_string();
    let transport = KvRpcTransport::new();

    // Offset the caller's wire IDs from the follower's independent counter.
    missing_get(&transport, &leader_endpoint, 9001).await;
    let get = missing_get(&transport, &follower_endpoint, 9002).await;
    assert!(get.not_found, "{get:?}");
    assert_eq!(get.request_id, 2);
    let scan = empty_scan(&transport, &follower_endpoint, 9003).await;
    assert!(scan.ok && scan.items.is_empty());
    assert_eq!(scan.request_id, 3);
    let journal = empty_journal(&transport, &follower_endpoint, 9004).await;
    assert!(journal.ok);
    assert_eq!(journal.request_id, 4);

    leader.stop();
    let rejected = missing_get(&transport, &follower_endpoint, 0).await;
    assert!(!rejected.ok && !rejected.not_leader_hint.is_empty());
    assert_eq!(rejected.request_id, 5);
    let rejected_scan = empty_scan(&transport, &follower_endpoint, 0).await;
    assert!(!rejected_scan.ok && !rejected_scan.not_leader_hint.is_empty());
    assert_eq!(rejected_scan.request_id, 6);
    let rejected_journal = empty_journal(&transport, &follower_endpoint, 0).await;
    assert!(!rejected_journal.ok && !rejected_journal.not_leader_hint.is_empty());
    assert_eq!(rejected_journal.request_id, 7);
    cluster.shutdown().await;
}
