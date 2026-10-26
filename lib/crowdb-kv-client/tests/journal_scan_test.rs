// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/owner_fence_server.rs"]
mod owner_fence_server;

use crowdb_kv::paxos::roles::Acceptor;
use crowdb_kv_client::{Error, KvRpcTransport, ReadMode};
use owner_fence_server::TestOwnerFenceServer;
use std::time::Duration;

#[tokio::test]
async fn trimmed_journal_returns_a_typed_gap_without_transport_retries() {
    let test = TestOwnerFenceServer::start().await;
    test.client.put(1, 1, b"retained", b"value", None).await.unwrap();
    test.store.get_group(1).unwrap().local_replica().acceptor.trim(2);
    let raw = KvRpcTransport::new()
        .send_journal_scan(&test.endpoint, 1, 0, b"", 0, 1, 1, 1, ReadMode::Linearizable)
        .await;
    assert!(matches!(raw, Err(Error::JournalScanGcGap)), "{raw:?}");
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        test.client
            .journal_scan(1, 1, 1, 0, b"", 0, 0, ReadMode::Linearizable, None),
    )
    .await;
    assert!(
        matches!(result, Ok(Err(Error::JournalScanGcGap))),
        "trimmed history must immediately select full-scan recovery: {result:?}"
    );
    let metrics = test.client.metrics();
    assert_eq!(metrics.scan_errors, 1);
    assert_eq!(metrics.transport_error_retry, 0);
    assert_eq!(metrics.retries_exhausted, 0);
}
