// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::ChunkdbClient;
use crowdb_paxos_kv_client::PaxosKvClient;
use crowdb_rpc_ffi::RpcServer;
use std::time::Duration;

pub async fn assert_warm_registry_independence(
    client: &ChunkdbClient,
    control: &PaxosKvClient,
    data: &PaxosKvClient,
    server: &RpcServer,
    endpoint: String,
) {
    // Keep the real owner/storage healthy while the warmed client's Group0
    // route fails after ordinary registry refresh becomes due.
    tokio::time::sleep(Duration::from_millis(5100)).await;
    control.seed_leader(0, 0, "127.0.0.1:0".into());
    let _ = control.drain_window();
    let _ = data.drain_window();
    let observed =
        tokio::time::timeout(Duration::from_millis(750), Box::pin(client.observe_readiness())).await;
    control.seed_leader(0, 0, endpoint);
    if !matches!(observed, Ok(Ok(()))) {
        server.stop();
    }
    assert!(
        matches!(observed, Ok(Ok(()))),
        "a validated route and current point reads must not wait for a registry scan: {observed:?}"
    );
    let work = control.drain_window();
    assert_eq!(work.get.len() + work.scan.len(), 0);
    assert_eq!(data.drain_window().get.len(), 2);
}
