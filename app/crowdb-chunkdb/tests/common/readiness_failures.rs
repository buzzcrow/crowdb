// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbClientError};
use crowdb_paxos_kv_client::PaxosKvClient;
use crowdb_protocol::chunk_id::{ChunkIdParts, CHUNK_TYPE_S3};
use crowdb_rpc_ffi::RpcServer;
use std::time::Duration;

pub async fn assert_storage_and_owner_failures(
    client: &ChunkdbClient,
    data: &PaxosKvClient,
    control: &PaxosKvClient,
    guard: &RangeGuard,
    server: &RpcServer,
) {
    // Corruption is an error, not an authoritative absent key.
    let mut key = b"/chunk/".to_vec();
    key.extend_from_slice(
        &ChunkIdParts {
            high: u64::from(CHUNK_TYPE_S3) << 56,
            low: 0,
        }
        .to_bytes(),
    );
    data.put(0, 1, &key, b"corrupt chunk record", None).await.unwrap();
    assert!(matches!(
        Box::pin(client.observe_readiness()).await,
        Err(ChunkdbClientError::Internal(_))
    ));
    // A stale cached owner rejects the probe and requests authoritative refresh.
    guard.replace_for_tests(&[]);
    let _ = control.drain_window();
    let rejected =
        tokio::time::timeout(Duration::from_millis(750), Box::pin(client.observe_readiness())).await;
    let rescanned = control.drain_window().scan.len();
    server.stop();
    assert!(
        !matches!(rejected, Ok(Ok(()))),
        "a rejected owner cannot be Ready"
    );
    assert!(
        rescanned > 0,
        "ownership rejection must still refresh the registry"
    );
}
