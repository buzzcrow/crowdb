// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkIoWriter, IoError, SmallWritePolicy};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned release storage containers with retained network endpoints"]
async fn protected_two_copy_write_stops_after_repair_and_chunk_rotation_fail() {
    let addresses = crate::common::management::addresses(14);
    let client = ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: addresses[..3]
            .iter()
            .map(|address| crate::common::management::origin(address))
            .collect(),
        diskio_connections_per_endpoint: 2,
        diskio_rpc_workers: 1,
        small_write: SmallWritePolicy {
            chunk_type: ChunkType::S3,
            conversion_enabled: false,
            mirror_copies: 2,
            ..SmallWritePolicy::new(ChunkType::S3)
        },
    })
    .await
    .unwrap();
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let storage: serde_json::Value =
        serde_json::from_slice(&std::fs::read(artifacts.join("storage-endpoints.json")).unwrap()).unwrap();
    assert_eq!(storage.as_array().unwrap().len(), 6);
    for endpoint in storage.as_array().unwrap() {
        let node = usize::try_from(endpoint["node"].as_u64().unwrap()).unwrap();
        crate::common::control::node("kill", node).await;
    }
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let data = Bytes::from_static(b"failed-protected-write");
        let mut writer = client.prepare_small_write(data.len()).await?;
        writer.on_data(data).await?;
        writer.on_finish().await.map(|_| ())
    })
    .await
    .expect("failed DiskIO write exceeded the 15-second fault budget");
    assert!(matches!(result, Err(IoError::WriteFailed(_))), "{result:?}");
    let metrics = client.small_write_metrics();
    assert_eq!(metrics.completed, 0);
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.exhausted_repairs, 2);
    assert_eq!(metrics.repairs_avoiding_rotation, 0);
    let _ = client.shutdown_small_writes().await;
}
