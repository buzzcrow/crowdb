// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Legacy exited-storage fault-budget comparison pending container semantics.

use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkIoWriter, IoError, SmallWritePolicy};
use crowdb_console_shared::{config::ServiceType, lifecycle, ops::s3};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_test_harness::test_dirs::TestDir;
use std::path::Path;
use std::time::Duration;

struct StopClusterOnDrop<'a>(&'a Path);
impl Drop for StopClusterOnDrop<'_> {
    fn drop(&mut self) {
        let _ = s3::stop(self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(
    target_os = "macos",
    ignore = "stops all real DiskIO processes in a simulated three-rack production cluster"
)]
async fn protected_two_copy_write_stops_after_repair_and_chunk_rotation_fail() {
    let dir = TestDir::new("access-protected-mirror-failure").unwrap();
    s3::start_protected_test_cluster(dir.path()).await.unwrap();
    let _cleanup = StopClusterOnDrop(dir.path());
    let (cluster, _) = s3::load(dir.path()).unwrap();
    let seeds = cluster
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::PaxosKv)
        .map(|server| server.url.clone())
        .collect();
    let client = ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: seeds,
        diskio_connections_per_endpoint: 2,
        diskio_rpc_workers: 1,
        small_write: SmallWritePolicy {
            chunk_type: ChunkType::S3,
            conversion_enabled: false,
            mirror_copies: 2,
            ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::S3)
        },
    })
    .await
    .unwrap();
    for server in cluster
        .servers
        .iter()
        .filter(|server| server.service_type == ServiceType::Diskio)
    {
        lifecycle::stop_pid_with_timeout(server.pid.unwrap(), Duration::from_secs(5)).unwrap();
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
