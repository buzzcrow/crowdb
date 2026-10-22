// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/mirror_sync_allocator.rs"]
mod allocator;
#[path = "../../crowdb-diskio-client/tests/common/synchronized_peer.rs"]
#[allow(dead_code)]
mod peer;

use bytes::Bytes;
use crowdb_chunk_client::{ChunkClientMetrics, ChunkIoClient, MirrorStripFlow, RoutedDiskWriter};
use crowdb_common::metrics::MetricsRegistry;
use crowdb_diskio_client::{DiskId, DiskioClient, DiskioClientConfig};
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_protocol::diskio_fb::FBDiskIoRetCode;
use peer::TestDiskioPeer;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn metrics_and_routing_forward_one_synchronized_rpc_per_required_replica() {
    run_flow(false).await;
}

#[tokio::test]
async fn replacement_is_synchronized_before_metadata_publication_and_completion() {
    run_flow(true).await;
}

async fn run_flow(repair: bool) {
    let peers = [
        TestDiskioPeer::start(true, false, FBDiskIoRetCode::Success).await,
        TestDiskioPeer::start(true, false, FBDiskIoRetCode::Success).await,
        TestDiskioPeer::start(
            true,
            !repair,
            if repair {
                FBDiskIoRetCode::IoError
            } else {
                FBDiskIoRetCode::Success
            },
        )
        .await,
        TestDiskioPeer::start(true, repair, FBDiskIoRetCode::Success).await,
    ];
    let routes = peers
        .iter()
        .enumerate()
        .map(|(index, peer)| peer.route(DiskId::new(index as u64 + 1, 0)))
        .collect();
    let diskio = Arc::new(
        DiskioClient::connect_for_tests(
            routes,
            DiskioClientConfig {
                retry_attempts: 1,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let allocator = Arc::new(allocator::TestAllocator::default());
    let metrics = Arc::new(ChunkClientMetrics::register(&mut MetricsRegistry::new()));
    let client = ChunkIoClient::from_parts(
        allocator.clone(),
        Arc::new(RoutedDiskWriter::from_client_for_tests(diskio)),
        ChunkType::Stream,
    )
    .with_metrics(&metrics);
    let (allocation, writer) = client.storage_parts();
    let flow = MirrorStripFlow::new(allocation, writer, 1, 1, true).unwrap();
    let operation = tokio::spawn(async move {
        let mut chunk = allocator::chunk();
        let data = Bytes::from_static(&[7; 32]);
        flow.write(&mut chunk, 0, 0, data.clone(), data, &mut None)
            .await
            .unwrap();
    });
    let gated = if repair { 3 } else { 2 };
    peers[gated].wait_for_write().await;
    assert!(!operation.is_finished());
    assert_eq!(allocator.publications.load(Ordering::Relaxed), 0);
    peers[gated].gate.notify_one();
    operation.await.unwrap();
    for (index, peer) in peers.iter().enumerate() {
        let expected = usize::from(index < 3 || repair);
        assert_eq!(peer.counts.writes.load(Ordering::Relaxed), expected);
        assert_eq!(peer.counts.sync_requests.load(Ordering::Relaxed), expected);
        assert_eq!(peer.counts.fsyncs.load(Ordering::Relaxed), 0);
    }
    assert_eq!(
        allocator.publications.load(Ordering::Relaxed),
        usize::from(repair)
    );
}
