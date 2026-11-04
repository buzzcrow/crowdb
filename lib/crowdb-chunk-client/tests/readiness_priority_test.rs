// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/readiness_peer.rs"]
mod peer;
#[path = "common/read_payload.rs"]
mod storage;

use crowdb_chunk_client::{
    client::observe_disk_readiness_for_tests, metrics::ReadinessMetrics, ChunkClientMetrics, ChunkIoClient,
    RoutedDiskWriter,
};
use crowdb_common::metrics::{MetricPoint, MetricsRegistry};
use crowdb_diskio_client::{DiskId, OperationOptions, SegmentTarget};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkType};
use crowdb_protocol::common::DiskId as ProtoDiskId;
use crowdb_rpc_ffi::OwnedPayload;
use peer::{TestProbeReply, TestReadinessPeer};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::time::Instant;

async fn exercise(reply: TestProbeReply) -> (crowdb_chunk_client::Result<()>, usize, ReadinessMetrics) {
    let peer = TestReadinessPeer::start(reply).await;
    let client = peer.client();
    let target = SegmentTarget::new(DiskId::new(1, 2), 0, 0, 16, 4096).unwrap();
    let bulk = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .read(target, 0, 65536, OperationOptions::within(Duration::from_secs(5)))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), peer.state.bulk_started.notified())
        .await
        .unwrap();
    let mut registry = MetricsRegistry::new();
    let aggregate = Arc::new(ChunkClientMetrics::register(&mut registry));
    let io = ChunkIoClient::from_parts(
        Arc::new(storage::TestPayloadStorage {
            chunk: Chunk::default(),
            data: OwnedPayload::from(Vec::new()),
        }),
        Arc::new(RoutedDiskWriter::from_client(client.clone())),
        ChunkType::S3,
    )
    .with_metrics(&aggregate);
    let (_, writer) = io.storage_parts();
    let metrics = ReadinessMetrics::default();
    let result = observe_disk_readiness_for_tests(
        writer.as_ref(),
        ProtoDiskId { high: 1, low: 2 },
        Instant::now() + Duration::from_millis(750),
        &metrics,
    )
    .await;
    assert!(matches!(
        registry.snapshot_named("chunkio.diskio.read.e2e.lh", 1.0),
        Some(MetricPoint::Histogram { total: 1, .. })
    ));
    assert!(
        !bulk.is_finished(),
        "the bulk response must remain held while readiness completes"
    );
    let headers = peer.state.headers.load(Ordering::Acquire);
    peer.state.release_probe.notify_one();
    peer.state.release_bulk.notify_one();
    let bytes = bulk.await.unwrap().unwrap();
    assert_eq!(bytes.len(), 65536);
    assert!(bytes.iter().all(|b| *b == 7));
    client.shutdown();
    (result, headers, metrics)
}

#[tokio::test]
async fn readiness_header_progresses_while_a_bulk_response_is_held() {
    let (result, headers, metrics) = exercise(TestProbeReply::Ready).await;
    assert!(
        result.is_ok(),
        "physical readiness must not wait behind a bulk TCP response: {result:?}"
    );
    assert_eq!(headers, 1);
    assert_eq!(metrics.snapshot().diskio.errors, 0);
}

#[tokio::test]
async fn rejected_priority_header_still_fails_readiness() {
    let (result, headers, metrics) = exercise(TestProbeReply::Rejected).await;
    assert!(result.is_err());
    assert_eq!(headers, 1);
    assert_eq!(metrics.snapshot().diskio.errors, 1);
}

#[tokio::test]
async fn held_priority_header_still_owes_the_original_readiness_deadline() {
    let (result, headers, metrics) = exercise(TestProbeReply::Held).await;
    assert!(result.unwrap_err().to_string().contains("readiness deadline"));
    assert_eq!(headers, 1);
    assert_eq!(metrics.snapshot().diskio.errors, 1);
}
