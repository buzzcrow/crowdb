// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/read_payload_fixture.rs"]
mod fixture;
#[path = "common/read_payload.rs"]
mod storage;

use crowdb_chunk_client::{ChunkReadPolicy, ChunkReader};
use std::sync::Arc;

#[tokio::test]
async fn framing_and_stream_credits_preserve_original_allocation_metadata() {
    let (storage, location) = fixture::storage();
    let frame_length = storage.data.len();
    let backing = Arc::clone(storage.data.allocation().unwrap());
    let reader = ChunkReader::new(storage.clone(), storage, ChunkReadPolicy::default()).unwrap();
    let mut stream = reader.read_stream(&[location]).unwrap();
    let payload = stream.next_payload().await.unwrap().unwrap();
    assert_eq!(payload.as_ref(), b"payload");
    assert!(Arc::ptr_eq(payload.allocation().unwrap(), &backing));
    assert_eq!(payload.allocation().unwrap().capacity(), 4096);
    assert_eq!(
        payload.as_ptr(),
        backing.bytes()[payload.allocation_offset()..].as_ptr()
    );
    assert!(stream.next_payload().await.is_none());
    drop(stream);
    assert_eq!(reader.flow_metrics_snapshot().stream_bytes_released, 0);
    let bytes = payload.into_bytes();
    assert_eq!(reader.flow_metrics_snapshot().stream_bytes_released, 0);
    drop(bytes);
    assert_eq!(
        reader.flow_metrics_snapshot().stream_bytes_released,
        frame_length as u64
    );
}

#[tokio::test]
async fn metrics_preserve_payload_ownership_and_record_one_read() {
    use crowdb_chunk_client::{ChunkClientMetrics, ChunkIoClient};
    use crowdb_common::metrics::{MetricPoint, MetricsRegistry};
    use crowdb_protocol::chunkdb::rpc::ChunkType;

    let (storage, location) = fixture::storage();
    let backing = Arc::clone(storage.data.allocation().unwrap());
    let frame_length = storage.data.len();
    let mut registry = MetricsRegistry::new();
    let metrics = Arc::new(ChunkClientMetrics::register(&mut registry));
    let client = ChunkIoClient::from_parts(storage.clone(), storage, ChunkType::S3).with_metrics(&metrics);
    assert_eq!(
        client.transport_status_json().unwrap(),
        br#"{"source":"original"}"#
    );
    let mut stream = client.read_stream(&[location]).unwrap();
    let payload = stream.next_payload().await.unwrap().unwrap();
    assert_eq!(payload.as_ref(), b"payload");
    assert!(Arc::ptr_eq(payload.allocation().unwrap(), &backing));
    assert_eq!(payload.allocation().unwrap().capacity(), 4096);
    assert_eq!(
        payload.as_ptr(),
        backing.bytes()[payload.allocation_offset()..].as_ptr()
    );
    assert!(stream.next_payload().await.is_none());
    drop(stream);
    assert_eq!(metrics.read_flow.snapshot().stream_bytes_released, 0);
    drop(payload);
    assert_eq!(
        metrics.read_flow.snapshot().stream_bytes_released,
        frame_length as u64
    );
    assert!(matches!(
        registry.snapshot_named("chunkio.diskio.read.e2e.lh", 1.0),
        Some(MetricPoint::Histogram { total: 1, .. })
    ));
    assert!(matches!(registry.snapshot_named("chunkio.diskio.read.bw", 1.0),
        Some(MetricPoint::Bandwidth { total_bytes, .. }) if total_bytes == frame_length as u64));
}
