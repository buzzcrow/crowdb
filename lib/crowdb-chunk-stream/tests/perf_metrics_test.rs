// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, register_stream_perf_metrics, stream_perf_metrics, ChunkStream, StreamBinding,
    StreamBindingState, StreamConfig, StreamName, StreamPurpose,
};
use crowdb_common::metrics::{MetricPoint, MetricsRegistry};

#[tokio::test]
async fn stream_stages_appear_in_perf_output_and_survive_window_flushes() {
    let mut registry = MetricsRegistry::new();
    register_stream_perf_metrics(&mut registry);
    register_stream_perf_metrics(&mut registry);
    let store = Arc::new(MemoryStreamStore::new(1024));
    let name = StreamName::generate();
    let stream = ChunkStream::create(
        StreamBinding {
            state: StreamBindingState::Active,
            ..StreamBinding::creating(name, None, StreamPurpose::Stream)
        },
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    stream.append(&[Bytes::from_static(b"sample")]).await.unwrap();
    assert_eq!(stream.read_at(0, 6).await.unwrap(), Bytes::from_static(b"sample"));
    for name in [
        "stream.append.e2e.lh",
        "stream.append.queue.lh",
        "stream.capacity.wait.lh",
        "stream.append.encode.lh",
        "stream.append.mirror.lh",
        "stream.append.cursor.lh",
        "stream.append.local_view.lh",
        "stream.read.e2e.lh",
        "stream.read.plan.lh",
        "stream.read.io.lh",
    ] {
        assert!(
            matches!(
                registry.snapshot_named(name, 1.0),
                Some(MetricPoint::Histogram { count: 1, .. })
            ),
            "missing phase: {name}"
        );
    }
    let cumulative = stream_perf_metrics();
    let mut output = Vec::new();
    registry.flush(&mut output, 1.0, "test");
    let text = String::from_utf8(output).unwrap();
    assert_eq!(text.matches("stream.append.cursor.lh").count(), 1);
    assert!(text.contains("stream.metadata.publish.lh"));
    assert!(matches!(
        registry.snapshot_named("stream.append.e2e.lh", 1.0),
        Some(MetricPoint::Histogram {
            count: 0,
            total: 1,
            ..
        })
    ));
    for ((name, before), (after_name, after)) in cumulative.into_iter().zip(stream_perf_metrics()) {
        assert_eq!(name, after_name);
        assert_eq!(before.count, after.count);
        assert_eq!(before.p99, after.p99);
    }
}
