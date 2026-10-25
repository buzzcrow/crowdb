// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamRecord, StreamRecordState,
};

#[tokio::test]
async fn published_chunk_record_summary_is_bounded_fenced_and_reads_no_data() {
    let store = Arc::new(MemoryStreamStore::new(65536));
    let stream = ChunkStream::create(
        StreamRecord {
            stream_name: StreamName { high: 1, low: 2 },
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Stream,
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: None,
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        u64::MAX,
        StreamConfig {
            ..StreamConfig::default()
        },
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    for _ in 0..105 {
        stream.append(&[Bytes::from_static(b"x")]).await.unwrap();
    }
    stream.checkpoint().await.unwrap();
    let before = stream.metrics();
    let publications = store.metadata_publish_count();
    let first = stream.observe_metadata(None, 0).unwrap();
    assert_eq!(first.writer_epoch, u64::MAX);
    assert_eq!(first.chunk.mapping_count, 105);
    assert_eq!(first.chunk.logical_start, 0);
    assert_eq!(first.chunk.logical_end, 105);
    assert_eq!(first.next_offset, None);
    assert!(stream.observe_metadata(None, 100).is_err());
    assert!(stream
        .observe_metadata(Some(first.generation), usize::MAX)
        .is_err());
    assert_eq!(stream.metrics(), before);
    assert_eq!(store.metadata_publish_count(), publications);
    stream.append(&[Bytes::from_static(b"y")]).await.unwrap();
    assert_eq!(
        stream.observe_metadata(None, 0).unwrap().generation,
        first.generation
    );
    stream.checkpoint().await.unwrap();
    assert!(stream.observe_metadata(Some(first.generation), 100).is_err());
    let current = stream.observe_metadata(None, 0).unwrap();
    assert!(current.generation > first.generation);
    assert_eq!(
        current.chunk.state,
        crowdb_protocol::chunk_stream::StreamChunkState::Active
    );
    stream.close().await.unwrap();
    let closed = stream.observe_metadata(None, 0).unwrap();
    assert!(closed.closed);
    assert_eq!(
        closed.chunk.state,
        crowdb_protocol::chunk_stream::StreamChunkState::Sealed
    );
    assert_eq!(closed.published_tail, 106);
}
