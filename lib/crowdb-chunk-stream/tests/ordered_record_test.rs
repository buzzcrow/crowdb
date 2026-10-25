// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamMetadataStore, StreamName, StreamPurpose,
    StreamRecord, StreamRecordState,
};
use crowdb_protocol::chunk_stream::StreamChunkState;
use std::sync::Arc;

async fn create(capacity: u64) -> (Arc<MemoryStreamStore>, ChunkStream) {
    let store = Arc::new(MemoryStreamStore::new(capacity));
    let stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(StreamName::generate(), Some("test".into()), StreamPurpose::Stream)
        },
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    (store, stream)
}

#[tokio::test]
async fn ordered_records_cover_rollover_and_only_tail_recovery() {
    let (store, stream) = create(128).await;
    let first = stream.append(&[Bytes::from(vec![1; 100])]).await.unwrap();
    let second = stream.append(&[Bytes::from(vec![2; 100])]).await.unwrap();
    assert_ne!(first.chunk_id, second.chunk_id);
    let metadata = store.record_store(1).unwrap();
    let old = metadata.locate(stream.stream_name(), 0).await.unwrap();
    assert_eq!(old.record().state, StreamChunkState::Sealed);
    assert_eq!(old.record().logical_start, Some(0));
    assert_eq!(old.record().logical_end, Some(100));
    assert_eq!(
        stream.read_at(0, 200).await.unwrap(),
        Bytes::from([vec![1; 100], vec![2; 100]].concat())
    );
    let reopened = ChunkStream::open(
        stream.stream_name(),
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.tail(), 200);
    assert_eq!(
        reopened.read_at(100, 100).await.unwrap(),
        Bytes::from(vec![2; 100])
    );
}

#[tokio::test]
async fn checkpoint_capture_is_finite_and_close_survives_reopen() {
    let (store, stream) = create(1024).await;
    stream.append(&[Bytes::from_static(b"first")]).await.unwrap();
    let captured = stream.capture_read_only().await.unwrap();
    stream.append(&[Bytes::from_static(b"second")]).await.unwrap();
    assert_eq!(captured.tail(), 5);
    assert_eq!(
        captured.read_at(0, 5).await.unwrap(),
        Bytes::from_static(b"first")
    );
    assert!(captured.read_at(5, 1).await.is_err());
    stream.close().await.unwrap();
    let tail = store
        .load_tail_for_tests(stream.stream_name())
        .await
        .unwrap()
        .unwrap();
    assert!(tail.record().closed);
    assert_eq!(tail.record().logical_end, Some(11));
    let reopened = ChunkStream::open(
        stream.stream_name(),
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    assert_eq!(reopened.tail(), 11);
    assert!(reopened.append(&[Bytes::from_static(b"x")]).await.is_err());
}

#[tokio::test]
async fn registered_candidate_is_distinct_from_active_tail() {
    let (store, stream) = create(1024).await;
    let metadata = store.record_store(1).unwrap();
    let candidates = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let candidates = metadata.candidates(stream.stream_name()).await.unwrap();
            if !candidates.is_empty() {
                break candidates;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let tail = metadata.load_tail(stream.stream_name()).await.unwrap().unwrap();
    assert_eq!(tail.record().state, StreamChunkState::Active);
    assert_eq!(candidates[0].record().state, StreamChunkState::Candidate);
    assert!(candidates[0].record().logical_start.is_none());
    assert_ne!(tail.record().chunk_id, candidates[0].record().chunk_id);
    stream.close().await.unwrap();
    assert!(metadata
        .candidates(stream.stream_name())
        .await
        .unwrap()
        .is_empty());
}
