// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamBinding, StreamBindingState, StreamChunkStore,
    StreamConfig, StreamName, StreamPurpose,
};
use std::sync::Arc;

async fn create(config: StreamConfig) -> (Arc<MemoryStreamStore>, ChunkStream) {
    let store = Arc::new(MemoryStreamStore::new(150_000));
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: StreamPurpose::Stream,
            stream_name: StreamName { high: 97, low: 101 },
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        9,
        config,
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    (store, stream)
}

#[tokio::test]
async fn trimming_one_frame_cannot_release_a_sealed_chunk_with_a_retained_suffix() {
    let (store, stream) = create(StreamConfig::default()).await;
    let payload = Bytes::from(vec![0x5a; 100_000]);
    let first = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    let second = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    assert_ne!(first.chunk_id, second.chunk_id);
    assert_eq!(stream.trim_prefix(70_000).await.unwrap(), 0);
    assert!(!store.is_released(first.chunk_id.unwrap()).await);
    assert_eq!(
        stream.read_at(70_000, 30_000).await.unwrap(),
        payload.slice(70_000..)
    );
    let physical = store
        .durable_cursor(first.chunk_id.unwrap(), 9)
        .await
        .unwrap()
        .offset;
    assert_eq!(stream.trim_prefix(first.end).await.unwrap(), physical);
    assert!(store.is_released(first.chunk_id.unwrap()).await);
    assert_eq!(
        stream.read_at(second.begin, payload.len()).await.unwrap(),
        payload
    );
}

#[tokio::test]
async fn trimming_a_whole_active_chunk_keeps_it_available_for_later_append() {
    let (store, stream) = create(StreamConfig::default()).await;
    let payload = Bytes::from(vec![0x5a; 50_000]);
    let first = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    let second = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    assert_eq!(first.chunk_id, second.chunk_id);
    assert_eq!(stream.trim_prefix(first.end).await.unwrap(), 0);
    assert_eq!(
        stream.read_at(second.begin, payload.len()).await.unwrap(),
        payload
    );
    assert_eq!(stream.trim_prefix(second.end).await.unwrap(), 0);
    assert!(!store.is_released(first.chunk_id.unwrap()).await);
    let tail = stream.append(&[Bytes::from_static(b"tail")]).await.unwrap();
    assert_eq!(tail.chunk_id, first.chunk_id);
    assert_eq!(
        stream.read_at(tail.begin, 4).await.unwrap(),
        Bytes::from_static(b"tail")
    );
    let physical = store
        .durable_cursor(first.chunk_id.unwrap(), 9)
        .await
        .unwrap()
        .offset;
    let next = stream.append(&[Bytes::from(vec![0x33; 100_000])]).await.unwrap();
    assert_ne!(next.chunk_id, first.chunk_id);
    assert_eq!(stream.trim_prefix(second.end).await.unwrap(), 0);
    assert_eq!(
        stream.read_at(tail.begin, 4).await.unwrap(),
        Bytes::from_static(b"tail")
    );
    assert_eq!(stream.trim_prefix(tail.end).await.unwrap(), physical);
    assert!(store.is_released(first.chunk_id.unwrap()).await);
}

#[tokio::test]
async fn a_gc_budget_smaller_than_one_chunk_still_reclaims_only_complete_chunks() {
    let (store, stream) = create(StreamConfig {
        gc_bytes_per_pass: 1,
        ..StreamConfig::default()
    })
    .await;
    let payload = Bytes::from(vec![0x5a; 100_000]);
    let first = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    let second = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    let third = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    let physical = store
        .durable_cursor(first.chunk_id.unwrap(), 9)
        .await
        .unwrap()
        .offset;
    assert_eq!(stream.trim_prefix(second.end).await.unwrap(), physical);
    assert!(store.is_released(first.chunk_id.unwrap()).await);
    assert!(!store.is_released(second.chunk_id.unwrap()).await);
    assert_eq!(stream.trim_prefix(second.end).await.unwrap(), physical);
    assert!(store.is_released(second.chunk_id.unwrap()).await);
    assert_eq!(stream.trim_prefix(second.end).await.unwrap(), 0);
    assert_eq!(stream.read_at(third.begin, payload.len()).await.unwrap(), payload);
}
