// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState, StreamViewSource,
};

async fn create() -> (Arc<MemoryStreamStore>, ChunkStream) {
    let store = Arc::new(MemoryStreamStore::new(1024 * 1024));
    let stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(StreamName::generate(), None, StreamPurpose::Stream)
        },
        1,
        StreamConfig {
            ..StreamConfig::default()
        },
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    (store, stream)
}

#[tokio::test]
async fn append_reads_and_recovery_do_not_require_per_batch_record_publication() {
    let (store, stream) = create().await;
    let mut expected = BytesMut::new();
    for value in 0_u64..2048 {
        let payload = Bytes::copy_from_slice(&value.to_le_bytes());
        let range = stream.append(std::slice::from_ref(&payload)).await.unwrap();
        assert_eq!(range.begin, expected.len() as u64);
        expected.extend_from_slice(&payload);
    }
    assert_eq!(store.metadata_publish_count(), 1, "initial attached record only");
    assert_eq!(stream.metrics().metadata_publications, 1);
    assert_eq!(stream.tail(), expected.len() as u64);
    assert_eq!(stream.read_at(0, expected.len()).await.unwrap(), expected);
    assert_eq!(
        stream.metrics().record_cache_misses,
        0,
        "local reads use live mappings"
    );
    let persisted = store
        .load_tail_for_tests(stream.stream_name())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.record().logical_end.unwrap(), 0);
    assert_eq!(persisted.record().mappings.count(), 0);

    let recovered = ChunkStream::open(
        stream.stream_name(),
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    assert_eq!(recovered.tail(), expected.len() as u64);
    assert_eq!(recovered.read_at(0, expected.len()).await.unwrap(), expected);
    assert!(stream.append(&[Bytes::from_static(b"stale")]).await.is_err());
}

#[tokio::test]
async fn owner_checkpoint_is_finite_and_fallback_reports_its_older_boundary() {
    let (store, stream) = create().await;
    stream.append(&[Bytes::from_static(b"first")]).await.unwrap();
    let captured = stream.capture_read_only().await.unwrap();
    assert_eq!(captured.view_source(), StreamViewSource::OwnerCheckpoint);
    assert_eq!(captured.tail(), 5);
    let publications = store.metadata_publish_count();
    assert_eq!(stream.checkpoint().await.unwrap(), 5);
    assert_eq!(
        store.metadata_publish_count(),
        publications,
        "unchanged checkpoint is free"
    );

    stream.append(&[Bytes::from_static(b"second")]).await.unwrap();
    let fallback = ChunkStream::open_read_only(
        stream.stream_name(),
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    assert_eq!(fallback.view_source(), StreamViewSource::PersistedCheckpoint);
    assert_eq!(fallback.tail(), 5);
    assert_eq!(captured.tail(), 5);
    assert_eq!(
        captured.read_at(0, 5).await.unwrap(),
        Bytes::from_static(b"first")
    );
    assert!(fallback.read_at(0, 11).await.is_err());
    assert_eq!(
        stream.read_at(0, 11).await.unwrap(),
        Bytes::from_static(b"firstsecond")
    );
    assert_eq!(store.metadata_publish_count(), publications);
    assert_eq!(stream.checkpoint().await.unwrap(), 11);
    assert_eq!(captured.tail(), 5);
    assert_eq!(fallback.tail(), 5);
}

#[tokio::test]
async fn a_capture_waits_for_the_queued_synchronized_append() {
    let (store, stream) = create().await;
    store.pause_writes();
    let writer = stream.clone();
    let append = tokio::spawn(async move { writer.append(&[Bytes::from_static(b"pending")]).await });
    store.wait_for_write().await;
    let owner = stream.clone();
    let capture = tokio::spawn(async move { owner.capture_read_only().await });
    tokio::task::yield_now().await;
    assert!(!capture.is_finished());
    assert_eq!(stream.tail(), 0);
    store.resume_writes();
    append.await.unwrap().unwrap();
    let reader = capture.await.unwrap().unwrap();
    assert_eq!(reader.tail(), 7);
    assert_eq!(
        reader.read_at(0, 7).await.unwrap(),
        Bytes::from_static(b"pending")
    );
}

#[tokio::test]
async fn an_unchanged_checkpoint_cannot_claim_current_owner_after_takeover() {
    let (store, writer) = create().await;
    writer.append(&[Bytes::from_static(b"first")]).await.unwrap();
    writer.checkpoint().await.unwrap();
    let new_owner = ChunkStream::open(
        writer.stream_name(),
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    assert!(matches!(
        writer.capture_read_only().await,
        Err(crowdb_chunk_stream::StreamError::StaleWriter)
    ));
    assert_eq!(new_owner.tail(), 5);
}
