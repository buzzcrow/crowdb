// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamChunkStore, StreamConfig, StreamError, StreamName,
    StreamPurpose, StreamRecord, StreamRecordState,
};
use std::sync::Arc;

async fn setup() -> (Arc<MemoryStreamStore>, ChunkStream) {
    let store = Arc::new(MemoryStreamStore::new(8 * 1024 * 1024));
    let name = StreamName { high: 743, low: 9 };
    let stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
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
async fn acknowledged_multiframe_writes_survive_reopen_without_any_cursor_commit() {
    let (store, stream) = setup().await;
    let payload = Bytes::from(vec![0x6b; 2 * 1024 * 1024]);
    let first = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    stream.append(&[Bytes::from_static(b"last")]).await.unwrap();
    assert_eq!(store.cursor_advance_count(), 0);
    assert_eq!(
        store.metadata_publish_count(),
        1,
        "initial attached chunk record only"
    );
    assert_eq!(
        store
            .durable_cursor(first.chunk_id.unwrap(), 1)
            .await
            .unwrap()
            .offset,
        0
    );
    assert_eq!(stream.read_at(0, payload.len()).await.unwrap(), payload);
    let recovered = ChunkStream::open(
        stream.stream_name(),
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.tail(), payload.len() as u64 + 4);
    assert_eq!(recovered.read_at(0, payload.len()).await.unwrap(), payload);
    assert_eq!(
        recovered.read_at(payload.len() as u64, 4).await.unwrap(),
        b"last"[..]
    );
    assert_eq!(
        store.cursor_advance_count(),
        0,
        "recovery seals the observed boundary directly"
    );
    assert!(matches!(
        stream.append(&[Bytes::from_static(b"stale")]).await,
        Err(StreamError::StaleWriter)
    ));
}

#[tokio::test]
async fn checkpoint_commits_many_appends_once_and_recovery_discards_an_incomplete_group() {
    let (store, stream) = setup().await;
    for _ in 0..100 {
        stream.append(&[Bytes::from_static(b"good")]).await.unwrap();
    }
    assert_eq!(store.cursor_advance_count(), 0);
    assert_eq!(stream.checkpoint().await.unwrap(), 400);
    assert_eq!(store.cursor_advance_count(), 1);
    let active = store
        .active_checkpoint_for_tests(stream.stream_name())
        .await
        .unwrap();
    let views = crowdb_protocol::frame::encode_append_group_views(
        active.chunk_id,
        active.acknowledged_cursor,
        &Bytes::from(vec![7; 100_000]),
        1,
    )
    .unwrap();
    let mut torn: Vec<_> = views.iter().flat_map(|view| view.iter().copied()).collect();
    // The first frame is complete, but its grouped append still needs followers.
    torn.truncate(crowdb_protocol::frame::MAX_FRAME_BYTES);
    store
        .stage_incomplete_tail_for_tests(active.chunk_id, Bytes::from(torn))
        .await;
    let recovered = ChunkStream::open(
        stream.stream_name(),
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    assert_eq!(recovered.tail(), 400);
    assert_eq!(recovered.read_at(396, 4).await.unwrap(), b"good"[..]);
    assert_eq!(
        recovered
            .append(&[Bytes::from_static(b"next")])
            .await
            .unwrap()
            .begin,
        400
    );
}

#[tokio::test]
async fn revoked_owner_cannot_acknowledge_a_previously_admitted_write() {
    let (store, stream) = setup().await;
    store.pause_writes();
    let old = stream.clone();
    let write = tokio::spawn(async move { old.append(&[Bytes::from_static(b"pending")]).await });
    store.wait_for_write().await;
    store.fence_writer_for_tests(stream.stream_name()).await;
    store.resume_writes();
    assert!(matches!(write.await.unwrap(), Err(StreamError::StaleWriter)));
    assert_eq!(stream.tail(), 0);
    assert_eq!(store.cursor_advance_count(), 0);
    assert!(
        stream.release_writer().await.is_err(),
        "a stalled write cannot produce a clean release proof"
    );
}

#[tokio::test(start_paused = true)]
async fn chunk_liveness_renews_while_a_data_write_is_blocked() {
    let store = Arc::new(MemoryStreamStore::new(1024));
    let name = StreamName { high: 743, low: 10 };
    let stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        },
        1,
        StreamConfig {
            liveness_interval: std::time::Duration::from_secs(1),
            ..StreamConfig::default()
        },
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    stream.append(&[Bytes::from_static(b"first")]).await.unwrap();
    store.pause_writes();
    let old = stream.clone();
    let write = tokio::spawn(async move { old.append(&[Bytes::from_static(b"pending")]).await });
    store.wait_for_write().await;
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        store.liveness_renew_count(),
        2,
        "both the attached chunk and its ready successor renew while data IO is blocked"
    );
    assert!(!write.is_finished());
    assert_eq!(store.cursor_advance_count(), 0);
    store.resume_writes();
    assert_eq!(write.await.unwrap().unwrap().end, 12);
}
