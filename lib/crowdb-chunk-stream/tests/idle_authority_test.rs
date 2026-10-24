// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamChunkStore, StreamConfig, StreamError, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState,
};
use std::{sync::Arc, time::Duration};

#[tokio::test(start_paused = true)]
async fn superseded_idle_writer_stops_renewing_and_publishing() {
    assert_retired_idle_writer(10).await;
}

#[tokio::test(start_paused = true)]
async fn same_epoch_reopen_retires_the_prior_idle_worker() {
    assert_retired_idle_writer(9).await;
}

async fn assert_retired_idle_writer(new_epoch: u64) {
    let store = Arc::new(MemoryStreamStore::new(64));
    let name = StreamName { high: 100, low: 1 };
    let config = StreamConfig {
        liveness_interval: Duration::from_secs(10),
        ..StreamConfig::default()
    };
    let old = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("idle-authority".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        9,
        config.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    old.append(&[Bytes::from_static(b"old")]).await.unwrap();
    let new_config = StreamConfig {
        liveness_interval: Duration::from_secs(3600),
        ..config
    };
    let new = ChunkStream::open(
        name,
        new_epoch,
        new_config,
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    let head = store
        .load_tail_for_tests(name)
        .await
        .unwrap()
        .map(|snapshot| snapshot.record().clone());
    let publications = store.metadata_publish_count();
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(
        store.liveness_attempt_count_for_tests(),
        0,
        "old writer must stop before renewing its chunk"
    );
    assert_eq!(store.metadata_publish_count(), publications);
    assert_eq!(
        store
            .load_tail_for_tests(name)
            .await
            .unwrap()
            .map(|snapshot| snapshot.record().clone()),
        head
    );
    assert_eq!(
        old.append(&[Bytes::from_static(b"stale")]).await,
        Err(StreamError::StaleWriter)
    );
    new.append(&[Bytes::from_static(b"new")]).await.unwrap();
    assert_eq!(new.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
}

#[tokio::test(start_paused = true)]
async fn current_idle_writer_can_rotate_its_own_sealed_chunk() {
    let store = Arc::new(MemoryStreamStore::new(64));
    let name = StreamName { high: 100, low: 2 };
    let config = StreamConfig {
        liveness_interval: Duration::from_secs(10),
        ..StreamConfig::default()
    };
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: None,
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        9,
        config,
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    stream.checkpoint().await.unwrap();
    let active = store.active_checkpoint_for_tests(name).await.unwrap();
    store
        .seal(active.chunk_id, 9, active.acknowledged_cursor)
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(stream.metrics().rollovers, 1);
    stream.append(&[Bytes::from_static(b"new")]).await.unwrap();
    assert_eq!(stream.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
}
