// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState,
};
use tokio::time::timeout;

async fn stream(store: Arc<MemoryStreamStore>, interval: Duration) -> ChunkStream {
    ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(StreamName::generate(), None, StreamPurpose::Stream)
        },
        1,
        StreamConfig {
            liveness_interval: interval,
            ..StreamConfig::default()
        },
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn pending_successor_only_backpressures_writes_that_require_rotation() {
    let store = Arc::new(MemoryStreamStore::new(128));
    store.pause_allocation_for_tests(2);
    let stream = stream(store.clone(), Duration::from_secs(60)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    timeout(Duration::from_secs(1), store.wait_for_allocation_for_tests())
        .await
        .unwrap();
    timeout(
        Duration::from_secs(1),
        stream.append(&[Bytes::from_static(b"new")]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(stream.tail(), 6);
    let writer = stream.clone();
    let mut rollover = tokio::spawn(async move { writer.append(&[Bytes::from(vec![0x61; 100])]).await });
    assert!(timeout(Duration::from_millis(20), &mut rollover).await.is_err());
    assert_eq!(stream.tail(), 6);
    store.resume_allocation_for_tests();
    let range = timeout(Duration::from_secs(1), rollover)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(range.chunk_id.unwrap().low, 2);
    assert_eq!(range.begin, 6);
    assert_eq!(stream.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
    stream.close().await.unwrap();
}

#[tokio::test]
async fn rotation_uses_the_prepared_candidate_while_its_replacement_is_blocked() {
    let store = Arc::new(MemoryStreamStore::new(128));
    store.pause_allocation_for_tests(3);
    let stream = stream(store.clone(), Duration::from_secs(60)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    timeout(Duration::from_secs(1), async {
        loop {
            if store.chunks_for_tests().await.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.chunks_for_tests().await.len(), 2);
    let range = timeout(
        Duration::from_secs(1),
        stream.append(&[Bytes::from(vec![0x61; 100])]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(range.chunk_id.unwrap().low, 2);
    timeout(Duration::from_secs(1), store.wait_for_allocation_for_tests())
        .await
        .unwrap();
    assert_eq!(stream.metrics().rollovers, 1);
    store.resume_allocation_for_tests();
    stream.close().await.unwrap();
    let chunks = store.chunks_for_tests().await;
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[2].1, 0);
    assert!(
        chunks[2].2 && chunks[2].3,
        "unused replacement must be sealed and released"
    );
    assert!(!chunks[1].3, "the attached chunk remains retained");
}

#[tokio::test]
async fn close_drains_candidate_initialization_and_reclaims_it_without_publication() {
    let store = Arc::new(MemoryStreamStore::new(128));
    store.pause_allocation_for_tests(2);
    let stream = stream(store.clone(), Duration::from_secs(60)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    timeout(Duration::from_secs(1), store.wait_for_allocation_for_tests())
        .await
        .unwrap();
    assert_eq!(
        store.metadata_publish_count(),
        1,
        "preparing a candidate must not publish a new tail checkpoint"
    );
    let mut closing = tokio::spawn(async move { stream.close().await });
    assert!(timeout(Duration::from_millis(20), &mut closing).await.is_err());
    store.resume_allocation_for_tests();
    timeout(Duration::from_secs(1), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let chunks = store.chunks_for_tests().await;
    assert_eq!(chunks.len(), 2);
    assert!(chunks[1].2 && chunks[1].3);
    assert_eq!(chunks[1].1, 0);
    assert_eq!(store.metadata_publish_count(), 2);
}

#[tokio::test(start_paused = true)]
async fn idle_candidate_is_renewed_without_publishing_or_rotating_the_stream() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let stream = stream(store.clone(), Duration::from_secs(10)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    tokio::task::yield_now().await;
    let publications = store.metadata_publish_count();
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
    }
    assert!(
        store.liveness_renew_count() >= 6,
        "both attached and prepared chunks require renewal"
    );
    assert_eq!(store.metadata_publish_count(), publications);
    assert_eq!(stream.metrics().rollovers, 0);
    stream.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn failed_candidate_renewal_replaces_only_the_unused_chunk() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let stream = stream(store.clone(), Duration::from_secs(10)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    tokio::task::yield_now().await;
    let candidate = store.chunks_for_tests().await[1].0;
    store.fail_chunk_renewal_for_tests(candidate);
    let publications = store.metadata_publish_count();
    tokio::time::advance(Duration::from_secs(10)).await;
    timeout(Duration::from_secs(1), async {
        while store.chunks_for_tests().await.len() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let chunks = store.chunks_for_tests().await;
    assert!(chunks[1].2 && chunks[1].3, "failed candidate must be reclaimed");
    assert_eq!(chunks[2].1, 0);
    assert!(!chunks[2].2 && !chunks[2].3);
    assert_eq!(stream.metrics().rollovers, 0);
    assert_eq!(store.metadata_publish_count(), publications);
    stream.append(&[Bytes::from_static(b"new")]).await.unwrap();
    assert_eq!(stream.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
    stream.close().await.unwrap();
}

#[tokio::test]
async fn explicit_owner_release_reclaims_the_candidate_and_does_not_replenish_it() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let stream = stream(store.clone(), Duration::from_secs(10)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    stream.release_writer().await.unwrap();
    tokio::task::yield_now().await;
    let chunks = store.chunks_for_tests().await;
    assert_eq!(chunks.len(), 2);
    assert!(chunks[1].2 && chunks[1].3);
    assert!(!chunks[0].3);
    assert!(stream.append(&[Bytes::from_static(b"late")]).await.is_err());
}

#[tokio::test]
async fn candidate_allocation_outage_backpressures_instead_of_poisoning_the_writer() {
    let store = Arc::new(MemoryStreamStore::new(128));
    store.fail_allocation_for_tests(2);
    let stream = stream(store.clone(), Duration::from_secs(10)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    timeout(Duration::from_secs(1), store.wait_for_allocation_for_tests())
        .await
        .unwrap();
    let writer = stream.clone();
    let mut pending = tokio::spawn(async move { writer.append(&[Bytes::from(vec![0x61; 100])]).await });
    assert!(timeout(Duration::from_millis(20), &mut pending).await.is_err());
    assert_eq!(stream.tail(), 3);
    let range = timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(range.chunk_id.unwrap().low, 3);
    assert_eq!(range.begin, 3);
    assert_eq!(stream.metrics().rollovers, 1);
    assert_eq!(stream.metrics().failed, 0);
    stream.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn active_liveness_outage_backpressures_without_rotating_a_healthy_tail() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let stream = stream(store.clone(), Duration::from_secs(10)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    tokio::task::yield_now().await;
    store.fail_next_renewals(100);
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..4 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
    let writer = stream.clone();
    let mut pending = tokio::spawn(async move { writer.append(&[Bytes::from_static(b"new")]).await });
    assert!(
        timeout(Duration::from_secs(1), &mut pending).await.is_err(),
        "unconfirmed chunk liveness must apply backpressure without sealing the tail"
    );
    assert_eq!(stream.tail(), 3);
    store.fail_next_renewals(0);
    timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stream.metrics().rollovers, 0);
    assert_eq!(stream.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
    stream.close().await.unwrap();
}

#[tokio::test]
async fn unused_candidate_cleanup_failure_does_not_prevent_durable_owner_release() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let stream = stream(store.clone(), Duration::from_secs(60)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    let name = stream.stream_name();
    store.fail_empty_release_for_tests();
    stream.release_writer().await.unwrap();
    let chunks = store.chunks_for_tests().await;
    assert!(!chunks[0].3, "attached data remains retained");
    assert!(
        chunks[1].2 && !chunks[1].3,
        "failed candidate cleanup remains retained for expiry: {chunks:?}"
    );
    let next = ChunkStream::open(
        name,
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    next.append(&[Bytes::from_static(b"new")]).await.unwrap();
    assert_eq!(next.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
    next.close().await.unwrap();
}

#[tokio::test]
async fn unexpected_candidate_data_blocks_owner_release_instead_of_becoming_cleanup_backlog() {
    let store = Arc::new(MemoryStreamStore::new(128));
    let stream = stream(store.clone(), Duration::from_secs(60)).await;
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    timeout(Duration::from_secs(1), async {
        while store.chunks_for_tests().await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let chunks = store.chunks_for_tests().await;
    let encoded = store.synchronized_bytes_for_tests(chunks[0].0).await;
    store
        .write_groups_for_tests(stream.stream_name(), 1, chunks[1].0, 0, encoded)
        .await
        .unwrap();
    let released = stream.release_writer().await;
    assert!(
        matches!(released, Err(crowdb_chunk_stream::StreamError::Corruption(_))),
        "a candidate with data violates the empty-candidate contract: {released:?}"
    );
    assert_eq!(stream.read_at(0, 3).await.unwrap(), Bytes::from_static(b"old"));
}
