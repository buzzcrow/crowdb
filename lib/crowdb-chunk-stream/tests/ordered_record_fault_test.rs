// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamError, StreamMetadataStore, StreamName,
    StreamPurpose, StreamRecord, StreamRecordState, StreamRegistry,
};
use crowdb_protocol::chunk_stream::{encode_stream_chunk_record, StreamMappings};
use std::{sync::Arc, time::Duration};

#[path = "ordered_record_fault_test/initialization.rs"]
mod initialization;

async fn create(capacity: u64) -> (Arc<MemoryStreamStore>, ChunkStream) {
    create_with_config(capacity, StreamConfig::default()).await
}

async fn create_with_config(capacity: u64, config: StreamConfig) -> (Arc<MemoryStreamStore>, ChunkStream) {
    let store = Arc::new(MemoryStreamStore::new(capacity));
    let stream = ChunkStream::create(
        StreamRecord {
            state: StreamRecordState::Active,
            consumer_id: Some("ordered-record-test".into()),
            ..StreamRecord::creating(StreamName::generate(), Some("test".into()), StreamPurpose::Stream)
        },
        1,
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
async fn lost_publication_response_is_reconciled_without_duplicate_records() {
    let (store, stream) = create(1024).await;
    stream
        .append(&[Bytes::from_static(b"acknowledged")])
        .await
        .unwrap();
    store.lose_publication_response_for_tests();
    assert_eq!(stream.checkpoint().await.unwrap(), 12);
    let records = store.chunk_records_for_tests(stream.stream_name()).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].logical_end, Some(12));
    assert_eq!(records[0].mappings.count(), 1);
    assert_eq!(store.metadata_publish_count(), 2);
    assert_eq!(
        stream.read_at(0, 12).await.unwrap(),
        Bytes::from_static(b"acknowledged")
    );
}

#[tokio::test]
async fn checkpoint_cannot_rewrite_previously_published_append_boundaries() {
    let (store, stream) = create(1024).await;
    stream.append(&[Bytes::from_static(b"a")]).await.unwrap();
    stream.append(&[Bytes::from_static(b"bcd")]).await.unwrap();
    stream.checkpoint().await.unwrap();
    let metadata = store.record_store(1).unwrap();
    let previous = metadata.load_tail(stream.stream_name()).await.unwrap().unwrap();
    let mut changed = previous.record().clone();
    changed.generation += 1;
    changed.mappings = StreamMappings::new(&[2, 2]).unwrap();
    assert!(matches!(
        metadata.publish(Some(&previous), changed, None, None).await,
        Err(StreamError::Corruption(_))
    ));
    assert_eq!(
        metadata
            .load_tail(stream.stream_name())
            .await
            .unwrap()
            .unwrap()
            .record(),
        previous.record()
    );
}

#[tokio::test]
async fn finite_reader_retains_trimmed_chunks_until_it_is_released() {
    let (store, stream) = create(128).await;
    let first = stream.append(&[Bytes::from(vec![1; 100])]).await.unwrap();
    stream.append(&[Bytes::from(vec![2; 100])]).await.unwrap();
    let mut reader = stream
        .reader(0, crowdb_chunk_stream::ReadHint::Bytes(100))
        .unwrap();
    assert_eq!(stream.trim_prefix(100).await.unwrap(), 0);
    assert!(!store.is_released(first.chunk_id.unwrap()).await);
    assert_eq!(reader.next().await.unwrap().unwrap(), Bytes::from(vec![1; 100]));
    drop(reader);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match stream.trim_prefix(100).await {
                Ok(_) | Err(StreamError::Backpressure) => {}
                Err(error) => panic!("trim after reader release: {error}"),
            }
            if store.is_released(first.chunk_id.unwrap()).await {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rollover_preserves_historical_records_and_static_stream_identity() {
    let (store, stream) = create(128).await;
    let identity = store.load(stream.stream_name()).await.unwrap().unwrap();
    stream.append(&[Bytes::from(vec![1; 100])]).await.unwrap();
    stream.append(&[Bytes::from(vec![2; 100])]).await.unwrap();
    let first = store
        .chunk_records_for_tests(stream.stream_name())
        .await
        .unwrap()
        .remove(0);
    let encoded = encode_stream_chunk_record(&first).unwrap();
    for _ in 0..254 {
        stream.append(&[Bytes::from(vec![3; 100])]).await.unwrap();
    }
    let records = store.chunk_records_for_tests(stream.stream_name()).await.unwrap();
    assert_eq!(records.len(), 256);
    assert_eq!(encode_stream_chunk_record(&records[0]).unwrap(), encoded);
    assert_eq!(store.load(stream.stream_name()).await.unwrap().unwrap(), identity);
    assert_eq!(stream.read_at(100, 1).await.unwrap(), Bytes::from_static(&[2]));
    assert_eq!(stream.metrics().record_cache_misses, 1);
}

#[tokio::test]
async fn paused_publication_does_not_expose_uncommitted_checkpoint_bounds() {
    let (store, stream) = create(1024).await;
    stream
        .append(&[Bytes::from_static(b"synchronized")])
        .await
        .unwrap();
    store.pause_publication_for_tests();
    let writer = stream.clone();
    let checkpoint = tokio::spawn(async move { writer.checkpoint().await });
    store.wait_for_publication_for_tests().await;
    let before = store
        .load_tail_for_tests(stream.stream_name())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.record().logical_end, Some(0));
    assert_eq!(
        stream.read_at(0, 12).await.unwrap(),
        Bytes::from_static(b"synchronized")
    );
    store.resume_publication_for_tests();
    assert_eq!(checkpoint.await.unwrap().unwrap(), 12);
    assert_eq!(
        store
            .load_tail_for_tests(stream.stream_name())
            .await
            .unwrap()
            .unwrap()
            .record()
            .logical_end,
        Some(12)
    );
}

#[tokio::test]
async fn takeover_after_seal_fences_a_paused_rollover_before_successor_data() {
    let (store, stream) = create(128).await;
    stream.append(&[Bytes::from(vec![1; 100])]).await.unwrap();
    store.pause_publication_for_tests();
    let old = stream.clone();
    let pending = tokio::spawn(async move { old.append(&[Bytes::from(vec![2; 100])]).await });
    store.wait_for_publication_for_tests().await;
    let new = ChunkStream::open(
        stream.stream_name(),
        2,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap();
    assert_eq!(new.tail(), 100);
    store.resume_publication_for_tests();
    assert!(matches!(pending.await.unwrap(), Err(StreamError::StaleWriter)));
    assert_eq!(new.read_at(0, 100).await.unwrap(), Bytes::from(vec![1; 100]));
    assert_eq!(
        new.append(&[Bytes::from_static(b"new")]).await.unwrap().begin,
        100
    );
    let records = store.chunk_records_for_tests(stream.stream_name()).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].logical_end, records[1].logical_start);
}

#[tokio::test]
async fn duplicate_identity_creation_cannot_overwrite_consumer_or_creation_time() {
    let (store, stream) = create(1024).await;
    let original = store.load(stream.stream_name()).await.unwrap().unwrap();
    let mut conflicting = original.clone();
    conflicting.consumer_id = Some("another-consumer".into());
    conflicting.created_at_ms += 1;
    assert!(store.create(conflicting).await.is_err());
    assert_eq!(store.load(stream.stream_name()).await.unwrap().unwrap(), original);
}

#[tokio::test(start_paused = true)]
async fn expired_read_lease_stops_pending_io_without_waiting_for_the_watchdog() {
    let (store, stream) = create_with_config(
        1024,
        StreamConfig {
            watchdog_interval: Duration::from_secs(3600),
            ..StreamConfig::default()
        },
    )
    .await;
    stream.append(&[Bytes::from_static(b"valid")]).await.unwrap();
    let captured = stream.capture_read_only().await.unwrap();
    store.pause_reads();
    let reader = captured.clone();
    let reading = tokio::spawn(async move { reader.read_at(0, 5).await });
    store.wait_for_concurrent_reads(1).await;
    captured.expire_read_lease_for_tests();
    tokio::time::advance(Duration::from_secs(60)).await;
    let result = tokio::time::timeout(Duration::from_millis(100), reading)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(StreamError::ReadUnavailable(_))));
    store.resume_reads();
}

#[tokio::test(start_paused = true)]
async fn a_pin_renewal_conflict_retries_before_the_next_regular_lease_interval() {
    let (store, _stream) = create(1024).await;
    tokio::task::yield_now().await;
    store.fail_pin_renewals_for_tests(1);
    tokio::time::advance(Duration::from_secs(15)).await;
    tokio::task::yield_now().await;
    assert_eq!(store.pin_renewal_count_for_tests(), 0);
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert_eq!(store.pin_renewal_count_for_tests(), 1);
    assert_eq!(store.metadata_publish_count(), 1);
}

#[tokio::test]
async fn interrupted_collection_keeps_a_retryable_record_across_owner_recovery() {
    for fail_delete in [false, true] {
        let (store, stream) = create(128).await;
        let first = stream.append(&[Bytes::from(vec![1; 100])]).await.unwrap();
        stream.append(&[Bytes::from(vec![2; 100])]).await.unwrap();
        stream.append(&[Bytes::from(vec![3; 100])]).await.unwrap();
        if fail_delete {
            store.fail_record_delete_for_tests();
        } else {
            store.fail_release_for_tests();
        }
        assert!(matches!(
            stream.trim_prefix(100).await,
            Err(StreamError::ReadUnavailable(_))
        ));
        let records = store.chunk_records_for_tests(stream.stream_name()).await.unwrap();
        assert_eq!(
            records[0].state,
            crowdb_protocol::chunk_stream::StreamChunkState::Deleting
        );
        assert_eq!(store.is_released(first.chunk_id.unwrap()).await, fail_delete);
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
        recovered.trim_prefix(100).await.unwrap();
        assert!(store.is_released(first.chunk_id.unwrap()).await);
        assert!(store
            .chunk_records_for_tests(stream.stream_name())
            .await
            .unwrap()
            .iter()
            .all(|record| record.logical_start != Some(0)));
        assert_eq!(
            recovered.read_at(100, 100).await.unwrap(),
            Bytes::from(vec![2; 100])
        );
    }
}
