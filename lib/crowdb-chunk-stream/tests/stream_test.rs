// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, CursorAdvance, ReadHint, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig,
    StreamError, StreamMetadataStore, StreamName, StreamRegistry,
};

fn binding(name: StreamName) -> StreamBinding {
    StreamBinding {
        stream_name: name,
        metadata_group_id: 7,
        binding_generation: 1,
        state: StreamBindingState::Active,
        owner_kind: Some("test".into()),
    }
}

async fn create_stream(store: &Arc<MemoryStreamStore>, capacity: u64, config: StreamConfig) -> ChunkStream {
    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    ChunkStream::create(
        binding(StreamName {
            high: 1,
            low: capacity,
        }),
        9,
        config,
        registry,
        metadata,
        chunks,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn first_append_has_no_timer_and_skips_metadata_consensus() {
    let store = Arc::new(MemoryStreamStore::new(64));
    let stream = create_stream(&store, 64, StreamConfig::default()).await;

    let range = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        stream.append(&[Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]),
    )
    .await
    .expect("single append must not wait for a batching timer")
    .unwrap();

    assert_eq!((range.begin, range.end), (0, 4));
    assert_eq!(store.chunk_write_count(), 1);
    assert_eq!(store.cursor_advance_count(), 1);
    assert_eq!(store.metadata_publish_count(), 2);
    assert_eq!(stream.read_at(0, 4).await.unwrap(), Bytes::from_static(b"abcd"));
}

#[tokio::test]
async fn chunk_bound_append_adds_selected_chunk_identity_and_provenance() {
    let store = Arc::new(MemoryStreamStore::new(64));
    let stream = create_stream(&store, 64, StreamConfig::default()).await;
    let range = stream
        .append_chunk_bound(&[Bytes::from_static(b"body"), Bytes::from_static(b"crc")])
        .await
        .unwrap();
    let chunk_id = range.chunk_id.expect("nonempty append has a chunk");
    assert_eq!((range.begin, range.end), (0, 23));
    let bytes = stream.read_at(0, 23).await.unwrap();
    assert_eq!(&bytes[..7], b"bodycrc");
    assert_eq!(&bytes[7..15], &chunk_id.high.to_be_bytes());
    assert_eq!(&bytes[15..23], &chunk_id.low.to_be_bytes());
    let segments = stream.read_at_with_provenance(0, 23).await.unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].chunk_id, chunk_id);

    let mut reader = stream.reader(4, ReadHint::Bytes(3)).unwrap();
    assert_eq!(reader.next().await.unwrap(), Some(Bytes::from_static(b"crc")));
    assert_eq!(reader.next().await.unwrap(), None);
    reader.seek(5).unwrap();
    assert_eq!(reader.next().await.unwrap(), Some(Bytes::from_static(b"rc")));
}

#[tokio::test]
async fn chunk_bound_batch_rolls_before_a_record_and_binds_each_chunk() {
    let store = Arc::new(MemoryStreamStore::new(20));
    let stream = create_stream(&store, 20, StreamConfig::default()).await;
    let ranges = stream
        .append_chunk_bound_batch(&[Bytes::from_static(b"aa"), Bytes::from_static(b"bb")])
        .await
        .unwrap();
    assert_eq!(ranges.len(), 2);
    assert_eq!((ranges[0].begin, ranges[0].end), (0, 18));
    assert_eq!((ranges[1].begin, ranges[1].end), (18, 36));
    assert_ne!(ranges[0].chunk_id, ranges[1].chunk_id);
    for range in ranges {
        let bytes = stream
            .read_at(range.begin, usize::try_from(range.end - range.begin).unwrap())
            .await
            .unwrap();
        let chunk_id = range.chunk_id.unwrap();
        assert_eq!(&bytes[2..10], &chunk_id.high.to_be_bytes());
        assert_eq!(&bytes[10..18], &chunk_id.low.to_be_bytes());
    }
}

#[tokio::test]
async fn queued_requests_aggregate_after_an_inflight_batch() {
    let store = Arc::new(MemoryStreamStore::new(64));
    store.pause_writes();
    let stream = create_stream(&store, 64, StreamConfig::default()).await;
    let first_stream = stream.clone();
    let first = tokio::spawn(async move { first_stream.append(&[Bytes::from_static(b"a")]).await });
    store.wait_for_write().await;

    let mut pending = Vec::new();
    for byte in *b"bcd" {
        let stream = stream.clone();
        pending.push(tokio::spawn(async move {
            stream.append(&[Bytes::copy_from_slice(&[byte])]).await
        }));
        tokio::task::yield_now().await;
    }
    store.resume_writes();

    let first = first.await.unwrap().unwrap();
    assert_eq!((first.begin, first.end), (0, 1));
    let mut returned = Vec::new();
    for result in pending {
        returned.push(result.await.unwrap().unwrap());
    }
    returned.sort_by_key(|range| range.begin);
    assert_eq!(
        returned
            .iter()
            .map(|range| (range.begin, range.end))
            .collect::<Vec<_>>(),
        vec![(1, 2), (2, 3), (3, 4)]
    );
    assert_eq!(store.chunk_write_count(), 2);
    assert_eq!(store.cursor_advance_count(), 2);
    assert_eq!(stream.read_at(0, 4).await.unwrap(), Bytes::from_static(b"abcd"));
}

#[tokio::test]
async fn rollover_keeps_append_whole_and_reads_across_chunks() {
    let store = Arc::new(MemoryStreamStore::new(5));
    let stream = create_stream(&store, 5, StreamConfig::default()).await;
    assert_eq!(stream.append(&[Bytes::from_static(b"abc")]).await.unwrap().end, 3);
    let range = stream.append(&[Bytes::from_static(b"defg")]).await.unwrap();
    assert_eq!((range.begin, range.end), (3, 7));
    assert_eq!(
        stream.read_at(0, 7).await.unwrap(),
        Bytes::from_static(b"abcdefg")
    );

    let mut reader = stream.read_from(2).unwrap();
    assert_eq!(
        reader.next().await.unwrap().unwrap(),
        Bytes::from_static(b"cdefg")
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn ambiguous_cursor_is_resolved_without_resubmission() {
    let committed_store = Arc::new(MemoryStreamStore::new(32));
    committed_store
        .queue_cursor_outcome(CursorAdvance::Ambiguous, true)
        .await;
    let committed = create_stream(&committed_store, 32, StreamConfig::default()).await;
    assert_eq!(
        committed.append(&[Bytes::from_static(b"yes")]).await.unwrap().end,
        3
    );
    assert_eq!(committed_store.chunk_write_count(), 1);

    let absent_store = Arc::new(MemoryStreamStore::new(31));
    absent_store
        .queue_cursor_outcome(CursorAdvance::Ambiguous, false)
        .await;
    let absent = create_stream(&absent_store, 31, StreamConfig::default()).await;
    assert!(matches!(
        absent.append(&[Bytes::from_static(b"no")]).await,
        Err(StreamError::DefinitelyNotCommitted(_))
    ));
    assert_eq!(absent.tail(), 0);
    assert_eq!(
        absent.append(&[Bytes::from_static(b"later")]).await,
        Err(StreamError::WriteStalled)
    );
    assert_eq!(absent_store.chunk_write_count(), 1);
}

#[tokio::test]
async fn byte_admission_applies_backpressure_while_write_is_pending() {
    let store = Arc::new(MemoryStreamStore::new(64));
    store.pause_writes();
    let config = StreamConfig {
        queue_bytes: 4,
        ..StreamConfig::default()
    };
    let stream = create_stream(&store, 64, config).await;
    let writer = stream.clone();
    let pending = tokio::spawn(async move { writer.append(&[Bytes::from_static(b"1234")]).await });
    store.wait_for_write().await;
    assert_eq!(
        stream.append(&[Bytes::from_static(b"x")]).await,
        Err(StreamError::Backpressure)
    );
    store.resume_writes();
    assert_eq!(pending.await.unwrap().unwrap().end, 4);
}

#[tokio::test]
async fn request_admission_counts_the_inflight_batch() {
    let store = Arc::new(MemoryStreamStore::new(64));
    store.pause_writes();
    let config = StreamConfig {
        queue_requests: 1,
        ..StreamConfig::default()
    };
    let stream = create_stream(&store, 64, config).await;
    let writer = stream.clone();
    let pending = tokio::spawn(async move { writer.append(&[Bytes::from_static(b"first")]).await });
    store.wait_for_write().await;
    assert_eq!(
        stream.append(&[Bytes::from_static(b"second")]).await,
        Err(StreamError::Backpressure)
    );
    store.resume_writes();
    assert_eq!(pending.await.unwrap().unwrap().end, 5);
}

#[tokio::test]
async fn trim_hides_prefix_before_reclaiming_complete_chunks() {
    let store = Arc::new(MemoryStreamStore::new(4));
    let stream = create_stream(&store, 4, StreamConfig::default()).await;
    stream.append(&[Bytes::from_static(b"abcd")]).await.unwrap();
    stream.append(&[Bytes::from_static(b"ef")]).await.unwrap();

    assert_eq!(stream.trim_prefix(4).await.unwrap(), 4);
    assert!(
        store
            .is_released(crowdb_protocol::common::ChunkId { high: 0, low: 1 })
            .await
    );
    assert!(matches!(
        stream.read_at(0, 1).await,
        Err(StreamError::InvalidRequest(_))
    ));
    assert_eq!(stream.read_at(4, 2).await.unwrap(), Bytes::from_static(b"ef"));
    assert_eq!(stream.trim_prefix(4).await.unwrap(), 0);
}

#[tokio::test]
async fn near_tail_seek_loads_only_the_target_extent_page() {
    let store = Arc::new(MemoryStreamStore::new(1));
    let config = StreamConfig {
        extent_page_entries: 2,
        ..StreamConfig::default()
    };
    let stream = create_stream(&store, 1, config).await;
    for byte in *b"abcde" {
        stream.append(&[Bytes::copy_from_slice(&[byte])]).await.unwrap();
    }

    let before = store.extent_page_load_count();
    assert_eq!(stream.read_at(3, 1).await.unwrap(), Bytes::from_static(b"d"));
    assert_eq!(store.extent_page_load_count() - before, 1);
}

#[tokio::test]
async fn reopen_recovers_the_durable_active_tail() {
    let store = Arc::new(MemoryStreamStore::new(32));
    let stream = create_stream(&store, 32, StreamConfig::default()).await;
    let name = StreamName { high: 1, low: 32 };
    stream.append(&[Bytes::from_static(b"old")]).await.unwrap();
    drop(stream);
    tokio::task::yield_now().await;

    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    let reopened = ChunkStream::open(name, 9, StreamConfig::default(), registry, metadata, chunks)
        .await
        .unwrap();
    assert_eq!(reopened.tail(), 3);
    assert_eq!(
        reopened
            .append(&[Bytes::from_static(b"new")])
            .await
            .unwrap()
            .begin,
        3
    );
    assert_eq!(
        reopened.read_at(0, 6).await.unwrap(),
        Bytes::from_static(b"oldnew")
    );
}

#[tokio::test]
async fn watchdog_observes_without_cancelling_or_repeating_after_completion() {
    let store = Arc::new(MemoryStreamStore::new(32));
    store.pause_writes();
    let config = StreamConfig {
        watchdog_interval: std::time::Duration::from_millis(5),
        ..StreamConfig::default()
    };
    let stream = create_stream(&store, 32, config).await;
    let writer = stream.clone();
    let append = tokio::spawn(async move { writer.append(&[Bytes::from_static(b"pending")]).await });
    store.wait_for_write().await;
    tokio::time::sleep(std::time::Duration::from_millis(16)).await;
    assert!(!append.is_finished());
    assert!(stream.metrics().watchdog_observations >= 2);

    store.resume_writes();
    assert_eq!(append.await.unwrap().unwrap().end, 7);
    let completed_observations = stream.metrics().watchdog_observations;
    tokio::time::sleep(std::time::Duration::from_millis(12)).await;
    assert_eq!(stream.metrics().watchdog_observations, completed_observations);
}

#[tokio::test]
async fn higher_epoch_reopens_same_bytes_and_fences_old_writer() {
    let store = Arc::new(MemoryStreamStore::new(32));
    let old = create_stream(&store, 32, StreamConfig::default()).await;
    let name = StreamName { high: 1, low: 32 };
    old.append(&[Bytes::from_static(b"old")]).await.unwrap();

    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    let new = ChunkStream::open(name, 10, StreamConfig::default(), registry, metadata, chunks)
        .await
        .unwrap();
    assert_eq!(new.tail(), 3);
    assert_eq!(
        old.append(&[Bytes::from_static(b"stale")]).await,
        Err(StreamError::StaleWriter)
    );
    assert_eq!(new.append(&[Bytes::from_static(b"new")]).await.unwrap().begin, 3);
    assert_eq!(new.read_at(0, 6).await.unwrap(), Bytes::from_static(b"oldnew"));
}

#[tokio::test]
async fn reopen_repairs_rollover_interrupted_before_manifest_publish() {
    let store = Arc::new(MemoryStreamStore::new(4));
    let stream = create_stream(&store, 4, StreamConfig::default()).await;
    let name = StreamName { high: 1, low: 4 };
    stream.append(&[Bytes::from_static(b"abcd")]).await.unwrap();
    store.fail_next_publish();
    assert!(matches!(
        stream.append(&[Bytes::from_static(b"e")]).await,
        Err(StreamError::Internal(_))
    ));
    drop(stream);
    tokio::task::yield_now().await;

    let registry: Arc<dyn StreamRegistry> = store.clone();
    let metadata: Arc<dyn StreamMetadataStore> = store.clone();
    let chunks: Arc<dyn StreamChunkStore> = store.clone();
    let reopened = ChunkStream::open(name, 9, StreamConfig::default(), registry, metadata, chunks)
        .await
        .unwrap();
    assert_eq!(reopened.tail(), 4);
    assert_eq!(
        reopened.append(&[Bytes::from_static(b"e")]).await.unwrap().begin,
        4
    );
    assert_eq!(
        reopened.read_at(0, 5).await.unwrap(),
        Bytes::from_static(b"abcde")
    );
}
