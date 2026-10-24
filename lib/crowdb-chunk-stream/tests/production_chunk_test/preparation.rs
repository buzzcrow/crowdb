// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn preparation_revision_does_not_abort_or_reinitialize_mirror_repair() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 241, low: 251 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    allocator.conflict_replacement_once.store(true, Ordering::Release);
    disks.fail_disk_once.store(1, Ordering::Release);
    let end = TestGroupedStrip::default()
        .append(&store, name, 9, active.chunk_id, b"retained")
        .await;
    assert!(end > 0);
    assert_eq!(allocator.replacement_calls.load(Ordering::Acquire), 2);
    assert_eq!(allocator.replacements.load(Ordering::Acquire), 1);
    let writes = disks.writes.lock().unwrap();
    assert_eq!(
        writes
            .iter()
            .filter(|(segment, _, _, _)| segment.disk_id.is_some_and(|id| id.high == 101))
            .count(),
        1
    );
}

#[tokio::test]
async fn growth_publication_preserves_cursor_progress_committed_during_allocation() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 233, low: 239 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let end = TestGroupedStrip::default()
        .append(&store, name, 9, active.chunk_id, b"retained")
        .await;
    allocator.advance_during_growth.store(end, Ordering::Release);
    let prepared = store
        .grow_mirrored(name, 9, active.chunk_id, active.capacity + 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.acknowledged_cursor, end);
    let writes = disks.writes.lock().unwrap().len();
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, end, end, 17)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    assert_eq!(disks.writes.lock().unwrap().len(), writes);
}

#[tokio::test]
async fn temporary_allocation_failure_does_not_poison_later_capacity() {
    let allocator = Arc::new(Allocator::new());
    allocator.pause_growth.store(true, Ordering::Release);
    allocator.fail_growth_once.store(true, Ordering::Release);
    let stream = test_stream(allocator.clone(), Arc::new(Disks::default())).await;
    stream.append(&[Bytes::from_static(b"retained")]).await.unwrap();
    timeout(Duration::from_secs(1), allocator.growth_started.notified())
        .await
        .unwrap();
    allocator.growth_resume.notify_one();
    let data = Bytes::from(vec![0x61; 1024 * 1024]);
    let range = timeout(Duration::from_secs(1), stream.append(std::slice::from_ref(&data)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(range.begin, 8);
    assert_eq!(stream.read_at(range.begin, data.len()).await.unwrap(), data);
    stream.close().await.unwrap();
}

#[tokio::test]
async fn uncertain_unused_candidate_is_reclaimed_and_replaced_without_reinitialization() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    // The primary chunk uses disks 1/2, 11/12, ...; its first candidate
    // has distinct disks 1001/1002, 1011/1012, ... . Fail candidate growth.
    disks.fail_fsync_once.store(1011, Ordering::Release);
    let stream = test_stream(allocator.clone(), disks.clone()).await;
    stream.append(&[Bytes::from_static(b"retained")]).await.unwrap();
    timeout(Duration::from_secs(1), async {
        while allocator.successors.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stream.close().await.unwrap();
    {
        let candidates = allocator.successors.lock().unwrap();
        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .values()
            .all(|chunk| chunk.state == ChunkState::Deleted as i32 && chunk.acknowledged_cursor == 0));
    }
    assert_eq!(
        disks
            .writes
            .lock()
            .unwrap()
            .iter()
            .filter(|(segment, ..)| segment.disk_id.unwrap().high == 1011)
            .count(),
        1,
        "uncertain initialization must never repeat on that candidate"
    );
    assert_eq!(stream.metrics().failed, 0);
    assert_eq!(stream.metrics().rollovers, 0);
    assert_eq!(
        stream.read_at(0, 8).await.unwrap(),
        Bytes::from_static(b"retained")
    );
}

async fn test_stream(allocator: Arc<Allocator>, disks: Arc<Disks>) -> ChunkStream {
    let metadata = Arc::new(MemoryStreamStore::new(256 * 1024 * 1024));
    let chunks = Arc::new(
        ProductionStreamChunkStore::new(allocator, disks, 30_000, ChunkReadPolicy::default()).unwrap(),
    );
    ChunkStream::create(
        StreamRecord {
            purpose: crowdb_chunk_stream::StreamPurpose::Stream,
            stream_name: StreamName { high: 211, low: 223 },
            metadata_group_id: 1,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("test".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        9,
        StreamConfig::default(),
        metadata.clone(),
        metadata,
        chunks,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn allocation_in_flight_does_not_block_writes_in_ready_capacity() {
    let allocator = Arc::new(Allocator::new());
    allocator.pause_growth.store(true, Ordering::Release);
    let stream = test_stream(allocator.clone(), Arc::new(Disks::default())).await;
    let payload = Bytes::from(vec![0x51; 4096]);
    timeout(
        Duration::from_secs(1),
        stream.append(std::slice::from_ref(&payload)),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(Duration::from_secs(1), allocator.growth_started.notified())
        .await
        .unwrap();
    let range = timeout(
        Duration::from_secs(1),
        stream.append(std::slice::from_ref(&payload)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(range.begin, 4096);
    allocator.growth_resume.notify_one();
    stream.close().await.unwrap();
    assert_eq!(stream.read_at(range.begin, payload.len()).await.unwrap(), payload);
}

#[tokio::test]
async fn only_a_write_reaching_uninitialized_capacity_waits() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    disks.pause_initialization.store(true, Ordering::Release);
    let stream = test_stream(allocator.clone(), disks.clone()).await;
    stream.append(&[Bytes::from(vec![0x31; 4096])]).await.unwrap();
    timeout(Duration::from_secs(1), disks.initialization_started.notified())
        .await
        .unwrap();
    timeout(
        Duration::from_secs(1),
        stream.append(&[Bytes::from(vec![0x32; 4096])]),
    )
    .await
    .unwrap()
    .unwrap();
    let large = Bytes::from(vec![0x33; 1024 * 1024]);
    let writer = stream.clone();
    let data = large.clone();
    let mut pending = tokio::spawn(async move { writer.append(&[data]).await });
    assert!(timeout(Duration::from_millis(20), &mut pending).await.is_err());
    assert_eq!(stream.tail(), 8192);
    disks.initialization_resume.notify_one();
    let range = timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(range.begin, 8192);
    assert_eq!(stream.read_at(range.begin, large.len()).await.unwrap(), large);
    stream.close().await.unwrap();
}

#[tokio::test]
async fn close_drains_preparation_before_sealing() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    disks.pause_initialization.store(true, Ordering::Release);
    let stream = test_stream(allocator.clone(), disks.clone()).await;
    stream.append(&[Bytes::from_static(b"retained")]).await.unwrap();
    timeout(Duration::from_secs(1), disks.initialization_started.notified())
        .await
        .unwrap();
    let mut closing = tokio::spawn(async move { stream.close().await });
    assert!(timeout(Duration::from_millis(20), &mut closing).await.is_err());
    assert_eq!(
        allocator.chunk.lock().unwrap().as_ref().unwrap().state,
        ChunkState::Active as i32
    );
    disks.initialization_resume.notify_one();
    timeout(Duration::from_secs(1), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        allocator.chunk.lock().unwrap().as_ref().unwrap().state,
        ChunkState::Sealed as i32
    );
}

#[tokio::test]
async fn failed_preparation_never_exposes_or_reinitializes_the_new_suffix() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    disks.fail_fsync_once.store(11, Ordering::Release);
    let stream = test_stream(allocator, disks.clone()).await;
    stream.append(&[Bytes::from_static(b"retained")]).await.unwrap();
    assert!(stream
        .append(&[Bytes::from(vec![0x41; 1024 * 1024])])
        .await
        .is_err());
    assert_eq!(stream.tail(), 8);
    assert_eq!(
        stream.read_at(0, 8).await.unwrap(),
        Bytes::from_static(b"retained")
    );
    let attempts = disks.writes.lock().unwrap().len();
    assert!(stream.append(&[Bytes::from_static(b"later")]).await.is_err());
    assert_eq!(disks.writes.lock().unwrap().len(), attempts);
}

#[tokio::test]
async fn definite_revision_conflict_rebases_cursor_without_replaying_data() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let store = ProductionStreamChunkStore::new(
        allocator.clone(),
        disks.clone(),
        30_000,
        ChunkReadPolicy::default(),
    )
    .unwrap();
    let name = StreamName { high: 227, low: 229 };
    let active = store
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Stream)
        .await
        .unwrap();
    let end = TestGroupedStrip::default()
        .append(&store, name, 9, active.chunk_id, b"retained")
        .await;
    let writes = disks.writes.lock().unwrap().len();
    allocator.conflict_advance_once.store(true, Ordering::Release);
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, 0, end, 17)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    assert_eq!(allocator.advance_calls.load(Ordering::Acquire), 2);
    assert_eq!(disks.writes.lock().unwrap().len(), writes);
}
