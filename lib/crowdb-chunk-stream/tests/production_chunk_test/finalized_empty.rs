// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn finalized_empty_tail_recovers_without_reviving_the_deleted_chunk() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let make = || {
        ProductionStreamChunkStore::new(
            allocator.clone(),
            disks.clone(),
            30_000,
            ChunkReadPolicy::default(),
        )
        .unwrap()
    };
    let name = StreamName { high: 813, low: 99 };
    let writer = make();
    let mut active = writer
        .allocate_mirrored(name, 9, crowdb_chunk_stream::StreamPurpose::Wal)
        .await
        .unwrap();
    active.logical_start = 42;
    let allocated = allocator.chunk.lock().unwrap().clone().unwrap();
    let deleted = {
        let mut guard = allocator.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        chunk.state = ChunkState::Deleted as i32;
        chunk.sealed_ts_ms = 123;
        chunk.modify_ts += 2;
        chunk.capacity = 0;
        chunk.strips.clear();
        chunk.clone()
    };
    drop(writer);
    let cold = make();
    assert!(matches!(
        cold.seal(active.chunk_id, 10, 0).await,
        Err(crowdb_chunk_stream::StreamError::StaleWriter)
    ));
    let metadata = MemoryStreamStore::new(1024);
    cold.bind_writer(name, 10, metadata.claim_writer(name, 10).await.unwrap());
    let recovered = cold.recover_tail(name, 10, &active).await.unwrap();
    assert_eq!((recovered.physical_end, recovered.logical_end), (0, 42));
    assert!(recovered.extents.is_empty());
    cold.seal(active.chunk_id, 10, 0).await.unwrap();
    assert_eq!(*allocator.chunk.lock().unwrap(), Some(deleted.clone()));
    for field in 0..11 {
        let mut invalid = deleted.clone();
        let mut descriptor = active.clone();
        match field {
            0 => invalid.sealed_ts_ms = 0,
            1 => invalid.capacity = 1,
            2 => invalid.sealed_length = 1,
            3 => invalid.acknowledged_cursor = 1,
            4 => descriptor.physical_start = 1,
            5 => descriptor.acknowledged_cursor = 1,
            6 => invalid.writer_epoch = 0,
            7 => invalid.chunk_type = crowdb_protocol::chunkdb::rpc::ChunkType::S3 as i32,
            8 => invalid
                .cleanup_intents
                .push(crowdb_protocol::chunkdb::rpc::StripCleanupIntent::default()),
            9 => invalid.strips.clone_from(&allocated.strips),
            _ => invalid.state = ChunkState::Active as i32,
        }
        *allocator.chunk.lock().unwrap() = Some(invalid);
        assert!(matches!(
            cold.recover_tail(name, 10, &descriptor).await,
            Err(crowdb_chunk_stream::StreamError::Corruption(_))
        ));
    }
}

#[tokio::test]
async fn finalized_empty_stream_tail_reopens_under_a_new_writer_with_a_fresh_chunk() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let make = || {
        Arc::new(
            ProductionStreamChunkStore::new(
                allocator.clone(),
                disks.clone(),
                30_000,
                ChunkReadPolicy::default(),
            )
            .unwrap(),
        )
    };
    let metadata = Arc::new(MemoryStreamStore::new(1024));
    let name = StreamName { high: 814, low: 100 };
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: crowdb_chunk_stream::StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: None,
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        9,
        StreamConfig::default(),
        metadata.clone(),
        metadata.clone(),
        make(),
    )
    .await
    .unwrap();
    stream.release_writer().await.unwrap();
    let old = {
        let mut guard = allocator.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        chunk.state = ChunkState::Deleted as i32;
        chunk.sealed_ts_ms = 123;
        chunk.modify_ts += 2;
        chunk.capacity = 0;
        chunk.strips.clear();
        chunk.clone()
    };
    let reopened = ChunkStream::open(
        name,
        10,
        StreamConfig::default(),
        metadata.clone(),
        metadata,
        make(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.tail(), 0);
    assert_eq!(*allocator.chunk.lock().unwrap(), Some(old.clone()));
    let appended = reopened
        .append(&[Bytes::from_static(b"fresh-writer")])
        .await
        .unwrap();
    assert_ne!(appended.chunk_id, old.id);
    assert_eq!(
        reopened.read_at(0, 12).await.unwrap(),
        Bytes::from_static(b"fresh-writer")
    );
    reopened.release_writer().await.unwrap();
}
