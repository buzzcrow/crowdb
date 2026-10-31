// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_chunk_stream::{StreamError, StreamPurpose};

#[tokio::test]
async fn reopening_refreshes_cached_capacity_after_another_owner_grows_the_chunk() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let make_chunks = || {
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
    let metadata = Arc::new(MemoryStreamStore::new(1));
    let name = StreamName { high: 277, low: 281 };
    let writer = ChunkStream::create(
        StreamRecord {
            metadata_group_id: 7,
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        },
        9,
        StreamConfig::default(),
        metadata.clone(),
        metadata.clone(),
        make_chunks(),
    )
    .await
    .unwrap();
    let chunk_id = allocator.chunk.lock().unwrap().as_ref().unwrap().id.unwrap();
    let observer = make_chunks();
    assert_eq!(observer.current_capacity(chunk_id).await.unwrap(), 1024 * 1024);
    let payload = Bytes::from(vec![0x69; 1536 * 1024]);
    writer.append(std::slice::from_ref(&payload)).await.unwrap();
    writer.checkpoint().await.unwrap();
    writer.release_writer().await.unwrap();
    let reader = ChunkStream::open_read_only(
        name,
        9,
        StreamConfig::default(),
        metadata.clone(),
        metadata.clone(),
        observer.clone(),
    )
    .await
    .expect("a retained adapter must refresh capacity before validating the newer checkpoint");
    assert_eq!(reader.read_at(0, payload.len()).await.unwrap(), payload);
    allocator.fail_query_once.store(true, Ordering::Release);
    assert!(observer.current_capacity(chunk_id).await.is_err());
    {
        let mut guard = allocator.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        chunk.strips.truncate(1);
        chunk.capacity = 1024;
        chunk.modify_ts += 1;
    }
    let result = ChunkStream::open_read_only(
        name,
        9,
        StreamConfig::default(),
        metadata.clone(),
        metadata,
        observer,
    )
    .await;
    assert!(
        matches!(result, Err(StreamError::Corruption(message)) if message.contains("checkpoint exceeds chunk capacity"))
    );
}
