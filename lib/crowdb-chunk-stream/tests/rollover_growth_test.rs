// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ActiveChunkDescriptor, ChunkStream, CursorAdvance, DurableCursor,
    MirrorStripImage, Result, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamName,
    StreamPurpose, TrimmedChunk,
};
use crowdb_protocol::common::ChunkId;

const INITIAL: u64 = 1024 * 1024;
const CAPACITY: u64 = 2 * INITIAL;

/// Models production allocation: one strip initially, bounded growth afterward.
struct TestGrowingChunks {
    store: Arc<MemoryStreamStore>,
}

#[async_trait]
impl StreamChunkStore for TestGrowingChunks {
    async fn allocate_mirrored(
        &self,
        name: StreamName,
        epoch: u64,
        purpose: StreamPurpose,
    ) -> Result<ActiveChunkDescriptor> {
        let mut active = self.store.allocate_mirrored(name, epoch, purpose).await?;
        active.capacity = INITIAL;
        Ok(active)
    }

    async fn grow_mirrored(
        &self,
        _name: StreamName,
        epoch: u64,
        chunk: ChunkId,
        required: u64,
    ) -> Result<Option<ActiveChunkDescriptor>> {
        if required > CAPACITY {
            return Ok(None);
        }
        let cursor = self.store.durable_cursor(chunk, epoch).await?;
        Ok(Some(ActiveChunkDescriptor {
            chunk_id: chunk,
            physical_start: cursor.offset,
            logical_start: 0,
            acknowledged_cursor: cursor.offset,
            capacity: CAPACITY,
        }))
    }

    async fn write_mirrors_with_images(
        &self,
        name: StreamName,
        epoch: u64,
        chunk: ChunkId,
        offset: u64,
        data: Bytes,
        images: &[MirrorStripImage],
    ) -> Result<()> {
        self.store
            .write_mirrors_with_images(name, epoch, chunk, offset, data, images)
            .await
    }

    async fn advance_cursor(
        &self,
        name: StreamName,
        epoch: u64,
        chunk: ChunkId,
        expected: u64,
        new: u64,
        checksum: u32,
    ) -> Result<CursorAdvance> {
        self.store
            .advance_cursor(name, epoch, chunk, expected, new, checksum)
            .await
    }

    async fn durable_cursor(&self, chunk: ChunkId, epoch: u64) -> Result<DurableCursor> {
        self.store.durable_cursor(chunk, epoch).await
    }

    async fn seal(&self, chunk: ChunkId, epoch: u64, cursor: u64) -> Result<()> {
        self.store.seal(chunk, epoch, cursor).await
    }

    async fn read(&self, chunk: ChunkId, offset: u64, length: usize) -> Result<Bytes> {
        self.store.read(chunk, offset, length).await
    }

    async fn release_trimmed(&self, chunk: ChunkId, end: u64) -> Result<TrimmedChunk> {
        self.store.release_trimmed(chunk, end).await
    }
}

#[tokio::test]
async fn rollover_grows_a_fresh_successor_before_admitting_the_waiting_append() {
    let store = Arc::new(MemoryStreamStore::new(CAPACITY));
    let chunks = Arc::new(TestGrowingChunks { store: store.clone() });
    let name = StreamName { high: 31, low: 41 };
    let config = StreamConfig {
        chunk_capacity_bytes: CAPACITY,
        ..StreamConfig::default()
    };
    let stream = ChunkStream::create(
        StreamBinding {
            purpose: StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
        1,
        config.clone(),
        store.clone(),
        store.clone(),
        chunks.clone(),
    )
    .await
    .unwrap();
    let payload = Bytes::from(vec![0x5a; usize::try_from(INITIAL + INITIAL / 2).unwrap()]);
    let first = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    let second = stream.append(std::slice::from_ref(&payload)).await.unwrap();
    assert_ne!(first.chunk_id, second.chunk_id);
    assert_eq!(second.begin, first.end);
    assert_eq!(stream.metrics().rollovers, 1);
    let reopened = ChunkStream::open_read_only(name, 1, config, store.clone(), store, chunks)
        .await
        .unwrap();
    assert_eq!(reopened.tail(), second.end);
    for range in [first, second] {
        assert_eq!(
            reopened.read_at(range.begin, payload.len()).await.unwrap(),
            payload
        );
    }
}
