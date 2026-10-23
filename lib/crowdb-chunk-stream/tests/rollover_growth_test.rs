// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ActiveChunkDescriptor, ChunkStream, CursorAdvance, DurableCursor, Result,
    StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamManifest, StreamMetadataStore,
    StreamName, StreamPurpose, StreamRegistry, TrimmedChunk,
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
    fn mirror_copies(&self) -> u32 {
        self.store.mirror_copies()
    }
    async fn current_capacity(&self, chunk: ChunkId) -> Result<u64> {
        self.store.current_capacity(chunk).await
    }
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

    async fn write_mirrors(
        &self,
        name: StreamName,
        epoch: u64,
        chunk: ChunkId,
        write: crowdb_chunk_stream::StreamMirrorWrite<'_>,
    ) -> Result<()> {
        self.store.write_mirrors(name, epoch, chunk, write).await
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

    async fn renew_liveness(&self, chunk: ChunkId, epoch: u64) -> Result<()> {
        self.store.renew_liveness(chunk, epoch).await
    }

    async fn seal(&self, chunk: ChunkId, epoch: u64, cursor: u64) -> Result<()> {
        self.store.seal(chunk, epoch, cursor).await
    }

    async fn read(&self, chunk: ChunkId, offset: u64, length: usize) -> Result<Bytes> {
        self.store.read(chunk, offset, length).await
    }

    async fn release_trimmed(&self, chunk: ChunkId, end: u64, physical_end: u64) -> Result<TrimmedChunk> {
        self.store.release_trimmed(chunk, end, physical_end).await
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
    assert_eq!(
        store.metadata_publish_count(),
        5,
        "growth changes only chunkinfo; initial head, attachments and append publications remain"
    );
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

#[tokio::test]
async fn stale_stream_capacity_does_not_reject_a_newer_durable_chunk_layout() {
    let store = Arc::new(MemoryStreamStore::new(CAPACITY));
    let chunks = Arc::new(TestGrowingChunks { store: store.clone() });
    let name = StreamName { high: 73, low: 79 };
    let config = StreamConfig {
        chunk_capacity_bytes: CAPACITY,
        ..StreamConfig::default()
    };
    StreamRegistry::create(
        store.as_ref(),
        StreamBinding {
            purpose: StreamPurpose::Stream,
            stream_name: name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("test".into()),
        },
    )
    .await
    .unwrap();
    let active = chunks
        .allocate_mirrored(name, 1, StreamPurpose::Stream)
        .await
        .unwrap();
    let manifest = StreamManifest {
        stream_name: name,
        metadata_group_id: 1,
        writer_epoch: 1,
        generation: 1,
        active: Some(active.clone()),
        ..StreamManifest::default()
    };
    store.publish(None, manifest.clone(), vec![]).await.unwrap();
    let payload = Bytes::from(vec![0x5a; usize::try_from(INITIAL + INITIAL / 2).unwrap()]);
    let frames = crowdb_protocol::frame::encode_append_group_views(active.chunk_id, 0, &payload, 1).unwrap();
    let data = Bytes::from(frames.concat());
    let end = data.len() as u64;
    assert!(end > INITIAL);
    chunks
        .grow_mirrored(name, 1, active.chunk_id, end)
        .await
        .unwrap()
        .unwrap();
    chunks
        .store
        .write_groups_for_tests(name, 1, active.chunk_id, 0, data.clone())
        .await
        .unwrap();
    chunks
        .advance_cursor(name, 1, active.chunk_id, 0, end, crc32fast::hash(&data))
        .await
        .unwrap();
    assert_eq!(store.load_current(name).await.unwrap(), Some(manifest));
    let reader = ChunkStream::open_read_only(
        name,
        1,
        config.clone(),
        store.clone(),
        store.clone(),
        chunks.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        reader.tail(),
        0,
        "the captured directory remains the finite read boundary"
    );
    let reopened = ChunkStream::open(name, 2, config, store.clone(), store.clone(), chunks)
        .await
        .unwrap();
    assert_eq!(reopened.tail(), payload.len() as u64);
    assert_eq!(reopened.read_at(0, payload.len()).await.unwrap(), payload);
    assert_eq!(reader.tail(), 0);
}
