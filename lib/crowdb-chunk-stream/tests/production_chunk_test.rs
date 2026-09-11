// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkAllocator, ChunkReadPolicy, DiskWriter, IoError};
use crowdb_chunk_stream::{CursorAdvance, ProductionStreamChunkStore, StreamChunkStore, StreamName};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AdvanceChunkWriteResponse, AllocateChunkRequest, AllocateChunkResponse,
    AppendChunkRequest, AppendChunkResponse, Chunk, ChunkState, ChunkStrip, DeleteChunkRequest,
    DeleteChunkResponse, MirrorStrip, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, Strip, StripType, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;

struct Allocator {
    chunk: Mutex<Option<Chunk>>,
}

impl Allocator {
    fn new() -> Self {
        Self {
            chunk: Mutex::new(None),
        }
    }
}

#[async_trait]
impl ChunkAllocator for Allocator {
    async fn allocate_chunk(
        &self,
        request: AllocateChunkRequest,
    ) -> crowdb_chunk_client::Result<AllocateChunkResponse> {
        let chunk_id = ChunkId { high: 7, low: 8 };
        let segments = (1..=3)
            .map(|disk| Segment {
                disk_id: Some(DiskId { high: disk, low: 0 }),
                owner_chunk: Some(chunk_id),
                unit_offset: 0,
                zone_index: 0,
                unit_count: 65_536,
                allocation_ts: 1,
            })
            .collect();
        let chunk = Chunk {
            id: Some(chunk_id),
            modify_ts: 1,
            state: ChunkState::Active as i32,
            capacity: 256 * 1024,
            strips: vec![ChunkStrip {
                chunk_offset: 0,
                strip_sequence: 0,
                unit_kb: 4,
                capacity: 256 * 1024,
                strip_type: StripType::Mirror as i32,
                strip: Some(Strip::MirrorStrip(MirrorStrip { segments })),
                ..ChunkStrip::default()
            }],
            chunk_type: request.chunk_type,
            writer_epoch: request.writer_epoch,
            writer_lease_deadline_ms: request.writer_lease_ms,
            next_strip_sequence: 1,
            ..Chunk::default()
        };
        *self.chunk.lock().unwrap() = Some(chunk.clone());
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(
        &self,
        _request: AppendChunkRequest,
    ) -> crowdb_chunk_client::Result<AppendChunkResponse> {
        unreachable!()
    }

    async fn advance_chunk_write(
        &self,
        request: AdvanceChunkWriteRequest,
    ) -> crowdb_chunk_client::Result<AdvanceChunkWriteResponse> {
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        if chunk.id != request.chunk_id
            || chunk.writer_epoch != request.writer_epoch
            || chunk.modify_ts != request.expected_modify_ts
        {
            return Err(IoError::MetadataConflict("stale cursor advance".into()));
        }
        chunk.modify_ts += 1;
        chunk.acknowledged_cursor = request.acknowledged_cursor;
        Ok(AdvanceChunkWriteResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn seal_chunk(&self, request: SealChunkRequest) -> crowdb_chunk_client::Result<SealChunkResponse> {
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        assert_eq!(chunk.id, request.chunk_id);
        chunk.state = ChunkState::Sealed as i32;
        chunk.sealed_length = request.seal_length;
        Ok(SealChunkResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn delete_chunk(
        &self,
        request: DeleteChunkRequest,
    ) -> crowdb_chunk_client::Result<DeleteChunkResponse> {
        let mut guard = self.chunk.lock().unwrap();
        let chunk = guard.as_mut().unwrap();
        assert_eq!(chunk.id, request.chunk_id);
        chunk.state = ChunkState::Deleted as i32;
        Ok(DeleteChunkResponse {
            chunk: Some(chunk.clone()),
        })
    }

    async fn update_chunk_strip(
        &self,
        _request: UpdateChunkStripRequest,
    ) -> crowdb_chunk_client::Result<UpdateChunkStripResponse> {
        unreachable!()
    }

    async fn query_chunk(
        &self,
        request: QueryChunkRequest,
    ) -> crowdb_chunk_client::Result<QueryChunkResponse> {
        let chunk = self.chunk.lock().unwrap().clone();
        if chunk.as_ref().and_then(|chunk| chunk.id) != request.chunk_id {
            return Err(IoError::ChunkNotFound("missing test chunk".into()));
        }
        Ok(QueryChunkResponse {
            chunk,
            layout_validity_ms: 60_000,
        })
    }
}

#[derive(Default)]
struct Disks {
    bytes: Mutex<HashMap<u64, Vec<u8>>>,
}

#[async_trait]
impl DiskWriter for Disks {
    async fn write(
        &self,
        _segment: &Segment,
        _unit_bytes: u64,
        _data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        unreachable!()
    }

    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        _unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        let disk = segment.disk_id.unwrap().high;
        let mut disks = self.bytes.lock().unwrap();
        let bytes = disks.entry(disk).or_default();
        let start = usize::try_from(offset).unwrap();
        bytes.resize(bytes.len().max(start + data.len()), 0);
        bytes[start..start + data.len()].copy_from_slice(&data);
        Ok(())
    }

    async fn read(
        &self,
        segment: &Segment,
        _unit_bytes: u64,
        offset: u64,
        length: u32,
    ) -> crowdb_chunk_client::Result<Bytes> {
        let disk = segment.disk_id.unwrap().high;
        let disks = self.bytes.lock().unwrap();
        let bytes = disks
            .get(&disk)
            .ok_or_else(|| IoError::ReadFailed("missing disk".into()))?;
        let start = usize::try_from(offset).unwrap();
        let end = start + usize::try_from(length).unwrap();
        Ok(Bytes::copy_from_slice(&bytes[start..end]))
    }
}

#[tokio::test]
async fn production_store_writes_reads_advances_and_releases_one_mirror_chunk() {
    let allocator = Arc::new(Allocator::new());
    let disks = Arc::new(Disks::default());
    let allocator_trait: Arc<dyn ChunkAllocator> = allocator;
    let disk_trait: Arc<dyn DiskWriter> = disks;
    let store =
        ProductionStreamChunkStore::new(allocator_trait, disk_trait, 30_000, ChunkReadPolicy::default())
            .unwrap();
    let name = StreamName { high: 1, low: 2 };
    let active = store.allocate_mirrored(name, 9).await.unwrap();
    store
        .write_mirrors(name, 9, active.chunk_id, 0, Bytes::from_static(b"stream"))
        .await
        .unwrap();
    assert_eq!(
        store
            .advance_cursor(name, 9, active.chunk_id, 0, 6, 17)
            .await
            .unwrap(),
        CursorAdvance::Committed
    );
    assert_eq!(
        store.read(active.chunk_id, 0, 6).await.unwrap(),
        Bytes::from_static(b"stream")
    );
    store.seal(active.chunk_id, 9, 6).await.unwrap();
    assert_eq!(
        store
            .release_trimmed(active.chunk_id, 6)
            .await
            .unwrap()
            .reclaimed_bytes,
        6
    );
}
