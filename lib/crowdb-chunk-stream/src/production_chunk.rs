// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free production chunk-store adapter for direct mirrored streams.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crowdb_chunk_client::{
    ChunkAllocator, ChunkReadPolicy, ChunkReader, DiskWriter, MirrorChunkWriter, ProtoLocation,
};
use crowdb_protocol::chunk_stream::{ActiveChunkDescriptor, StreamName};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, Chunk, ChunkState, DeleteChunkRequest, QueryChunkRequest, SealChunkRequest,
    Strip,
};
use crowdb_protocol::common::ChunkId;

use crate::{CursorAdvance, DurableCursor, Result, StreamChunkStore, StreamError, TrimmedChunk};

struct ChunkStateView {
    chunk: Chunk,
    modify_ts: AtomicU64,
    cursor: AtomicU64,
    sealed: AtomicBool,
    last_checksum: AtomicU32,
    has_checksum: AtomicBool,
}

/// Production `StreamChunkStore` using direct three-copy mirror writes and the
/// unified chunk reader. Per-chunk mutable metadata is atomic; the stream's
/// single-owner worker remains the only operation sequencer.
pub struct ProductionStreamChunkStore {
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    reader: ChunkReader,
    writer_lease_ms: u64,
    chunks: SkipMap<(u64, u64), Arc<ChunkStateView>>,
}

impl ProductionStreamChunkStore {
    /// Creates a production adapter.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid lease or read policy.
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        writer_lease_ms: u64,
        read_policy: ChunkReadPolicy,
    ) -> Result<Self> {
        if writer_lease_ms == 0 {
            return Err(StreamError::InvalidRequest(
                "stream chunk writer lease must be nonzero".into(),
            ));
        }
        let reader = ChunkReader::new(Arc::clone(&allocator), Arc::clone(&disk_writer), read_policy)
            .map_err(read_error)?;
        Ok(Self {
            allocator,
            disk_writer,
            reader,
            writer_lease_ms,
            chunks: SkipMap::new(),
        })
    }

    fn install(&self, chunk: Chunk) -> Result<Arc<ChunkStateView>> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| StreamError::Corruption("chunk metadata has no identity".into()))?;
        let key = (chunk_id.high, chunk_id.low);
        if let Some(existing) = self.chunks.get(&key) {
            return Ok(Arc::clone(existing.value()));
        }
        let view = Arc::new(ChunkStateView {
            modify_ts: AtomicU64::new(chunk.modify_ts),
            cursor: AtomicU64::new(chunk.acknowledged_cursor),
            sealed: AtomicBool::new(chunk.state == ChunkState::Sealed as i32),
            last_checksum: AtomicU32::new(0),
            has_checksum: AtomicBool::new(false),
            chunk,
        });
        let installed = self.chunks.get_or_insert(key, view);
        Ok(Arc::clone(installed.value()))
    }

    async fn state(&self, chunk_id: ChunkId) -> Result<Arc<ChunkStateView>> {
        if let Some(existing) = self.chunks.get(&(chunk_id.high, chunk_id.low)) {
            return Ok(Arc::clone(existing.value()));
        }
        let response = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await
            .map_err(io_error)?;
        let chunk = response
            .chunk
            .ok_or_else(|| StreamError::ReadUnavailable("stream chunk is missing".into()))?;
        self.install(chunk)
    }
}

#[async_trait]
impl StreamChunkStore for ProductionStreamChunkStore {
    async fn allocate_mirrored(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ActiveChunkDescriptor> {
        let writer = MirrorChunkWriter::allocate(
            Arc::clone(&self.allocator),
            Arc::clone(&self.disk_writer),
            writer_epoch,
            self.writer_lease_ms,
        )
        .await
        .map_err(io_error)?;
        let chunk_id = writer.chunk_id();
        let capacity = writer.capacity();
        let cursor = writer.cursor();
        self.install(writer.chunk().clone())?;
        Ok(ActiveChunkDescriptor {
            chunk_id,
            physical_start: 0,
            logical_start: 0,
            acknowledged_cursor: cursor,
            capacity,
        })
    }

    async fn write_mirrors(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        physical_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let state = self.state(chunk_id).await?;
        if state.chunk.writer_epoch != writer_epoch
            || state.sealed.load(Ordering::Acquire)
            || state.cursor.load(Ordering::Acquire) != physical_offset
        {
            return Err(StreamError::StaleWriter);
        }
        let strip = state
            .chunk
            .strips
            .first()
            .ok_or_else(|| StreamError::Corruption("stream chunk has no strip".into()))?;
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(StreamError::Corruption(
                "stream chunk strip is not mirrored".into(),
            ));
        };
        if mirror.segments.len() != 3 {
            return Err(StreamError::Corruption(
                "stream chunk does not have three mirrors".into(),
            ));
        }
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut writes = tokio::task::JoinSet::new();
        for segment in &mirror.segments {
            let segment = *segment;
            let disk_writer = Arc::clone(&self.disk_writer);
            let data = data.clone();
            writes.spawn(async move {
                disk_writer
                    .write_at_byte_offset(&segment, unit_bytes, physical_offset, data)
                    .await
            });
        }
        while let Some(result) = writes.join_next().await {
            result
                .map_err(|error| StreamError::Internal(format!("mirror write task failed: {error}")))?
                .map_err(io_error)?;
        }
        Ok(())
    }

    async fn advance_cursor(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        expected_cursor: u64,
        new_cursor: u64,
        checksum: u32,
    ) -> Result<CursorAdvance> {
        let state = self.state(chunk_id).await?;
        if state.chunk.writer_epoch != writer_epoch || state.cursor.load(Ordering::Acquire) != expected_cursor
        {
            return Ok(CursorAdvance::DefinitelyNotCommitted);
        }
        let advance = self
            .allocator
            .advance_chunk_write(AdvanceChunkWriteRequest {
                chunk_id: Some(chunk_id),
                writer_epoch,
                expected_modify_ts: state.modify_ts.load(Ordering::Acquire),
                acknowledged_cursor: new_cursor,
                closed_strip_sequence: None,
                writer_lease_ms: self.writer_lease_ms,
            })
            .await;
        if let Ok(response) = advance {
            let chunk = response
                .chunk
                .ok_or_else(|| StreamError::Corruption("cursor advance returned no chunk".into()))?;
            if chunk.id != Some(chunk_id)
                || chunk.writer_epoch != writer_epoch
                || chunk.acknowledged_cursor != new_cursor
            {
                return Err(StreamError::Corruption(
                    "cursor advance returned inconsistent metadata".into(),
                ));
            }
            state.modify_ts.store(chunk.modify_ts, Ordering::Release);
            state.cursor.store(new_cursor, Ordering::Release);
            state.last_checksum.store(checksum, Ordering::Release);
            state.has_checksum.store(true, Ordering::Release);
            return Ok(CursorAdvance::Committed);
        }
        let response = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await;
        let Ok(response) = response else {
            return Ok(CursorAdvance::Ambiguous);
        };
        let Some(chunk) = response.chunk else {
            return Ok(CursorAdvance::Ambiguous);
        };
        state.modify_ts.store(chunk.modify_ts, Ordering::Release);
        state.cursor.store(chunk.acknowledged_cursor, Ordering::Release);
        if chunk.acknowledged_cursor == new_cursor {
            state.last_checksum.store(checksum, Ordering::Release);
            state.has_checksum.store(true, Ordering::Release);
            Ok(CursorAdvance::Committed)
        } else if chunk.acknowledged_cursor == expected_cursor {
            Ok(CursorAdvance::DefinitelyNotCommitted)
        } else {
            Ok(CursorAdvance::Ambiguous)
        }
    }

    async fn durable_cursor(&self, chunk_id: ChunkId, writer_epoch: u64) -> Result<DurableCursor> {
        let response = self
            .allocator
            .query_chunk(QueryChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await
            .map_err(io_error)?;
        let chunk = response
            .chunk
            .ok_or_else(|| StreamError::ReadUnavailable("stream chunk is missing".into()))?;
        if chunk.writer_epoch > writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        let state = self.install(chunk.clone())?;
        state.modify_ts.store(chunk.modify_ts, Ordering::Release);
        state.cursor.store(chunk.acknowledged_cursor, Ordering::Release);
        state
            .sealed
            .store(chunk.state == ChunkState::Sealed as i32, Ordering::Release);
        Ok(DurableCursor {
            offset: chunk.acknowledged_cursor,
            last_advance_checksum: state
                .has_checksum
                .load(Ordering::Acquire)
                .then(|| state.last_checksum.load(Ordering::Acquire)),
            sealed: chunk.state == ChunkState::Sealed as i32,
        })
    }

    async fn seal(&self, chunk_id: ChunkId, writer_epoch: u64, cursor: u64) -> Result<()> {
        let state = self.state(chunk_id).await?;
        if state.chunk.writer_epoch > writer_epoch || state.cursor.load(Ordering::Acquire) != cursor {
            return Err(StreamError::StaleWriter);
        }
        let response = self
            .allocator
            .seal_chunk(SealChunkRequest {
                chunk_id: Some(chunk_id),
                seal_length: u32::try_from(cursor.div_ceil(1024)).unwrap_or(u32::MAX),
            })
            .await
            .map_err(io_error)?;
        let chunk = response
            .chunk
            .ok_or_else(|| StreamError::Corruption("seal returned no chunk".into()))?;
        if chunk.id != Some(chunk_id)
            || chunk.state != ChunkState::Sealed as i32
            || chunk.acknowledged_cursor < cursor
        {
            return Err(StreamError::Corruption(
                "seal returned inconsistent stream chunk".into(),
            ));
        }
        state.sealed.store(true, Ordering::Release);
        Ok(())
    }

    async fn read(&self, chunk_id: ChunkId, physical_offset: u64, length: usize) -> Result<Bytes> {
        let length = u64::try_from(length)
            .map_err(|_| StreamError::InvalidRequest("chunk read length exceeds u64".into()))?;
        self.reader
            .read_range(
                &[ProtoLocation {
                    chunk_id: Some(chunk_id),
                    offset: physical_offset,
                    length,
                    logical_offset: 0,
                    logical_length: length,
                }],
                0,
                length,
            )
            .await
            .map_err(read_error)
    }

    async fn release_trimmed(&self, chunk_id: ChunkId, _logical_end: u64) -> Result<TrimmedChunk> {
        let state = self.state(chunk_id).await?;
        let reclaimed_bytes = state.cursor.load(Ordering::Acquire);
        self.allocator
            .delete_chunk(DeleteChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await
            .map_err(io_error)?;
        Ok(TrimmedChunk {
            chunk_id,
            reclaimed_bytes,
        })
    }
}

#[allow(clippy::needless_pass_by_value)]
fn io_error(error: crowdb_chunk_client::IoError) -> StreamError {
    StreamError::Internal(format!("stream chunk IO failed: {error}"))
}

#[allow(clippy::needless_pass_by_value)]
fn read_error(error: crowdb_chunk_client::ReadError) -> StreamError {
    StreamError::ReadUnavailable(error.to_string())
}
