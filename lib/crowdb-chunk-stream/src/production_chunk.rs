// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free production chunk-store adapter for direct mirrored streams.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crowdb_chunk_client::{
    ChunkAllocator, ChunkReadPolicy, ChunkReader, DiskWriter, FailedDiskList, MirrorChunkWriter,
    MirrorStripFlow, ProtoLocation,
};
use crowdb_protocol::chunk_stream::{ActiveChunkDescriptor, StreamName};
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AppendChunkRequest, Chunk, ChunkState, DeleteChunkRequest, QueryChunkRequest,
    SealChunkRequest, Strip, StripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::FrameMagic;

use crate::{
    CursorAdvance, DurableCursor, MirrorStripImage, Result, StreamChunkStore, StreamError, TrimmedChunk,
};

struct ChunkStateView {
    chunk: ArcSwap<Chunk>,
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
    mirror_copies: u32,
    failed_disks: Arc<FailedDiskList>,
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
        Self::new_with_mirror_copies(allocator, disk_writer, writer_lease_ms, read_policy, 3)
    }

    /// Creates a production adapter with an explicit stream mirror count.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid lease, mirror count, or read policy.
    pub fn new_with_mirror_copies(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        writer_lease_ms: u64,
        read_policy: ChunkReadPolicy,
        mirror_copies: u32,
    ) -> Result<Self> {
        if writer_lease_ms == 0 || mirror_copies == 0 {
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
            mirror_copies,
            failed_disks: Arc::new(FailedDiskList::new(Duration::from_secs(60))),
            chunks: SkipMap::new(),
        })
    }

    fn install(&self, chunk: Chunk) -> Result<Arc<ChunkStateView>> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| StreamError::Corruption("chunk metadata has no identity".into()))?;
        let key = (chunk_id.high, chunk_id.low);
        if let Some(existing) = self.chunks.get(&key) {
            let existing = Arc::clone(existing.value());
            update_state(&existing, chunk);
            return Ok(existing);
        }
        let view = Arc::new(ChunkStateView {
            modify_ts: AtomicU64::new(chunk.modify_ts),
            cursor: AtomicU64::new(chunk.acknowledged_cursor),
            sealed: AtomicBool::new(chunk.state == ChunkState::Sealed as i32),
            last_checksum: AtomicU32::new(0),
            has_checksum: AtomicBool::new(false),
            chunk: ArcSwap::from_pointee(chunk),
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

fn chunk_capacity(chunk: &Chunk) -> Result<u64> {
    chunk
        .strips
        .iter()
        .try_fold(0_u64, |total, strip| {
            total
                .checked_add(u64::from(strip.capacity) * 1024)
                .ok_or_else(|| StreamError::Corruption("stream chunk capacity overflows".into()))
        })
        .map(|capacity| capacity.min(crowdb_chunk_client::STREAM_CHUNK_BYTES))
}

fn update_state(state: &ChunkStateView, chunk: Chunk) {
    state.modify_ts.store(chunk.modify_ts, Ordering::Release);
    state.cursor.store(chunk.acknowledged_cursor, Ordering::Release);
    state
        .sealed
        .store(chunk.state == ChunkState::Sealed as i32, Ordering::Release);
    state.chunk.store(Arc::new(chunk));
}

#[async_trait]
impl StreamChunkStore for ProductionStreamChunkStore {
    async fn allocate_mirrored(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ActiveChunkDescriptor> {
        let writer = MirrorChunkWriter::allocate_with_copy_count(
            Arc::clone(&self.allocator),
            Arc::clone(&self.disk_writer),
            stream_name,
            writer_epoch,
            self.writer_lease_ms,
            self.mirror_copies,
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

    async fn grow_mirrored(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        required_capacity: u64,
    ) -> Result<Option<ActiveChunkDescriptor>> {
        if required_capacity > crowdb_chunk_client::STREAM_CHUNK_BYTES {
            return Ok(None);
        }
        let state = self.state(chunk_id).await?;
        for attempt in 0..2 {
            let chunk = state.chunk.load_full();
            if chunk.writer_epoch != writer_epoch || state.sealed.load(Ordering::Acquire) {
                return Err(StreamError::StaleWriter);
            }
            let capacity = chunk_capacity(&chunk)?;
            if capacity >= required_capacity {
                return Ok(Some(ActiveChunkDescriptor {
                    chunk_id,
                    physical_start: chunk.acknowledged_cursor,
                    logical_start: 0,
                    acknowledged_cursor: chunk.acknowledged_cursor,
                    capacity,
                }));
            }
            let first = chunk
                .strips
                .first()
                .ok_or_else(|| StreamError::Corruption("stream chunk has no mirror strip".into()))?;
            let Some(Strip::MirrorStrip(mirror)) = &first.strip else {
                return Err(StreamError::Corruption(
                    "stream chunk strip is not mirrored".into(),
                ));
            };
            let unit_count = mirror
                .segments
                .first()
                .map(|segment| segment.unit_count)
                .filter(|count| *count > 0)
                .ok_or_else(|| {
                    StreamError::Corruption("stream mirror strip has no segment geometry".into())
                })?;
            let strip_bytes = u64::from(first.capacity) * 1024;
            let strip_count = u32::try_from((required_capacity - capacity).div_ceil(strip_bytes))
                .map_err(|_| StreamError::InvalidRequest("stream growth needs too many strips".into()))?;
            let response = self
                .allocator
                .append_chunk(AppendChunkRequest {
                    chunk_id: Some(chunk_id),
                    modify_ts: chunk.modify_ts,
                    strip_size: unit_count,
                    strip_count,
                    strip_type: StripType::Mirror as i32,
                    data_num: 0,
                    code_num: 0,
                    copy_count: self.mirror_copies,
                })
                .await
                .map_err(io_error)?;
            if let Some(current) = response.chunk {
                update_state(&state, current);
                if attempt == 0 {
                    continue;
                }
                return Err(StreamError::WriteStalled);
            }
            if response.strips.is_empty() {
                return Err(StreamError::Corruption(
                    "stream chunk growth returned no strips".into(),
                ));
            }
            let mut updated = (*chunk).clone();
            updated.strips.extend(response.strips);
            updated.modify_ts = response.modify_ts;
            updated.capacity = updated.strips.iter().map(|strip| strip.capacity).sum();
            let capacity = chunk_capacity(&updated)?;
            update_state(&state, updated);
            return Ok(Some(ActiveChunkDescriptor {
                chunk_id,
                physical_start: state.cursor.load(Ordering::Acquire),
                logical_start: 0,
                acknowledged_cursor: state.cursor.load(Ordering::Acquire),
                capacity,
            }));
        }
        unreachable!("two append attempts either return or fail")
    }

    async fn write_mirrors_with_images(
        &self,
        _stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        physical_offset: u64,
        data: Bytes,
        images: &[MirrorStripImage],
    ) -> Result<()> {
        let state = self.state(chunk_id).await?;
        let mut chunk = (*state.chunk.load_full()).clone();
        if chunk.writer_epoch != writer_epoch
            || state.sealed.load(Ordering::Acquire)
            || state.cursor.load(Ordering::Acquire) != physical_offset
        {
            return Err(StreamError::StaleWriter);
        }
        let data_end = physical_offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| StreamError::InvalidRequest("stream write cursor overflows".into()))?;
        let flow = MirrorStripFlow::with_shared_failures(
            Arc::clone(&self.allocator),
            Arc::clone(&self.disk_writer),
            Arc::clone(&self.failed_disks),
            writer_epoch,
            3,
            true,
        )
        .map_err(io_error)?
        .resolve_ambiguity();
        let mut copied = 0_usize;
        let mut image_index = 0_usize;
        for strip_index in 0..chunk.strips.len() {
            let strip = chunk.strips[strip_index].clone();
            let strip_start = u64::from(strip.chunk_offset) * 1024;
            let strip_end = strip_start.saturating_add(u64::from(strip.capacity) * 1024);
            let write_start = physical_offset.max(strip_start);
            let write_end = data_end.min(strip_end);
            if write_start >= write_end {
                continue;
            }
            let count = usize::try_from(write_end - write_start)
                .map_err(|_| StreamError::InvalidRequest("stream write view is too large".into()))?;
            let image = images
                .get(image_index)
                .ok_or_else(|| StreamError::InvalidRequest("stream mirror image is missing".into()))?;
            if image.block_offset != write_start - strip_start
                || image.data != data.slice(copied..copied + count)
            {
                return Err(StreamError::InvalidRequest(
                    "stream mirror image does not match data".into(),
                ));
            }
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(StreamError::Corruption(
                    "stream chunk strip is not mirrored".into(),
                ));
            };
            if mirror.segments.len() != self.mirror_copies as usize {
                return Err(StreamError::Corruption(
                    "stream chunk mirror count differs from configuration".into(),
                ));
            }
            let mut pending_advance = None;
            let result = flow
                .write(
                    &mut chunk,
                    physical_offset,
                    strip.strip_sequence,
                    image.block_offset,
                    image.data.clone(),
                    image.full_image.clone(),
                    &mut pending_advance,
                )
                .await;
            if chunk.modify_ts != state.modify_ts.load(Ordering::Acquire) {
                update_state(&state, chunk.clone());
            }
            result.map_err(|error| match error {
                crowdb_chunk_client::IoError::MetadataConflict(_) => StreamError::StaleWriter,
                other => io_error(other),
            })?;
            copied += count;
            image_index += 1;
        }
        if copied != data.len() || image_index != images.len() {
            return Err(StreamError::Corruption(
                "stream mirror images do not cover the write range".into(),
            ));
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
        if state.chunk.load().writer_epoch != writer_epoch
            || state.cursor.load(Ordering::Acquire) != expected_cursor
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
        if let Err(error) = &advance {
            tracing::warn!(?chunk_id, writer_epoch, expected_cursor, new_cursor, %error, "stream cursor advance failed; querying durable state");
        }
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
            update_state(&state, chunk);
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
        if chunk.writer_epoch != writer_epoch {
            return Err(StreamError::StaleWriter);
        }
        update_state(&state, chunk.clone());
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
        update_state(&state, chunk.clone());
        Ok(DurableCursor {
            offset: chunk.acknowledged_cursor,
            last_advance_checksum: state
                .has_checksum
                .load(Ordering::Acquire)
                .then(|| state.last_checksum.load(Ordering::Acquire)),
            sealed: chunk.state == ChunkState::Sealed as i32,
        })
    }

    async fn renew_liveness(&self, chunk_id: ChunkId, writer_epoch: u64) -> Result<()> {
        let state = self.state(chunk_id).await?;
        let cursor = state.cursor.load(Ordering::Acquire);
        let response = self
            .allocator
            .advance_chunk_write(AdvanceChunkWriteRequest {
                chunk_id: Some(chunk_id),
                writer_epoch,
                expected_modify_ts: state.modify_ts.load(Ordering::Acquire),
                acknowledged_cursor: cursor,
                closed_strip_sequence: None,
                writer_lease_ms: self.writer_lease_ms,
            })
            .await
            .map_err(io_error)?;
        let chunk = response
            .chunk
            .ok_or_else(|| StreamError::Corruption("liveness renewal returned no chunk".into()))?;
        if chunk.writer_epoch != writer_epoch || chunk.acknowledged_cursor != cursor {
            return Err(StreamError::StaleWriter);
        }
        update_state(&state, chunk);
        Ok(())
    }

    async fn seal(&self, chunk_id: ChunkId, writer_epoch: u64, cursor: u64) -> Result<()> {
        let state = self.state(chunk_id).await?;
        if state.chunk.load().writer_epoch > writer_epoch || state.cursor.load(Ordering::Acquire) != cursor {
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
        update_state(&state, chunk);
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

    async fn read_verified_frame(
        &self,
        chunk_id: ChunkId,
        physical_offset: u64,
        length: usize,
    ) -> Result<Bytes> {
        self.reader
            .read_verified_frame(
                chunk_id,
                physical_offset,
                u64::try_from(length)
                    .map_err(|_| StreamError::InvalidRequest("chunk frame length exceeds u64".into()))?,
                FrameMagic::StreamV1,
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
