// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Direct, one-chunk mirror writer for ordered byte streams.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AllocateChunkRequest, AppendChunkRequest, Chunk, ChunkState, ChunkType,
    SealChunkRequest, Strip, StripType,
};
use crowdb_protocol::common::ChunkId;
use tokio::task::JoinSet;

use crate::{ChunkAllocator, DiskWriter, IoError, Result};

/// Default hard logical capacity of one stream chunk.
pub const STREAM_CHUNK_BYTES: u64 = 256 * 1024 * 1024;
/// Allocation granularity for a growing stream chunk.
pub const STREAM_STRIP_BYTES: u64 = 1024 * 1024;

/// A single-owner direct writer that replicates each append to every mirror
/// before advancing chunkdb's fenced acknowledged cursor.
pub struct MirrorChunkWriter {
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    chunk: Chunk,
    chunk_id: ChunkId,
    writer_epoch: u64,
    writer_lease_ms: u64,
    capacity: u64,
}

impl MirrorChunkWriter {
    /// Allocates one three-copy WAL chunk with a fixed logical capacity.
    ///
    /// # Errors
    ///
    /// Returns an allocation or layout error when chunkdb cannot supply an
    /// active mirror strip.
    pub async fn allocate(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        stream_name: StreamName,
        writer_epoch: u64,
        writer_lease_ms: u64,
    ) -> Result<Self> {
        Self::allocate_with_copy_count(
            allocator,
            disk_writer,
            stream_name,
            writer_epoch,
            writer_lease_ms,
            3,
        )
        .await
    }

    /// Allocates one WAL chunk with an explicit mirror count.
    ///
    /// # Errors
    ///
    /// Returns an allocation or layout error for a zero count or unavailable placement.
    pub async fn allocate_with_copy_count(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        stream_name: StreamName,
        writer_epoch: u64,
        writer_lease_ms: u64,
        copy_count: u32,
    ) -> Result<Self> {
        if writer_epoch == 0 || writer_lease_ms == 0 || copy_count == 0 {
            return Err(IoError::AllocationFailed(
                "stream mirror writer requires a nonzero epoch and lease".into(),
            ));
        }
        let response = allocator
            .allocate_chunk(AllocateChunkRequest {
                chunk_id: None,
                write_granularity: u32::try_from(STREAM_STRIP_BYTES / 1024).unwrap_or(u32::MAX),
                strip_count: 1,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count,
                chunk_type: ChunkType::Stream as i32,
                writer_epoch,
                writer_lease_ms,
                owner_key: stream_name.chunk_owner_key(),
            })
            .await?;
        let chunk = response
            .chunk
            .ok_or_else(|| IoError::AllocationFailed("stream chunk allocation returned no chunk".into()))?;
        Self::open_with_copy_count(
            allocator,
            disk_writer,
            chunk,
            stream_name,
            writer_epoch,
            writer_lease_ms,
            copy_count,
        )
    }

    /// Opens an allocated or recovered direct mirror chunk.
    ///
    /// # Errors
    ///
    /// Returns an error for a mismatched epoch, state, capacity, or layout.
    pub fn open(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        chunk: Chunk,
        stream_name: StreamName,
        writer_epoch: u64,
        writer_lease_ms: u64,
    ) -> Result<Self> {
        Self::open_with_copy_count(
            allocator,
            disk_writer,
            chunk,
            stream_name,
            writer_epoch,
            writer_lease_ms,
            3,
        )
    }

    /// Opens a direct mirror chunk with the configured layout width.
    ///
    /// # Errors
    ///
    /// Returns an error for a mismatched epoch, state, capacity, or mirror count.
    pub fn open_with_copy_count(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        chunk: Chunk,
        stream_name: StreamName,
        writer_epoch: u64,
        writer_lease_ms: u64,
        copy_count: u32,
    ) -> Result<Self> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("stream chunk has no identity".into()))?;
        if chunk.writer_epoch != writer_epoch
            || chunk.state != ChunkState::Active as i32
            || chunk.chunk_type != ChunkType::Stream as i32
            || chunk.owner_key != stream_name.chunk_owner_key()
            || chunk.strips.is_empty()
        {
            return Err(IoError::MetadataConflict(
                "stream chunk ownership, state, type, or strip count is invalid".into(),
            ));
        }
        let capacity = chunk
            .strips
            .iter()
            .try_fold(0_u64, |total, strip| {
                let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                    return Err(IoError::MetadataConflict(
                        "stream chunk does not contain a mirror strip".into(),
                    ));
                };
                if mirror.segments.len() != copy_count as usize || strip.unit_kb == 0 || strip.capacity == 0 {
                    return Err(IoError::MetadataConflict(
                        "stream chunk mirror geometry is invalid".into(),
                    ));
                }
                total
                    .checked_add(u64::from(strip.capacity) * 1024)
                    .ok_or_else(|| IoError::MetadataConflict("stream chunk capacity overflows".into()))
            })?
            .min(STREAM_CHUNK_BYTES);
        if chunk.acknowledged_cursor > capacity {
            return Err(IoError::MetadataConflict(
                "stream chunk cursor exceeds its capacity".into(),
            ));
        }
        Ok(Self {
            allocator,
            disk_writer,
            chunk,
            chunk_id,
            writer_epoch,
            writer_lease_ms,
            capacity,
        })
    }

    #[must_use]
    pub fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.chunk.acknowledged_cursor
    }

    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the current authoritative chunk metadata snapshot.
    #[must_use]
    pub fn chunk(&self) -> &Chunk {
        &self.chunk
    }

    /// Appends mirror strips until the requested capacity is addressable.
    ///
    /// # Errors
    ///
    /// Returns an allocation error when chunkdb cannot extend this active
    /// chunk, or when the logical stream-chunk limit would be exceeded.
    pub async fn grow_to(&mut self, required_capacity: u64) -> Result<bool> {
        if required_capacity <= self.capacity {
            return Ok(true);
        }
        if required_capacity > STREAM_CHUNK_BYTES {
            return Ok(false);
        }
        let first = self
            .chunk
            .strips
            .first()
            .ok_or_else(|| IoError::MetadataConflict("stream chunk has no mirror strip".into()))?;
        let strip_bytes = u64::from(first.capacity) * 1024;
        let strip_count = u32::try_from((required_capacity - self.capacity).div_ceil(strip_bytes))
            .map_err(|_| IoError::AllocationFailed("stream chunk extension is too large".into()))?;
        let unit_count = first
            .strip
            .as_ref()
            .and_then(|strip| match strip {
                Strip::MirrorStrip(mirror) => mirror.segments.first(),
                Strip::EcStrip(_) => None,
            })
            .map(|segment| segment.unit_count)
            .filter(|count| *count > 0)
            .ok_or_else(|| IoError::MetadataConflict("stream chunk mirror geometry changed".into()))?;
        for attempt in 0..2 {
            let response = self
                .allocator
                .append_chunk(AppendChunkRequest {
                    chunk_id: Some(self.chunk_id),
                    modify_ts: self.chunk.modify_ts,
                    strip_size: unit_count,
                    strip_count,
                    strip_type: StripType::Mirror as i32,
                    data_num: 0,
                    code_num: 0,
                    copy_count: self.copy_count()?,
                })
                .await?;
            if let Some(current) = response.chunk {
                self.chunk = current;
                self.capacity = chunk_capacity(&self.chunk)?;
                if attempt == 0 {
                    continue;
                }
                return Err(IoError::MetadataConflict(
                    "stream chunk revision changed twice".into(),
                ));
            }
            if response.strips.is_empty() {
                return Err(IoError::AllocationFailed(
                    "stream chunk extension returned no strips".into(),
                ));
            }
            self.chunk.strips.extend(response.strips);
            self.chunk.modify_ts = response.modify_ts;
            self.chunk.capacity = self.chunk.strips.iter().map(|strip| strip.capacity).sum();
            self.capacity = chunk_capacity(&self.chunk)?;
            return Ok(self.capacity >= required_capacity);
        }
        unreachable!("two append attempts either return or fail")
    }

    fn copy_count(&self) -> Result<u32> {
        self.chunk
            .strips
            .first()
            .and_then(|strip| match &strip.strip {
                Some(Strip::MirrorStrip(mirror)) => u32::try_from(mirror.segments.len()).ok(),
                _ => None,
            })
            .filter(|count| *count > 0)
            .ok_or_else(|| IoError::MetadataConflict("stream chunk mirror layout changed".into()))
    }

    /// Replicates an owned buffer and durably advances the fenced cursor.
    ///
    /// # Errors
    ///
    /// Returns a typed write or metadata error without retrying bytes at a
    /// different cursor.
    pub async fn append(&mut self, data: Bytes) -> Result<(u64, u64)> {
        if data.is_empty() {
            return Ok((self.cursor(), self.cursor()));
        }
        let begin = self.cursor();
        let end =
            begin
                .checked_add(u64::try_from(data.len()).map_err(|_| {
                    IoError::WriteFailed("stream append length exceeds addressable range".into())
                })?)
                .ok_or_else(|| IoError::WriteFailed("stream append cursor overflows".into()))?;
        if end > self.capacity {
            return Err(IoError::WriteFailed(
                "stream append exceeds the direct chunk capacity".into(),
            ));
        }
        let mut writes = JoinSet::new();
        let mut copied = 0_usize;
        for strip in &self.chunk.strips {
            let strip_start = u64::from(strip.chunk_offset) * 1024;
            let strip_end = strip_start.saturating_add(u64::from(strip.capacity) * 1024);
            let write_start = begin.max(strip_start);
            let write_end = end.min(strip_end);
            if write_start >= write_end {
                continue;
            }
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(IoError::MetadataConflict(
                    "stream chunk mirror layout changed".into(),
                ));
            };
            let byte_count = usize::try_from(write_end - write_start)
                .map_err(|_| IoError::WriteFailed("stream append slice is too large".into()))?;
            let view = data.slice(copied..copied + byte_count);
            copied += byte_count;
            let unit_bytes = u64::from(strip.unit_kb) * 1024;
            for segment in &mirror.segments {
                let segment = *segment;
                let disk_writer = Arc::clone(&self.disk_writer);
                let view = view.clone();
                let offset = write_start - strip_start;
                writes.spawn(async move {
                    disk_writer
                        .write_at_byte_offset(&segment, unit_bytes, offset, view)
                        .await
                });
            }
        }
        if copied != data.len() {
            return Err(IoError::MetadataConflict(
                "stream chunk strips do not cover the append range".into(),
            ));
        }
        while let Some(result) = writes.join_next().await {
            result.map_err(|error| IoError::WriteFailed(format!("mirror write task failed: {error}")))??;
        }
        let response = self
            .allocator
            .advance_chunk_write(AdvanceChunkWriteRequest {
                chunk_id: Some(self.chunk_id),
                writer_epoch: self.writer_epoch,
                expected_modify_ts: self.chunk.modify_ts,
                acknowledged_cursor: end,
                closed_strip_sequence: None,
                writer_lease_ms: self.writer_lease_ms,
            })
            .await?;
        let updated = response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("cursor advance returned no chunk".into()))?;
        if updated.id != Some(self.chunk_id)
            || updated.writer_epoch != self.writer_epoch
            || updated.acknowledged_cursor != end
        {
            return Err(IoError::MetadataConflict(
                "cursor advance returned an inconsistent chunk".into(),
            ));
        }
        self.chunk = updated;
        Ok((begin, end))
    }

    /// Seals this chunk at its acknowledged cursor.
    ///
    /// # Errors
    ///
    /// Returns a metadata error if chunkdb cannot durably seal the chunk.
    pub async fn seal(&mut self) -> Result<()> {
        let seal_length = u32::try_from(self.cursor().div_ceil(1024)).unwrap_or(u32::MAX);
        let response = self
            .allocator
            .seal_chunk(SealChunkRequest {
                chunk_id: Some(self.chunk_id),
                seal_length,
            })
            .await?;
        let sealed = response
            .chunk
            .ok_or_else(|| IoError::MetadataConflict("seal returned no chunk".into()))?;
        if sealed.id != Some(self.chunk_id)
            || sealed.state != ChunkState::Sealed as i32
            || sealed.acknowledged_cursor < self.cursor()
        {
            return Err(IoError::MetadataConflict(
                "seal returned an inconsistent stream chunk".into(),
            ));
        }
        self.chunk = sealed;
        Ok(())
    }
}

fn chunk_capacity(chunk: &Chunk) -> Result<u64> {
    chunk
        .strips
        .iter()
        .try_fold(0_u64, |total, strip| {
            total
                .checked_add(u64::from(strip.capacity) * 1024)
                .ok_or_else(|| IoError::MetadataConflict("stream chunk capacity overflows".into()))
        })
        .map(|capacity| capacity.min(STREAM_CHUNK_BYTES))
}
