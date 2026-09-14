// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Direct, one-chunk mirror writer for ordered byte streams.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::chunkdb::rpc::{
    AdvanceChunkWriteRequest, AllocateChunkRequest, Chunk, ChunkState, ChunkType, SealChunkRequest, Strip,
    StripType,
};
use crowdb_protocol::common::ChunkId;
use tokio::task::JoinSet;

use crate::{ChunkAllocator, DiskWriter, IoError, Result};

/// Default hard logical capacity of one stream chunk.
pub const STREAM_CHUNK_BYTES: u64 = 256 * 1024 * 1024;

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
    /// Returns an allocation or layout error when chunkdb cannot supply one
    /// active mirror strip covering the requested capacity.
    pub async fn allocate(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        stream_name: StreamName,
        writer_epoch: u64,
        writer_lease_ms: u64,
    ) -> Result<Self> {
        if writer_epoch == 0 || writer_lease_ms == 0 {
            return Err(IoError::AllocationFailed(
                "stream mirror writer requires a nonzero epoch and lease".into(),
            ));
        }
        let response = allocator
            .allocate_chunk(AllocateChunkRequest {
                chunk_id: None,
                write_granularity: u32::try_from(STREAM_CHUNK_BYTES / 1024).unwrap_or(u32::MAX),
                strip_count: 1,
                strip_type: StripType::Mirror as i32,
                data_num: 0,
                code_num: 0,
                copy_count: 3,
                chunk_type: ChunkType::Stream as i32,
                writer_epoch,
                writer_lease_ms,
                owner_key: stream_name.chunk_owner_key(),
            })
            .await?;
        let chunk = response
            .chunk
            .ok_or_else(|| IoError::AllocationFailed("stream chunk allocation returned no chunk".into()))?;
        Self::open(
            allocator,
            disk_writer,
            chunk,
            stream_name,
            writer_epoch,
            writer_lease_ms,
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
        let chunk_id = chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("stream chunk has no identity".into()))?;
        if chunk.writer_epoch != writer_epoch
            || chunk.state != ChunkState::Active as i32
            || chunk.chunk_type != ChunkType::Stream as i32
            || chunk.owner_key != stream_name.chunk_owner_key()
            || chunk.strips.len() != 1
        {
            return Err(IoError::MetadataConflict(
                "stream chunk ownership, state, type, or strip count is invalid".into(),
            ));
        }
        let strip = &chunk.strips[0];
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(IoError::MetadataConflict(
                "stream chunk does not contain a mirror strip".into(),
            ));
        };
        if mirror.segments.len() != 3 || strip.unit_kb == 0 || strip.capacity == 0 {
            return Err(IoError::MetadataConflict(
                "stream chunk mirror geometry is invalid".into(),
            ));
        }
        let capacity = u64::from(strip.capacity)
            .checked_mul(1024)
            .ok_or_else(|| IoError::MetadataConflict("stream chunk capacity overflows".into()))?
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
        let strip = &self.chunk.strips[0];
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(IoError::MetadataConflict(
                "stream chunk mirror layout changed".into(),
            ));
        };
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut writes = JoinSet::new();
        for segment in &mirror.segments {
            let segment = *segment;
            let disk_writer = Arc::clone(&self.disk_writer);
            let data = data.clone();
            writes.spawn(async move {
                disk_writer
                    .write_at_byte_offset(&segment, unit_bytes, begin, data)
                    .await
            });
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
