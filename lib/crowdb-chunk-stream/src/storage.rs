// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, StreamBinding, StreamExtentPage, StreamManifest, StreamName,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{parse_frame, FrameMagic};

use crate::Result;

mod unwritten;
pub(crate) use unwritten::{padded_end, validate_write_bounds};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorAdvance {
    Committed,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrimmedChunk {
    pub chunk_id: ChunkId,
    pub reclaimed_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableCursor {
    pub offset: u64,
    pub last_advance_checksum: Option<u32>,
    pub sealed: bool,
}

#[derive(Clone)]
pub struct MirrorStripImage {
    pub block_offset: u64,
    pub data: Bytes,
    pub full_image: Bytes,
}

/// Ordered bounded strip images and the exact complete data boundary of one batch.
/// Image data never extends beyond the complete boundary, including on retries.
#[derive(Clone, Copy)]
pub struct StreamMirrorWrite<'a> {
    pub physical_offset: u64,
    pub complete_end: u64,
    pub images: &'a [MirrorStripImage],
}

#[async_trait]
pub trait StreamRegistry: Send + Sync {
    async fn load(&self, stream_name: StreamName) -> Result<Option<StreamBinding>>;
    async fn create(&self, binding: StreamBinding) -> Result<()>;
}

#[async_trait]
pub trait StreamMetadataStore: Send + Sync {
    /// # Errors
    /// Rejects a group that differs from the bound metadata.
    fn directory_metadata(&self, metadata_group_id: u64) -> Result<crate::DirectoryMetadataStore>;
    async fn load_current(&self, stream_name: StreamName) -> Result<Option<StreamManifest>>;
    async fn load_extent_page(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        generation: u64,
        page_index: u64,
    ) -> Result<Option<StreamExtentPage>>;
    async fn publish(
        &self,
        expected: Option<(u64, u64)>,
        manifest: StreamManifest,
        extent_pages: Vec<StreamExtentPage>,
    ) -> Result<()>;
    async fn reclaim_extent_pages_before(
        &self,
        stream_name: StreamName,
        retained_generation: u64,
        max_pages: usize,
    ) -> Result<u64>;
}

#[async_trait]
pub trait StreamChunkStore: Send + Sync {
    /// Required synchronized replica count for append traffic accounting.
    fn mirror_copies(&self) -> u32;
    /// Current physical capacity from the chunk layer, independent of a cached
    /// Stream checkpoint.
    /// # Errors
    /// Returns chunk metadata/availability errors.
    async fn current_capacity(&self, chunk_id: ChunkId) -> Result<u64>;
    async fn allocate_mirrored(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        purpose: crowdb_protocol::chunk_stream::StreamPurpose,
    ) -> Result<ActiveChunkDescriptor>;
    /// Extends an active mirror chunk so `required_capacity` bytes can be
    /// addressed. `None` means that this store has reached the chunk's fixed
    /// logical limit and the caller must seal and roll over.
    /// One preparation may overlap appends to the previously initialized
    /// prefix. Return capacity only after every new replica is initialized;
    /// layout publication must preserve concurrent writer progress.
    async fn grow_mirrored(
        &self,
        _stream_name: StreamName,
        _writer_epoch: u64,
        _chunk_id: ChunkId,
        _required_capacity: u64,
    ) -> Result<Option<ActiveChunkDescriptor>> {
        Ok(None)
    }
    async fn write_mirrors(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        write: StreamMirrorWrite<'_>,
    ) -> Result<()>;
    async fn advance_cursor(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        expected_cursor: u64,
        new_cursor: u64,
        checksum: u32,
    ) -> Result<CursorAdvance>;
    async fn durable_cursor(&self, chunk_id: ChunkId, writer_epoch: u64) -> Result<DurableCursor>;
    /// Renews the one Active-chunk liveness task without advancing its cursor.
    async fn renew_liveness(&self, chunk_id: ChunkId, writer_epoch: u64) -> Result<()>;
    async fn seal(&self, chunk_id: ChunkId, writer_epoch: u64, cursor: u64) -> Result<()>;
    async fn read(&self, chunk_id: ChunkId, physical_offset: u64, length: usize) -> Result<Bytes>;
    /// Reads and validates a public stream frame before its payload is exposed.
    ///
    /// In-memory and test stores use the default parser. Production overrides
    /// this to let `ChunkReader` retry a corrupt serving mirror or reconstruct
    /// an EC stripe before returning the frame.
    async fn read_verified_frame(
        &self,
        chunk_id: ChunkId,
        physical_offset: u64,
        length: usize,
    ) -> Result<Bytes> {
        let frame = self.read(chunk_id, physical_offset, length).await?;
        let parsed = parse_frame(&frame, chunk_id)
            .map_err(|error| crate::StreamError::Corruption(format!("invalid stream frame: {error}")))?;
        if parsed.header.magic != FrameMagic::StreamV2 {
            return Err(crate::StreamError::Corruption(
                "stream extent has the wrong frame kind".into(),
            ));
        }
        Ok(frame)
    }
    /// Releases an exactly sealed whole chunk. Active chunks or a different
    /// physical end must fail before deletion; already released IDs are idempotent.
    async fn release_trimmed(
        &self,
        chunk_id: ChunkId,
        logical_end: u64,
        physical_end: u64,
    ) -> Result<TrimmedChunk>;
}
