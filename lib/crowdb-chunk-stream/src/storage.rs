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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorAdvance {
    Committed,
    DefinitelyNotCommitted,
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

#[async_trait]
pub trait StreamRegistry: Send + Sync {
    async fn load(&self, stream_name: StreamName) -> Result<Option<StreamBinding>>;
    async fn create(&self, binding: StreamBinding) -> Result<()>;
}

#[async_trait]
pub trait StreamMetadataStore: Send + Sync {
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
    async fn allocate_mirrored(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ActiveChunkDescriptor>;
    /// Extends an active mirror chunk so `required_capacity` bytes can be
    /// addressed. `None` means that this store has reached the chunk's fixed
    /// logical limit and the caller must seal and roll over.
    async fn grow_mirrored(
        &self,
        _stream_name: StreamName,
        _writer_epoch: u64,
        _chunk_id: ChunkId,
        _required_capacity: u64,
    ) -> Result<Option<ActiveChunkDescriptor>> {
        Ok(None)
    }
    async fn write_mirrors_with_images(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        physical_offset: u64,
        data: Bytes,
        images: &[MirrorStripImage],
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
    async fn renew_liveness(&self, _chunk_id: ChunkId, _writer_epoch: u64) -> Result<()> {
        Ok(())
    }
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
        if parsed.header.magic != FrameMagic::StreamV1 {
            return Err(crate::StreamError::Corruption(
                "stream extent has the wrong frame kind".into(),
            ));
        }
        Ok(frame)
    }
    async fn release_trimmed(&self, chunk_id: ChunkId, logical_end: u64) -> Result<TrimmedChunk>;
}
