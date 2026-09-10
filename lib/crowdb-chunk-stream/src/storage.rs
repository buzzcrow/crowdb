// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, StreamBinding, StreamExtentPage, StreamManifest, StreamName,
};
use crowdb_protocol::common::ChunkId;

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
        expected_generation: u64,
        manifest: StreamManifest,
        extent_pages: Vec<StreamExtentPage>,
    ) -> Result<()>;
}

#[async_trait]
pub trait StreamChunkStore: Send + Sync {
    async fn allocate_mirrored(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
    ) -> Result<ActiveChunkDescriptor>;
    async fn write_mirrors(
        &self,
        stream_name: StreamName,
        writer_epoch: u64,
        chunk_id: ChunkId,
        physical_offset: u64,
        data: Bytes,
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
    async fn durable_cursor(&self, chunk_id: ChunkId, writer_epoch: u64) -> Result<u64>;
    async fn seal(&self, chunk_id: ChunkId, writer_epoch: u64, cursor: u64) -> Result<()>;
    async fn read(&self, chunk_id: ChunkId, physical_offset: u64, length: usize) -> Result<Bytes>;
    async fn release_trimmed(&self, chunk_id: ChunkId, logical_end: u64) -> Result<TrimmedChunk>;
}
