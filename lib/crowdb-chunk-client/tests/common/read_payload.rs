// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkAllocator, DiskWriter, Result};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk,
    DeleteChunkRequest, DeleteChunkResponse, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_rpc_ffi::OwnedPayload;

pub struct TestPayloadStorage {
    pub chunk: Chunk,
    pub data: OwnedPayload,
}

#[async_trait]
impl ChunkAllocator for TestPayloadStorage {
    async fn allocate_chunk(&self, _: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        panic!("read allocated storage")
    }
    async fn append_chunk(&self, _: AppendChunkRequest) -> Result<AppendChunkResponse> {
        panic!("read appended storage")
    }
    async fn seal_chunk(&self, _: SealChunkRequest) -> Result<SealChunkResponse> {
        panic!("read sealed storage")
    }
    async fn delete_chunk(&self, _: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        panic!("read deleted storage")
    }
    async fn update_chunk_strip(&self, _: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        panic!("read updated storage")
    }
    async fn query_chunk(&self, _: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Ok(QueryChunkResponse {
            chunk: Some(self.chunk.clone()),
            layout_validity_ms: 60_000,
        })
    }
}

#[async_trait]
impl DiskWriter for TestPayloadStorage {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote storage")
    }
    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote storage")
    }
    async fn read(&self, _: &Segment, _: u64, _: u64, _: u32) -> Result<Bytes> {
        panic!("reader erased allocation metadata before delivery")
    }
    async fn read_payload(&self, _: &Segment, _: u64, offset: u64, length: u32) -> Result<OwnedPayload> {
        let offset = usize::try_from(offset).unwrap();
        Ok(self.data.slice(offset..offset + length as usize))
    }
}
