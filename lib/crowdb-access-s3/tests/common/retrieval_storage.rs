// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_client::{ChunkAllocator, ChunkIoClient, DiskWriter, IoError, Result};
use crowdb_protocol::chunkdb::rpc::*;
use crowdb_protocol::diskdb::rpc::Segment;
use hyper::body::Bytes;

pub struct TestReadStorage {
    pub queries: AtomicUsize,
}

impl TestReadStorage {
    pub fn client(self: &Arc<Self>) -> ChunkIoClient {
        ChunkIoClient::from_parts(self.clone(), self.clone(), ChunkType::S3)
    }
}

#[async_trait]
impl ChunkAllocator for TestReadStorage {
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
    async fn query_chunk(&self, _: QueryChunkRequest) -> Result<QueryChunkResponse> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        Err(IoError::Topology("test owner unavailable".into()))
    }
}

#[async_trait]
impl DiskWriter for TestReadStorage {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote storage")
    }
    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        panic!("read wrote storage")
    }
}
