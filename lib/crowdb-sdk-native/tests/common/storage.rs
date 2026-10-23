// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkAllocator, ChunkIoClient, DiskWriter, Result};
use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvRpcTransportConfig};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::*;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_sdk_native::runtime::RuntimeClients;
use crowdb_sdk_native::{ClientRuntime, RuntimeLimits};

struct TestNoPayload;

#[async_trait]
impl ChunkAllocator for TestNoPayload {
    async fn allocate_chunk(&self, _: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        panic!("metadata operation allocated data")
    }
    async fn append_chunk(&self, _: AppendChunkRequest) -> Result<AppendChunkResponse> {
        panic!("metadata operation appended data")
    }
    async fn seal_chunk(&self, _: SealChunkRequest) -> Result<SealChunkResponse> {
        panic!("metadata operation sealed data")
    }
    async fn delete_chunk(&self, _: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        panic!("metadata operation deleted data")
    }
    async fn update_chunk_strip(&self, _: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        panic!("metadata operation updated data")
    }
    async fn query_chunk(&self, _: QueryChunkRequest) -> Result<QueryChunkResponse> {
        panic!("metadata operation read payload")
    }
}

#[async_trait]
impl DiskWriter for TestNoPayload {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        panic!("metadata operation wrote data")
    }
    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        panic!("metadata operation wrote data")
    }
}

#[allow(dead_code)] // Shared fixture has a custom catalog entry point in topology tests.
pub async fn runtime() -> ClientRuntime {
    runtime_with_metadata(super::metadata::client().await)
}

pub fn runtime_with_metadata(metadata: Arc<crowdb_chunk_kv_client::ChunkKvClient>) -> ClientRuntime {
    let metadata_transport =
        Arc::new(ChunkKvRpcTransport::with_config(8, 1, 1, &ChunkKvRpcTransportConfig::default()).unwrap());
    ClientRuntime::from_clients(
        RuntimeClients {
            control: Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new()))),
            metadata,
            metadata_transport,
            chunks: Arc::new(ChunkIoClient::from_parts(
                Arc::new(TestNoPayload),
                Arc::new(TestNoPayload),
                ChunkType::S3,
            )),
        },
        RuntimeLimits::default(),
    )
    .unwrap()
}
