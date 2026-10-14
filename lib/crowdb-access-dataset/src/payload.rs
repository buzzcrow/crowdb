// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoWriter, ReclaimOutcome, SmallWriteIntent};
use crowdb_protocol::chunkdb::rpc::Location;
use std::sync::Arc;

mod reader;
pub use reader::UploadedChunkReader;

use crate::{UploadError, MAX_UPLOAD_PART_BYTES};

/// Storage boundary for bounded, immutable upload parts.
#[async_trait]
pub trait PayloadStorage: Send + Sync {
    async fn write(
        &self,
        key: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError>;
    async fn read(&self, locations: &[Location]) -> Result<Bytes, UploadError>;
    async fn discard(&self, locations: &[Location]) -> Result<bool, UploadError>;
}

/// Dataset-specific chunk pool, supplied by production service discovery.
pub struct ChunkPayloadStorage {
    chunks: ChunkIoClient,
}

impl ChunkPayloadStorage {
    #[must_use]
    pub fn new(chunks: ChunkIoClient) -> Self {
        Self { chunks }
    }
}

#[async_trait]
impl PayloadStorage for ChunkPayloadStorage {
    async fn write(
        &self,
        key: &[u8],
        data: Bytes,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        if data.is_empty() || data.len() > MAX_UPLOAD_PART_BYTES {
            return Err(UploadError::Invalid);
        }
        let mut writer = self
            .chunks
            .prepare_shared_object_write_for_key(data.len(), key)
            .await
            .map_err(|error| UploadError::Storage(error.to_string()))?;
        if let Err(error) = writer.on_data(data).await {
            let _ = writer.on_error().await;
            return Err(UploadError::Storage(error.to_string()));
        }
        writer
            .finish_durable_with_intent(intent)
            .await
            .map_err(|error| UploadError::Storage(error.to_string()))
    }

    async fn read(&self, locations: &[Location]) -> Result<Bytes, UploadError> {
        let mut stream = self
            .chunks
            .read_stream(locations)
            .map_err(|error| UploadError::Storage(error.to_string()))?;
        let mut output = bytes::BytesMut::new();
        while let Some(data) = stream.next_chunk().await {
            let data = data.map_err(|error| UploadError::Storage(error.to_string()))?;
            if output.len().saturating_add(data.len()) > MAX_UPLOAD_PART_BYTES {
                return Err(UploadError::Integrity);
            }
            output.extend_from_slice(&data);
        }
        Ok(output.freeze())
    }

    async fn discard(&self, locations: &[Location]) -> Result<bool, UploadError> {
        let (allocator, _) = self.chunks.storage_parts();
        let mut complete = true;
        for location in locations {
            complete &= crowdb_chunk_client::reclaim_location(allocator.as_ref(), location)
                .await
                .map_err(|error| UploadError::Storage(error.to_string()))?
                == ReclaimOutcome::Reclaimed;
        }
        Ok(complete)
    }
}
