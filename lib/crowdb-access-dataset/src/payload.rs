// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{
    ChunkIoClient, ChunkIoWriter, FramedWriteBuffer, ReclaimOutcome, SmallWriteIntent,
};
use crowdb_protocol::chunkdb::rpc::Location;
use std::sync::Arc;

mod reader;
mod stream;
pub use reader::UploadedChunkReader;
pub use stream::PayloadStream;

use crate::{UploadError, MAX_UPLOAD_PART_BYTES};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadDurability {
    Durable,
    Pending,
    Abandoned,
}

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
    /// Submits caller-prepared immutable storage frames, retaining their owner.
    /// Implementations without this path reject it instead of staging a copy.
    async fn write_framed(
        &self,
        _key: &[u8],
        _data: Box<dyn FramedWriteBuffer>,
        _intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        Err(UploadError::Invalid)
    }
    /// Retains physical allocation owners for bounded native delivery.
    /// The compatibility default preserves the existing byte reader.
    async fn read_stream(&self, locations: &[Location]) -> Result<Box<dyn PayloadStream>, UploadError> {
        Ok(Box::new(stream::ByteStream(Some(self.read(locations).await?))))
    }
    async fn durability(&self, locations: &[Location]) -> Result<PayloadDurability, UploadError>;
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
    async fn write_framed(
        &self,
        key: &[u8],
        data: Box<dyn FramedWriteBuffer>,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<Location>, UploadError> {
        let length = usize::try_from(data.logical_len()).map_err(|_| UploadError::Invalid)?;
        if length == 0 || length > MAX_UPLOAD_PART_BYTES {
            return Err(UploadError::Invalid);
        }
        let mut writer = self
            .chunks
            .prepare_shared_object_write_for_key(length, key)
            .await
            .map_err(|error| UploadError::Storage(error.to_string()))?;
        if let Err(error) = writer.on_framed_data(data).await {
            let _ = writer.on_error().await;
            return Err(UploadError::Storage(error.to_string()));
        }
        writer
            .finish_durable_with_intent(intent)
            .await
            .map_err(|error| UploadError::Storage(error.to_string()))
    }

    async fn read_stream(&self, locations: &[Location]) -> Result<Box<dyn PayloadStream>, UploadError> {
        let stream = self
            .chunks
            .read_stream(locations)
            .map_err(|error| UploadError::Storage(error.to_string()))?;
        Ok(Box::new(stream::ChunkStream(stream)))
    }

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

    async fn durability(&self, locations: &[Location]) -> Result<PayloadDurability, UploadError> {
        use crowdb_protocol::chunkdb::rpc::{ChunkState, ChunkType, QueryChunkRequest};
        let (allocator, _) = self.chunks.storage_parts();
        let mut abandoned = false;
        let mut pending = false;
        for location in locations {
            let id = location.chunk_id.ok_or(UploadError::Integrity)?;
            let end = location
                .offset
                .checked_add(location.length)
                .ok_or(UploadError::Integrity)?;
            let result = allocator
                .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
                .await;
            let chunk = match result {
                Ok(result) => result.chunk.ok_or(UploadError::Integrity)?,
                Err(crowdb_chunk_client::IoError::ChunkNotFound(_)) => {
                    abandoned = true;
                    continue;
                }
                Err(error) => return Err(UploadError::Storage(error.to_string())),
            };
            if chunk.id != Some(id)
                || chunk.chunk_type != ChunkType::Dataset as i32
                || chunk.writer_epoch == 0
            {
                return Err(UploadError::Integrity);
            }
            let state = ChunkState::try_from(chunk.state).map_err(|()| UploadError::Integrity)?;
            if state == ChunkState::Deleted {
                abandoned = true;
            } else if chunk.acknowledged_cursor < end {
                if state == ChunkState::Sealed {
                    abandoned = true;
                } else {
                    pending = true;
                }
            }
        }
        Ok(if pending {
            PayloadDurability::Pending
        } else if abandoned {
            PayloadDurability::Abandoned
        } else {
            PayloadDurability::Durable
        })
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
