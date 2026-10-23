// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared bounded S3 writer selection and durable completion.

use super::S3Executor;
use crate::error::S3ErrorCode;
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoWriter, IoError, LargeWritePolicy, PreparedLargeWrite, SharedObjectWriter,
};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::ChunkType;
use std::sync::Arc;

#[derive(Clone)]
pub struct S3WritePolicy {
    pub small_object_limit: usize,
    pub large_write: LargeWritePolicy,
}

impl S3WritePolicy {
    #[must_use]
    pub fn basic(small_object_limit: usize) -> Self {
        Self {
            small_object_limit,
            large_write: LargeWritePolicy {
                ec_scheme: EcScheme::new(8, 4),
                client: Arc::new(ChunkClientConfig::new(ChunkType::S3)),
            },
        }
    }
}

impl S3Executor {
    /// # Errors
    /// Returns bounded admission, discovery or chunk preparation failure.
    pub async fn prepare_writer(
        &self,
        content_length: Option<u64>,
        key: &[u8],
        policy: &S3WritePolicy,
    ) -> Result<ObjectWriter, S3ErrorCode> {
        if let Some(length) = content_length
            .and_then(|length| usize::try_from(length).ok())
            .filter(|length| *length <= policy.small_object_limit)
        {
            if length > crowdb_protocol::frame::MAX_FRAME_V2_PAYLOAD_BYTES {
                return self
                    .storage
                    .chunks
                    .prepare_shared_object_write_for_key(length, key)
                    .await
                    .map(ObjectWriter::Small)
                    .map_err(|_| S3ErrorCode::SlowDown);
            }
            match self.storage.chunks.prepare_small_write_for_key(length, key).await {
                Ok(writer) => return Ok(ObjectWriter::Small(writer)),
                Err(IoError::ObjectTooLarge { .. }) => {}
                Err(IoError::MemoryBudgetExhausted) => return Err(S3ErrorCode::SlowDown),
                Err(_) => return Err(S3ErrorCode::ServiceUnavailable),
            }
        }
        let mut prepared = self
            .storage
            .chunks
            .prepare_large_write(content_length, policy.large_write.clone());
        prepared.wait_until_prepared().await.map_err(|error| {
            tracing::warn!(%error, "S3 large write preparation failed");
            S3ErrorCode::ServiceUnavailable
        })?;
        Ok(ObjectWriter::Large(Box::new(prepared)))
    }
}

pub enum ObjectWriter {
    Small(SharedObjectWriter),
    Large(Box<PreparedLargeWrite>),
}

impl ObjectWriter {
    #[must_use]
    pub fn is_large(&self) -> bool {
        matches!(self, Self::Large(_))
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for ObjectWriter {
    async fn on_data(
        &mut self,
        buffer: hyper::body::Bytes,
    ) -> crowdb_chunk_client::Result<crowdb_chunk_client::FeedStatus> {
        match self {
            Self::Small(writer) => writer.on_data(buffer).await,
            Self::Large(writer) => writer.on_data(buffer).await,
        }
    }

    async fn on_framed_data(
        &mut self,
        buffer: Box<dyn crowdb_chunk_client::FramedWriteBuffer>,
    ) -> crowdb_chunk_client::Result<crowdb_chunk_client::FeedStatus> {
        match self {
            Self::Small(writer) => writer.on_framed_data(buffer).await,
            Self::Large(writer) => writer.on_framed_data(buffer).await,
        }
    }

    async fn on_finish(&mut self) -> crowdb_chunk_client::Result<Vec<crowdb_chunk_client::ProtoLocation>> {
        match self {
            Self::Small(writer) => writer.finish_durable().await,
            Self::Large(writer) => writer.on_finish().await,
        }
    }

    async fn on_error(&mut self) -> crowdb_chunk_client::Result<Vec<crowdb_chunk_client::ProtoLocation>> {
        match self {
            Self::Small(writer) => writer.on_error().await,
            Self::Large(writer) => writer.on_error().await,
        }
    }

    fn require_data(&self) -> bool {
        match self {
            Self::Small(writer) => writer.require_data(),
            Self::Large(writer) => writer.require_data(),
        }
    }

    fn input_complete(&self) -> bool {
        match self {
            Self::Small(writer) => writer.input_complete(),
            Self::Large(writer) => writer.input_complete(),
        }
    }

    async fn wait_for_capacity(&mut self) {
        match self {
            Self::Small(writer) => writer.wait_for_capacity().await,
            Self::Large(writer) => writer.wait_for_capacity().await,
        }
    }
}
