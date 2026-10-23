// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_rpc_ffi::OwnedPayload;

use crate::UploadError;

/// Pull-based payload storage. Successful EOF is independent of processing ack.
#[async_trait]
pub trait PayloadStream: Send {
    async fn next(&mut self) -> Result<Option<OwnedPayload>, UploadError>;
}

pub(super) struct ByteStream(pub Option<bytes::Bytes>);
#[async_trait]
impl PayloadStream for ByteStream {
    async fn next(&mut self) -> Result<Option<OwnedPayload>, UploadError> {
        Ok(self.0.take().map(OwnedPayload::from))
    }
}

pub(super) struct ChunkStream(pub crowdb_chunk_client::ChunkReadStream);
#[async_trait]
impl PayloadStream for ChunkStream {
    async fn next(&mut self) -> Result<Option<OwnedPayload>, UploadError> {
        self.0
            .next_payload()
            .await
            .transpose()
            .map_err(|error| UploadError::Storage(error.to_string()))
    }
}
