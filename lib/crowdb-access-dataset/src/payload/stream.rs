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

pub(super) async fn collect(
    stream: &mut dyn PayloadStream,
    length: usize,
) -> Result<bytes::Bytes, UploadError> {
    if length == 0 || length > crate::MAX_UPLOAD_PART_BYTES {
        return Err(UploadError::Integrity);
    }
    let first = stream.next().await?.ok_or(UploadError::Integrity)?;
    if first.is_empty() || first.len() > length {
        return Err(UploadError::Integrity);
    }
    if first.len() == length {
        // A complete logical object needs no further IO credit to reach EOF.
        // Still pull once: an error after the last data frame must not become success.
        if stream.next().await?.is_some() {
            return Err(UploadError::Integrity);
        }
        return Ok(first.into_bytes());
    }
    let mut output = bytes::BytesMut::with_capacity(length);
    output.extend_from_slice(&first);
    // A subsequent fetch may require this allocation's read credit.
    drop(first);
    while let Some(data) = stream.next().await? {
        if data.is_empty() || data.len() > length - output.len() {
            return Err(UploadError::Integrity);
        }
        output.extend_from_slice(&data);
    }
    if output.len() != length {
        return Err(UploadError::Integrity);
    }
    Ok(output.freeze())
}
