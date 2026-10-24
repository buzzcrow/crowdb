// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_rpc_ffi::OwnedPayload;
use md5::{Digest, Md5};

use super::{DatasetUploads, UploadError, UploadPart, UploadReadDescriptor};
use crate::PayloadStream;

/// An accepted upload part with incremental integrity checks. Emitted owners
/// remain valid independently; only successful EOF certifies the whole part.
pub struct VerifiedUploadPart {
    stream: Option<Box<dyn PayloadStream>>,
    expected: UploadPart,
    delivered: u64,
    digest: Md5,
    failed: bool,
}

impl DatasetUploads {
    /// Opens one immutable accepted part without concatenating receive buffers.
    /// # Errors
    /// Rejects uncommitted uploads, invalid part boundaries or unavailable storage.
    pub async fn read_part_stream(
        &self,
        location: &[u8],
        index: u32,
    ) -> Result<VerifiedUploadPart, UploadError> {
        self.open_read(location).await?.read_part_stream(index).await
    }
}

impl UploadReadDescriptor {
    /// Opens a part using this request's immutable upload description.
    /// # Errors
    /// Rejects invalid boundaries, reclaimed parts and unavailable storage.
    pub async fn read_part_stream(&self, index: u32) -> Result<VerifiedUploadPart, UploadError> {
        let part = self.part(index).await?;
        let stream = self.uploads.payloads.read_stream(&part.locations).await?;
        Ok(VerifiedUploadPart {
            stream: Some(stream),
            expected: part,
            delivered: 0,
            digest: Md5::new(),
            failed: false,
        })
    }
}

impl VerifiedUploadPart {
    /// Failure is terminal; subsequent pulls cannot report successful EOF.
    /// # Errors
    /// Returns storage failures, truncated/oversized payloads or digest mismatch.
    pub async fn next(&mut self) -> Result<Option<OwnedPayload>, UploadError> {
        if self.failed {
            return Err(UploadError::Integrity);
        }
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let result = match stream.next().await {
            Ok(Some(payload)) => {
                let length = self.delivered.saturating_add(payload.len() as u64);
                if payload.is_empty() || length > self.expected.length {
                    Err(UploadError::Integrity)
                } else {
                    self.delivered = length;
                    self.digest.update(payload.as_ref());
                    Ok(Some(payload))
                }
            }
            Ok(None) => {
                if self.delivered != self.expected.length
                    || <[u8; 16]>::from(self.digest.clone().finalize()) != self.expected.md5
                {
                    Err(UploadError::Integrity)
                } else {
                    self.stream = None;
                    Ok(None)
                }
            }
            Err(error) => Err(error),
        };
        if result.is_err() {
            self.failed = true;
            self.stream = None;
        }
        result
    }
}
