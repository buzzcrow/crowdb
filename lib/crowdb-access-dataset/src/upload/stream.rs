// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_rpc_ffi::OwnedPayload;
use md5::{Digest, Md5};

use super::{decode_part, part_key, DatasetUploads, PartState, UploadError, UploadPart, UploadState};
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
        let session = self.session(location).await?;
        if !matches!(session.state, UploadState::Complete | UploadState::Claimed) {
            return Err(UploadError::Conflict);
        }
        let entry = self
            .store
            .get(&part_key(location, index))
            .await?
            .ok_or(UploadError::NotFound)?;
        let entry = decode_part(&entry.bytes)?;
        let start = u64::from(index) * u64::from(session.part_size);
        if entry.state != PartState::Accepted
            || entry.part.index != index
            || start >= session.length
            || entry.part.length != (session.length - start).min(u64::from(session.part_size))
        {
            return Err(UploadError::Integrity);
        }
        let stream = self.payloads.read_stream(&entry.part.locations).await?;
        Ok(VerifiedUploadPart {
            stream: Some(stream),
            expected: entry.part,
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
