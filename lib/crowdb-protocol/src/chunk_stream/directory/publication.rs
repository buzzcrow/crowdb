// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable protection of a bounded path mutation before any new page is stored.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::super::record::{decode_body, decode_envelope, encode_record, StreamRecordEncoding};
use super::{
    StreamDirectoryError, StreamDirectoryHead, StreamDirectoryPageId, StreamName,
    MAX_STREAM_DIRECTORY_RECORD_BYTES,
};

const GUARD_KIND: u8 = 4;
const CONTROL_KIND: u8 = 5;
pub const MAX_STREAM_PUBLICATION_PAGES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamPublicationId(pub [u8; 32]);

/// Base reachability plus the bounded new page set protects a not-yet-readable root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPublicationGuard {
    pub head: StreamDirectoryHead,
    pub base: Option<StreamDirectoryHead>,
    #[serde(deserialize_with = "super::bounded::publication_pages")]
    pub pages: Vec<StreamDirectoryPageId>,
}

/// Its KV revision changes atomically with the retained root/guard/pin set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDirectoryControl {
    pub stream_name: StreamName,
    pub purpose: super::StreamPurpose,
}

impl StreamDirectoryControl {
    /// # Errors
    /// Returns serialization errors for the fixed-size control record.
    pub fn encode(self) -> Result<Vec<u8>, StreamDirectoryError> {
        encode_record(CONTROL_KIND, &self).map_err(|error| StreamDirectoryError::Codec(error.to_string()))
    }

    /// # Errors
    /// Rejects wrong scope, versions/kinds, trailing bytes and malformed bodies.
    pub fn decode(bytes: &[u8], expected: Self) -> Result<Self, StreamDirectoryError> {
        let body = decode_envelope(CONTROL_KIND, bytes)
            .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
        let control: Self = decode_body(body, StreamRecordEncoding::Version1)
            .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
            .value;
        if control != expected {
            return Err(StreamDirectoryError::IdentityMismatch);
        }
        Ok(control)
    }
}

impl StreamPublicationGuard {
    /// # Errors
    /// Rejects an inconsistent candidate/base or an oversized/duplicate page set.
    pub fn validate(&self) -> Result<(), StreamDirectoryError> {
        self.head.validate()?;
        if self.pages.len() > MAX_STREAM_PUBLICATION_PAGES
            || self.pages.iter().any(|id| id.0 == [0; 32])
            || self.pages.iter().collect::<HashSet<_>>().len() != self.pages.len()
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        if let Some(base) = &self.base {
            base.validate()?;
            if base.stream_name != self.head.stream_name
                || base.purpose != self.head.purpose
                || base.metadata_group_id != self.head.metadata_group_id
                || base.writer_epoch > self.head.writer_epoch
                || base.generation.checked_add(1) != Some(self.head.generation)
                || self.head.previous != Some(base.identity())
                || base.trim_offset > self.head.trim_offset
                || base.published_tail > self.head.published_tail
            {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        } else if self.head.generation != 1 || self.head.previous.is_some() {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        if let Some(root) = self.head.root {
            if self.base.as_ref().and_then(|base| base.root) != Some(root) && !self.pages.contains(&root.id) {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        Ok(())
    }
}

/// # Errors
/// Rejects invalid indexed heads before computing their canonical publication identity.
pub fn stream_publication_id(
    head: &StreamDirectoryHead,
) -> Result<StreamPublicationId, StreamDirectoryError> {
    let bytes = super::super::encode_stream_directory_head(head)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    Ok(StreamPublicationId(Sha256::digest(bytes).into()))
}

/// # Errors
/// Rejects invalid protection records or serialization failures.
pub fn encode_stream_publication_guard(
    guard: &StreamPublicationGuard,
) -> Result<Vec<u8>, StreamDirectoryError> {
    guard.validate()?;
    encode_record(GUARD_KIND, guard).map_err(|error| StreamDirectoryError::Codec(error.to_string()))
}

/// # Errors
/// Rejects oversized records before decoding, invented allocations, wrong scope or identity.
pub fn decode_stream_publication_guard(
    bytes: &[u8],
    expected: StreamPublicationId,
    name: StreamName,
) -> Result<StreamPublicationGuard, StreamDirectoryError> {
    if bytes.len() > MAX_STREAM_DIRECTORY_RECORD_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    let body =
        decode_envelope(GUARD_KIND, bytes).map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    let guard: StreamPublicationGuard = decode_body(body, StreamRecordEncoding::Version1)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
        .value;
    guard.validate()?;
    if guard.head.stream_name != name || stream_publication_id(&guard.head)? != expected {
        return Err(StreamDirectoryError::IdentityMismatch);
    }
    Ok(guard)
}
