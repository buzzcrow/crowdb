// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact immutable whole-chunk bounds published when an active attachment ends.

use serde::{Deserialize, Serialize};

use super::super::record::{decode_body, decode_envelope, encode_record, StreamRecordEncoding};
use super::{StreamDirectoryError, StreamName, StreamPurpose, MAX_STREAM_CHUNK_PHYSICAL_BYTES};
use crate::{common::ChunkId, frame::FrameLayout};

const SPAN_KIND: u8 = 7;
const MAX_SPAN_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSealedChunkSpan {
    pub stream_name: StreamName,
    pub purpose: StreamPurpose,
    pub chunk_id: ChunkId,
    pub logical_start: u64,
    pub logical_end: u64,
    pub physical_end: u64,
}

impl StreamSealedChunkSpan {
    /// # Errors
    /// Rejects wrong purpose, inverted ranges, impossible geometry or capacity.
    pub fn validate(self) -> Result<(), StreamDirectoryError> {
        if !self.purpose.matches(self.chunk_id) {
            return Err(StreamDirectoryError::WrongPurpose);
        }
        let length = self
            .logical_end
            .checked_sub(self.logical_start)
            .ok_or(StreamDirectoryError::InvalidBounds)?;
        let minimum = FrameLayout::StreamGroupV2
            .physical_length(length)
            .map_err(|_| StreamDirectoryError::InvalidBounds)?;
        if self.physical_end > MAX_STREAM_CHUNK_PHYSICAL_BYTES
            || self.physical_end < minimum
            || (length == 0 && self.physical_end != 0)
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        Ok(())
    }
}

/// # Errors
/// Rejects invalid exact bounds or an oversized record.
pub fn encode_stream_sealed_chunk_span(
    span: &StreamSealedChunkSpan,
) -> Result<Vec<u8>, StreamDirectoryError> {
    span.validate()?;
    let bytes =
        encode_record(SPAN_KIND, span).map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    if bytes.len() > MAX_SPAN_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    Ok(bytes)
}

/// # Errors
/// Rejects unknown envelopes, invalid bounds, trailing bytes or wrong keys.
pub fn decode_stream_sealed_chunk_span(
    bytes: &[u8],
    name: StreamName,
    chunk_id: ChunkId,
) -> Result<StreamSealedChunkSpan, StreamDirectoryError> {
    if bytes.len() > MAX_SPAN_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    let body =
        decode_envelope(SPAN_KIND, bytes).map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    let span: StreamSealedChunkSpan = decode_body(body, StreamRecordEncoding::Version1)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
        .value;
    span.validate()?;
    if span.stream_name != name || span.chunk_id != chunk_id {
        return Err(StreamDirectoryError::IdentityMismatch);
    }
    Ok(span)
}
