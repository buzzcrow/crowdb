// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact bounded read-view descriptors retained independently of the root index.

use serde::{Deserialize, Serialize};

use super::super::record::{decode_body, decode_envelope, encode_record, StreamRecordEncoding};
use super::{StreamDirectoryError, StreamDirectoryHead, StreamName};

const PIN_KIND: u8 = 6;
const MAX_PIN_BYTES: usize = 1024;
pub const MAX_STREAM_ROOT_PINS: usize = 512;
pub const MAX_STREAM_ROOT_PIN_LEASE_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamRootPinId(pub [u8; 16]);

/// The complete bounded head survives retirement of its epoch/generation index
/// record. A pin grants only its captured logical interval, never a later tail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRootPin {
    pub id: StreamRootPinId,
    pub head: StreamDirectoryHead,
    pub needed_offset: u64,
    pub captured_tail: u64,
    /// None is a durable consumer pin; leased readers must renew before expiry.
    pub expires_at_ms: Option<u64>,
}

impl StreamRootPin {
    /// # Errors
    /// Rejects invalid heads, identities, deadlines and out-of-view ranges.
    pub fn validate(&self) -> Result<(), StreamDirectoryError> {
        self.head.validate()?;
        if self.id.0 == [0; 16]
            || self.needed_offset < self.head.trim_offset
            || self.needed_offset > self.captured_tail
            || self.captured_tail > self.head.published_tail
            || self.expires_at_ms == Some(0)
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        Ok(())
    }
}

/// # Errors
/// Rejects invalid pin scope/ranges or an oversized record.
pub fn encode_stream_root_pin(pin: &StreamRootPin) -> Result<Vec<u8>, StreamDirectoryError> {
    pin.validate()?;
    let bytes =
        encode_record(PIN_KIND, pin).map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    if bytes.len() > MAX_PIN_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    Ok(bytes)
}

/// # Errors
/// Rejects unsupported envelopes, trailing bytes, invalid ranges or wrong keys.
pub fn decode_stream_root_pin(
    bytes: &[u8],
    name: StreamName,
    id: StreamRootPinId,
) -> Result<StreamRootPin, StreamDirectoryError> {
    if bytes.len() > MAX_PIN_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    let body =
        decode_envelope(PIN_KIND, bytes).map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    let pin: StreamRootPin = decode_body(body, StreamRecordEncoding::Version1)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
        .value;
    pin.validate()?;
    if pin.id != id || pin.head.stream_name != name {
        return Err(StreamDirectoryError::IdentityMismatch);
    }
    Ok(pin)
}
