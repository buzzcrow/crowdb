use std::ops::Range;

use crate::error::ValidationError;

use super::{IcebergKey, TableStorageKey, MAX_KEY_BYTES};

/// Dispatches storage envelopes without trying another layout on decode failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageKey {
    Legacy(IcebergKey),
    NativeTable(TableStorageKey),
}

impl StorageKey {
    /// # Errors
    /// Rejects invalid keys in the explicitly selected layout.
    pub fn encode(&self) -> Result<Vec<u8>, ValidationError> {
        match self {
            Self::Legacy(key) => key.encode(),
            Self::NativeTable(key) => key.encode(),
        }
    }
    /// # Errors
    /// Rejects unsupported versions and malformed keys in their selected layout.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValidationError> {
        if bytes.len() > MAX_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        if bytes.get(..4) != Some(b"ICE\0") {
            return Err(ValidationError::Key);
        }
        match bytes.get(4).copied().ok_or(ValidationError::Key)? {
            1 => IcebergKey::decode(bytes).map(Self::Legacy),
            2 => TableStorageKey::decode(bytes).map(Self::NativeTable),
            version => Err(ValidationError::KeyVersion(version)),
        }
    }

    /// # Errors
    /// Rejects unbounded, reversed or oversized scans and scans crossing layouts.
    pub fn validate_range(range: &Range<Vec<u8>>) -> Result<(), ValidationError> {
        if range.start.len() > MAX_KEY_BYTES || range.end.len() > MAX_KEY_BYTES {
            return Err(ValidationError::KeyTooLarge);
        }
        let (lower, upper): (&[u8], &[u8]) = match range.start.get(..5) {
            Some(b"ICE\0\x01") => (b"ICE\0\x01", b"ICE\0\x02"),
            Some(b"ICE\0\x02") => (b"ICE\0\x02", b"ICE\0\x03"),
            _ => return Err(ValidationError::Key),
        };
        if range.start.as_slice() < lower || range.end.as_slice() > upper || range.start >= range.end {
            return Err(ValidationError::Key);
        }
        Ok(())
    }
}
