// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit record dispatch around the retained manifest/page body schemas.

use bincode::Options;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use super::{StreamExtentPage, StreamManifest};

const MAGIC: &[u8; 4] = b"CSTR";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 12;
const MANIFEST: u8 = 1;
const EXTENT_PAGE: u8 = 2;

/// Distinguishes raw retained bytes from a record with an explicit envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamRecordEncoding {
    Legacy,
    Version1,
    Version2,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DecodedStreamRecord<T> {
    pub value: T,
    pub encoding: StreamRecordEncoding,
}

#[derive(Debug, Error)]
pub enum StreamRecordError {
    #[error("stream metadata envelope is incomplete or has invalid flags/length")]
    InvalidEnvelope,
    #[error("unsupported stream metadata version {0}")]
    UnsupportedVersion(u16),
    #[error("stream metadata record kind {actual} does not match expected {expected}")]
    WrongKind { expected: u8, actual: u8 },
    #[error("stream metadata codec failed: {0}")]
    Codec(#[from] Box<bincode::ErrorKind>),
    #[error("invalid indexed stream directory: {0}")]
    InvalidDirectory(String),
}

/// # Errors
/// Returns an error when the retained body cannot be encoded or is too large.
pub fn encode_stream_manifest_record(value: &StreamManifest) -> Result<Vec<u8>, StreamRecordError> {
    encode_record(MANIFEST, value)
}

/// # Errors
/// Rejects unknown versions, a different record kind, trailing bytes or a malformed body.
pub fn decode_stream_manifest_record(
    bytes: &[u8],
) -> Result<DecodedStreamRecord<StreamManifest>, StreamRecordError> {
    decode_record(MANIFEST, bytes)
}

/// # Errors
/// Returns an error when the retained body cannot be encoded or is too large.
pub fn encode_stream_extent_page_record(value: &StreamExtentPage) -> Result<Vec<u8>, StreamRecordError> {
    encode_record(EXTENT_PAGE, value)
}

/// # Errors
/// The owning key namespace supplies the encoding: legacy page bodies start
/// with arbitrary stream-ID bytes and can overlap the envelope magic.
/// Rejects wrong versions/kinds, trailing bytes or a malformed body.
pub fn decode_stream_extent_page_record(
    bytes: &[u8],
    encoding: StreamRecordEncoding,
) -> Result<DecodedStreamRecord<StreamExtentPage>, StreamRecordError> {
    let body = match encoding {
        StreamRecordEncoding::Legacy => bytes,
        StreamRecordEncoding::Version2 => return Err(StreamRecordError::UnsupportedVersion(2)),
        StreamRecordEncoding::Version1 => {
            if !bytes.starts_with(MAGIC) {
                return Err(StreamRecordError::InvalidEnvelope);
            }
            decode_envelope(EXTENT_PAGE, bytes)?
        }
    };
    decode_body(body, encoding)
}

pub(super) fn encode_record<T: Serialize>(kind: u8, value: &T) -> Result<Vec<u8>, StreamRecordError> {
    encode_versioned_record(VERSION, kind, value)
}

pub(super) fn encode_versioned_record<T: Serialize>(
    version: u16,
    kind: u8,
    value: &T,
) -> Result<Vec<u8>, StreamRecordError> {
    let body = bincode::serialize(value)?;
    let length = u32::try_from(body.len()).map_err(|_| StreamRecordError::InvalidEnvelope)?;
    let mut bytes = Vec::with_capacity(HEADER_BYTES + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&[kind, 0]);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

fn decode_record<T: DeserializeOwned>(
    kind: u8,
    bytes: &[u8],
) -> Result<DecodedStreamRecord<T>, StreamRecordError> {
    let (body, encoding) = if bytes.starts_with(MAGIC) {
        (decode_envelope(kind, bytes)?, StreamRecordEncoding::Version1)
    } else {
        (bytes, StreamRecordEncoding::Legacy)
    };
    decode_body(body, encoding)
}

pub(super) fn decode_body<T: DeserializeOwned>(
    body: &[u8],
    encoding: StreamRecordEncoding,
) -> Result<DecodedStreamRecord<T>, StreamRecordError> {
    let value = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(body.len() as u64)
        .reject_trailing_bytes()
        .deserialize(body)?;
    Ok(DecodedStreamRecord { value, encoding })
}

pub(super) fn decode_envelope(kind: u8, bytes: &[u8]) -> Result<&[u8], StreamRecordError> {
    decode_versioned_envelope(VERSION, kind, bytes)
}

pub(super) fn decode_versioned_envelope(
    expected_version: u16,
    kind: u8,
    bytes: &[u8],
) -> Result<&[u8], StreamRecordError> {
    if !bytes.starts_with(MAGIC) {
        return Err(StreamRecordError::InvalidEnvelope);
    }
    let header = bytes
        .get(..HEADER_BYTES)
        .ok_or(StreamRecordError::InvalidEnvelope)?;
    let version = u16::from_le_bytes([header[4], header[5]]);
    if version != expected_version {
        return Err(StreamRecordError::UnsupportedVersion(version));
    }
    if header[6] != kind {
        return Err(StreamRecordError::WrongKind {
            expected: kind,
            actual: header[6],
        });
    }
    let length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    if header[7] != 0 || usize::try_from(length).ok() != Some(bytes.len() - HEADER_BYTES) {
        return Err(StreamRecordError::InvalidEnvelope);
    }
    Ok(&bytes[HEADER_BYTES..])
}

/// A stored head is decoded explicitly; compatibility readers cannot publish
/// over an indexed head by treating it as the retained body schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamHeadRecord {
    Legacy(StreamManifest),
    Indexed(super::StreamDirectoryHead),
}

/// # Errors
/// Rejects invalid indexed checkpoints or an unencodable record.
pub fn encode_stream_directory_head(
    value: &super::StreamDirectoryHead,
) -> Result<Vec<u8>, StreamRecordError> {
    value
        .validate()
        .map_err(|error| StreamRecordError::InvalidDirectory(error.to_string()))?;
    encode_versioned_record(2, MANIFEST, value)
}

/// # Errors
/// Rejects unknown versions/kinds, invalid bounds and trailing bytes.
pub fn decode_stream_head_record(bytes: &[u8]) -> Result<StreamHeadRecord, StreamRecordError> {
    if !bytes.starts_with(MAGIC) {
        return decode_stream_manifest_record(bytes).map(|record| StreamHeadRecord::Legacy(record.value));
    }
    let version = bytes.get(4..6).ok_or(StreamRecordError::InvalidEnvelope)?;
    match u16::from_le_bytes([version[0], version[1]]) {
        1 => decode_stream_manifest_record(bytes).map(|record| StreamHeadRecord::Legacy(record.value)),
        2 => {
            let body = decode_versioned_envelope(2, MANIFEST, bytes)?;
            let decoded: DecodedStreamRecord<super::StreamDirectoryHead> =
                decode_body(body, StreamRecordEncoding::Version2)?;
            decoded
                .value
                .validate()
                .map_err(|error| StreamRecordError::InvalidDirectory(error.to_string()))?;
            Ok(StreamHeadRecord::Indexed(decoded.value))
        }
        other => Err(StreamRecordError::UnsupportedVersion(other)),
    }
}
