// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use sha2::{Digest, Sha256};

use super::super::record::{decode_body, decode_envelope, encode_record, StreamRecordEncoding};
use super::{
    StreamDirectoryError, StreamDirectoryPage, StreamDirectoryPageId, StreamDirectoryRef, StreamName,
    StreamPurpose, MAX_STREAM_DIRECTORY_RECORD_BYTES,
};

const PAGE_KIND: u8 = 3;

/// # Errors
/// Rejects records above the physical page budget before hashing their identity.
pub fn stream_directory_page_id(bytes: &[u8]) -> Result<StreamDirectoryPageId, StreamDirectoryError> {
    if bytes.len() > MAX_STREAM_DIRECTORY_RECORD_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    Ok(StreamDirectoryPageId(Sha256::digest(bytes).into()))
}

/// # Errors
/// Rejects invalid bounds, purpose or a record larger than the page budget.
pub fn encode_stream_directory_page(
    page: &StreamDirectoryPage,
) -> Result<(StreamDirectoryRef, Vec<u8>), StreamDirectoryError> {
    let bounds = page.bounds()?;
    let bytes =
        encode_record(PAGE_KIND, page).map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    if bytes.len() > MAX_STREAM_DIRECTORY_RECORD_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    let id = stream_directory_page_id(&bytes)?;
    Ok((
        StreamDirectoryRef {
            id,
            level: bounds.level,
            first_logical: bounds.first_logical,
            end_logical: bounds.end_logical,
            extent_count: bounds.extent_count,
        },
        bytes,
    ))
}

/// # Errors
/// Rejects corrupt content, wrong scope, invalid geometry or a mismatched fence.
pub fn decode_stream_directory_page(
    bytes: &[u8],
    expected: StreamDirectoryRef,
    stream_name: StreamName,
    purpose: StreamPurpose,
) -> Result<StreamDirectoryPage, StreamDirectoryError> {
    expected.validate()?;
    if bytes.len() > MAX_STREAM_DIRECTORY_RECORD_BYTES {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    if stream_directory_page_id(bytes)? != expected.id {
        return Err(StreamDirectoryError::IdentityMismatch);
    }
    let body =
        decode_envelope(PAGE_KIND, bytes).map_err(|error| StreamDirectoryError::Codec(error.to_string()))?;
    let page: StreamDirectoryPage = decode_body(body, StreamRecordEncoding::Version1)
        .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
        .value;
    if page.stream_name() != stream_name || page.purpose() != purpose {
        return Err(StreamDirectoryError::WrongPurpose);
    }
    let bounds = page.bounds()?;
    if bounds.level != expected.level
        || bounds.first_logical != expected.first_logical
        || bounds.end_logical != expected.end_logical
        || bounds.extent_count != expected.extent_count
    {
        return Err(StreamDirectoryError::IdentityMismatch);
    }
    Ok(page)
}
