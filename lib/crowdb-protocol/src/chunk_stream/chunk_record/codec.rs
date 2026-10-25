// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{StreamChunkRecord, StreamChunkRecordError, MAX_STREAM_CHUNK_RECORD_BYTES};
use bincode::Options;
use StreamChunkRecordError::Invalid;
const HEADER: &[u8; 8] = b"CSCR\x01\x00\x00\x00";

/// # Errors
/// Rejects malformed mappings/identity and oversized encoded records.
pub fn encode_stream_chunk_record(record: &StreamChunkRecord) -> Result<Vec<u8>, StreamChunkRecordError> {
    record.validate()?;
    let body = bincode::serialize(record).map_err(|_| Invalid("record encoding"))?;
    if body.len() > MAX_STREAM_CHUNK_RECORD_BYTES - 16 {
        return Err(Invalid("record byte budget"));
    }
    let mut bytes = Vec::with_capacity(body.len() + 16);
    bytes.extend_from_slice(HEADER);
    bytes.extend_from_slice(
        &u32::try_from(body.len())
            .map_err(|_| Invalid("record length"))?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&body);
    let checksum = crowdb_common::ec_isal::crc32c_update(0, &bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

/// # Errors
/// Rejects unsupported schemas, checksum/length errors and invalid mappings.
pub fn decode_stream_chunk_record(bytes: &[u8]) -> Result<StreamChunkRecord, StreamChunkRecordError> {
    if bytes.len() < 16
        || bytes.len() > MAX_STREAM_CHUNK_RECORD_BYTES
        || bytes.get(..8) != Some(HEADER.as_slice())
    {
        return Err(Invalid("record envelope"));
    }
    let length = u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| Invalid("record length"))?) as usize;
    if length != bytes.len() - 16 {
        return Err(Invalid("record length"));
    }
    let checksum = u32::from_le_bytes(
        bytes[bytes.len() - 4..]
            .try_into()
            .map_err(|_| Invalid("record checksum"))?,
    );
    if checksum != crowdb_common::ec_isal::crc32c_update(0, &bytes[..bytes.len() - 4]) {
        return Err(Invalid("record checksum"));
    }
    let record: StreamChunkRecord = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(length as u64)
        .reject_trailing_bytes()
        .deserialize(&bytes[12..bytes.len() - 4])
        .map_err(|_| Invalid("record decoding"))?;
    record.validate()?;
    Ok(record)
}
