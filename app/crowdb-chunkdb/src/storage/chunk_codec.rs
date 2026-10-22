// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Strict versioned chunk metadata records.

use super::{Result, StoreError};
use bincode::Options;
use crowdb_protocol::chunkdb::rpc::Chunk;

const MAGIC: &[u8; 4] = b"CCHK";
const VERSION: u16 = 2;
const HEADER: usize = 12;

pub(super) fn encode_chunk(chunk: &Chunk) -> Result<Vec<u8>> {
    let body = bincode::serialize(chunk).map_err(codec_error)?;
    let length = u32::try_from(body.len()).map_err(codec_error)?;
    let mut bytes = Vec::with_capacity(HEADER + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&[0, 0]);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub(super) fn decode_chunk(bytes: &[u8]) -> Result<Chunk> {
    let header = bytes
        .get(..HEADER)
        .ok_or_else(|| codec_error("incomplete chunk envelope"))?;
    let version = u16::from_le_bytes([header[4], header[5]]);
    let length = u32::from_le_bytes(header[8..12].try_into().map_err(codec_error)?);
    if &header[..4] != MAGIC
        || version != VERSION
        || header[6..8] != [0, 0]
        || usize::try_from(length).ok() != Some(bytes.len() - HEADER)
    {
        return Err(codec_error("unsupported or malformed chunk envelope"));
    }
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(u64::from(length))
        .reject_trailing_bytes()
        .deserialize(&bytes[HEADER..])
        .map_err(codec_error)
}

fn codec_error(error: impl std::fmt::Display) -> StoreError {
    StoreError::Serde(format!("chunk metadata codec: {error}"))
}
