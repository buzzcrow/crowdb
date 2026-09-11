// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{ChunkKvError, Result, WalRecord};
use crowdb_chunk_stream::ChunkId;

const MAGIC: [u8; 4] = *b"CKVJ";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = 12;
const CRC_BYTES: usize = 4;
const CHUNK_ID_BYTES: usize = 16;
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedFrame {
    pub record: WalRecord,
    pub chunk_id: ChunkId,
    pub bytes_consumed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameDecode {
    Complete(Box<DecodedFrame>),
    Incomplete { required_bytes: usize },
}

/// Encodes one versioned, length-delimited, CRC32C-protected journal frame.
///
/// # Errors
///
/// Returns an error when serialization fails or the frame exceeds its bound.
pub fn encode_frame(record: &WalRecord) -> Result<Vec<u8>> {
    let body = bincode::serialize(record)
        .map_err(|error| ChunkKvError::InvalidRequest(format!("WAL record serialization failed: {error}")))?;
    let frame_len = HEADER_BYTES
        .checked_add(body.len())
        .and_then(|length| length.checked_add(CRC_BYTES))
        .and_then(|length| length.checked_add(CHUNK_ID_BYTES))
        .ok_or_else(|| ChunkKvError::InvalidRequest("WAL frame length overflows".into()))?;
    if frame_len > MAX_FRAME_BYTES {
        return Err(ChunkKvError::InvalidRequest(
            "WAL frame exceeds maximum size".into(),
        ));
    }
    let body_len = u32::try_from(body.len())
        .map_err(|_| ChunkKvError::InvalidRequest("WAL frame body exceeds addressable size".into()))?;
    let mut frame = Vec::with_capacity(frame_len - CHUNK_ID_BYTES);
    frame.extend_from_slice(&MAGIC);
    frame.extend_from_slice(&VERSION.to_le_bytes());
    frame.extend_from_slice(&0_u16.to_le_bytes());
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(&body);
    let checksum = crowdb_tree_ffi::crc32c(&frame);
    frame.extend_from_slice(&checksum.to_le_bytes());
    Ok(frame)
}

/// Decodes one frame and reports a bounded incomplete tail without consuming it.
///
/// # Errors
///
/// Returns corruption for invalid magic/version/flags/length/checksum/body.
pub fn decode_frame(bytes: &[u8]) -> Result<FrameDecode> {
    if bytes.len() < HEADER_BYTES {
        return Ok(FrameDecode::Incomplete {
            required_bytes: HEADER_BYTES,
        });
    }
    if bytes[..4] != MAGIC {
        return Err(ChunkKvError::JournalCorruption("WAL frame magic mismatch".into()));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if version != VERSION || flags != 0 {
        return Err(ChunkKvError::JournalCorruption(
            "WAL frame version or flags are unsupported".into(),
        ));
    }
    let body_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let frame_len = HEADER_BYTES
        .checked_add(body_len)
        .and_then(|length| length.checked_add(CRC_BYTES))
        .and_then(|length| length.checked_add(CHUNK_ID_BYTES))
        .ok_or_else(|| ChunkKvError::JournalCorruption("WAL frame length overflows".into()))?;
    if frame_len > MAX_FRAME_BYTES {
        return Err(ChunkKvError::JournalCorruption(
            "WAL frame exceeds maximum size".into(),
        ));
    }
    if bytes.len() < frame_len {
        return Ok(FrameDecode::Incomplete {
            required_bytes: frame_len,
        });
    }
    let checksum_end = frame_len - CHUNK_ID_BYTES;
    let checksum_begin = checksum_end - CRC_BYTES;
    let expected = u32::from_le_bytes(
        bytes[checksum_begin..checksum_end]
            .try_into()
            .map_err(|_| ChunkKvError::JournalCorruption("WAL checksum is truncated".into()))?,
    );
    let observed = crowdb_tree_ffi::crc32c(&bytes[..checksum_begin]);
    if observed != expected {
        return Err(ChunkKvError::JournalCorruption(
            "WAL frame checksum mismatch".into(),
        ));
    }
    let record = bincode::deserialize(&bytes[HEADER_BYTES..checksum_begin])
        .map_err(|error| ChunkKvError::JournalCorruption(format!("WAL record decode failed: {error}")))?;
    let chunk_id = ChunkId {
        high: u64::from_be_bytes(
            bytes[checksum_end..checksum_end + 8]
                .try_into()
                .map_err(|_| ChunkKvError::JournalCorruption("WAL chunk ID is truncated".into()))?,
        ),
        low: u64::from_be_bytes(
            bytes[checksum_end + 8..frame_len]
                .try_into()
                .map_err(|_| ChunkKvError::JournalCorruption("WAL chunk ID is truncated".into()))?,
        ),
    };
    Ok(FrameDecode::Complete(Box::new(DecodedFrame {
        record,
        chunk_id,
        bytes_consumed: frame_len,
    })))
}
