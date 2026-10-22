// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Public self-validating chunk frame encoding and location arithmetic.

use super::encoding::crc32c_parts;
use super::revision::parse_role;
use super::{
    ChunkId, FrameError, FrameHeaderPrefix, FrameMagic, FrameRole, ParsedFrame, ParsedFrameViews,
    FRAME_FOOTER_BYTES, FRAME_GROUP_HEADER_BYTES, FRAME_HEADER_PREFIX_BYTES, FRAME_V2_HEADER_BYTES,
    MAX_FRAME_BYTES,
};

/// Verify a frame over ordered views without assembling its payload.
///
/// # Errors
/// Returns an error for an incomplete, malformed, or corrupt frame.
pub fn parse_frame_views(
    views: &[&[u8]],
    expected_chunk_id: ChunkId,
) -> Result<ParsedFrameViews, FrameError> {
    let mut header_bytes = [0_u8; FRAME_HEADER_PREFIX_BYTES];
    copy_view_range(views, 0, &mut header_bytes)?;
    let header = parse_header(&header_bytes)?;
    let length = frame_length(header)?;
    let available = views
        .iter()
        .fold(0_usize, |total, view| total.saturating_add(view.len()));
    if available < length {
        return Err(FrameError::Incomplete {
            required_bytes: length,
        });
    }
    let footer_start = length - FRAME_FOOTER_BYTES;
    let mut footer = [0_u8; FRAME_FOOTER_BYTES];
    copy_view_range(views, footer_start, &mut footer)?;
    let expected_crc = u32::from_le_bytes(
        footer[..4]
            .try_into()
            .map_err(|_| FrameError::InvalidRegionLength)?,
    );
    let mut crc = 0_u32;
    let mut cursor = 0_usize;
    for view in views {
        let included = footer_start.saturating_sub(cursor).min(view.len());
        if included > 0 {
            crc = crowdb_common::ec_isal::crc32c_update(crc, &view[..included]);
        }
        cursor = cursor.saturating_add(view.len());
        if cursor >= footer_start {
            break;
        }
    }
    let mut extension = [0; FRAME_GROUP_HEADER_BYTES];
    let role = if header.magic.is_v2() {
        let size = usize::from(header.payload_offset);
        copy_view_range(views, 0, &mut extension[..size])?;
        parse_role(header, &extension[..size])?
    } else {
        FrameRole::Ordinary
    };
    if crc != expected_crc {
        return Err(FrameError::ChecksumMismatch);
    }
    let chunk_id = ChunkId {
        high: u64::from_be_bytes(
            footer[4..12]
                .try_into()
                .map_err(|_| FrameError::InvalidRegionLength)?,
        ),
        low: u64::from_be_bytes(
            footer[12..20]
                .try_into()
                .map_err(|_| FrameError::InvalidRegionLength)?,
        ),
    };
    if chunk_id != expected_chunk_id {
        return Err(FrameError::ChunkIdMismatch);
    }
    Ok(ParsedFrameViews {
        header,
        role,
        chunk_id,
        physical_length: length,
    })
}

fn copy_view_range(views: &[&[u8]], start: usize, target: &mut [u8]) -> Result<(), FrameError> {
    let required_bytes = start.saturating_add(target.len());
    let mut cursor = 0_usize;
    let mut copied = 0_usize;
    for view in views {
        let view_end = cursor.saturating_add(view.len());
        if view_end > start && copied < target.len() {
            let from = start.saturating_sub(cursor);
            let count = (view.len() - from).min(target.len() - copied);
            target[copied..copied + count].copy_from_slice(&view[from..from + count]);
            copied += count;
        }
        cursor = view_end;
        if copied == target.len() {
            return Ok(());
        }
    }
    Err(FrameError::Incomplete { required_bytes })
}

/// Parse and verify one complete frame. The expected chunk ID is mandatory so
/// a valid frame copied from a different chunk is rejected.
///
/// # Errors
///
/// Returns an error when the frame is incomplete, malformed, belongs to a
/// different chunk, or fails its CRC32C verification.
pub fn parse_frame(bytes: &[u8], expected_chunk_id: ChunkId) -> Result<ParsedFrame<'_>, FrameError> {
    let header = parse_header(bytes)?;
    let length = frame_length(header)?;
    if bytes.len() < length {
        return Err(FrameError::Incomplete {
            required_bytes: length,
        });
    }
    let footer_start = length - FRAME_FOOTER_BYTES;
    let checksum = u32::from_le_bytes(bytes[footer_start..footer_start + 4].try_into().map_err(|_| {
        FrameError::Incomplete {
            required_bytes: length,
        }
    })?);
    let role = parse_role(header, bytes)?;
    let crc = crc32c_parts([&bytes[..footer_start]]);
    if crc != checksum {
        return Err(FrameError::ChecksumMismatch);
    }
    let chunk_id =
        ChunkId {
            high: u64::from_be_bytes(bytes[footer_start + 4..footer_start + 12].try_into().map_err(
                |_| FrameError::Incomplete {
                    required_bytes: length,
                },
            )?),
            low: u64::from_be_bytes(bytes[footer_start + 12..length].try_into().map_err(|_| {
                FrameError::Incomplete {
                    required_bytes: length,
                }
            })?),
        };
    if chunk_id != expected_chunk_id {
        return Err(FrameError::ChunkIdMismatch);
    }
    let payload_start = usize::from(header.payload_offset);
    let payload_end = payload_start + usize::from(header.payload_size);
    Ok(ParsedFrame {
        header,
        role,
        payload: &bytes[payload_start..payload_end],
        chunk_id,
        physical_length: length,
    })
}

///
/// # Errors
///
/// Returns an error when the prefix is incomplete, has an unknown magic, or
/// declares a payload before the fixed prefix.
pub fn parse_header(bytes: &[u8]) -> Result<FrameHeaderPrefix, FrameError> {
    if bytes.len() < FRAME_HEADER_PREFIX_BYTES {
        return Err(FrameError::Incomplete {
            required_bytes: FRAME_HEADER_PREFIX_BYTES,
        });
    }
    let magic = FrameMagic::try_from(u16::from_le_bytes(bytes[0..2].try_into().map_err(|_| {
        FrameError::Incomplete {
            required_bytes: FRAME_HEADER_PREFIX_BYTES,
        }
    })?))?;
    let payload_offset = u16::from_le_bytes(bytes[2..4].try_into().map_err(|_| FrameError::Incomplete {
        required_bytes: FRAME_HEADER_PREFIX_BYTES,
    })?);
    if usize::from(payload_offset) < FRAME_HEADER_PREFIX_BYTES {
        return Err(FrameError::InvalidPayloadOffset {
            offset: payload_offset,
        });
    }
    if magic.is_v2()
        && ![FRAME_V2_HEADER_BYTES, FRAME_GROUP_HEADER_BYTES].contains(&usize::from(payload_offset))
    {
        return Err(FrameError::InvalidPayloadOffset {
            offset: payload_offset,
        });
    }
    Ok(FrameHeaderPrefix {
        magic,
        payload_offset,
        payload_size: u16::from_le_bytes(bytes[4..6].try_into().map_err(|_| FrameError::Incomplete {
            required_bytes: FRAME_HEADER_PREFIX_BYTES,
        })?),
        write_time_ms: u64::from_le_bytes(bytes[6..14].try_into().map_err(|_| FrameError::Incomplete {
            required_bytes: FRAME_HEADER_PREFIX_BYTES,
        })?),
    })
}

///
/// # Errors
///
/// Returns an error when the encoded length overflows or exceeds 64 KiB.
pub fn frame_length(header: FrameHeaderPrefix) -> Result<usize, FrameError> {
    let length = usize::from(header.payload_offset)
        .checked_add(usize::from(header.payload_size))
        .and_then(|value| value.checked_add(FRAME_FOOTER_BYTES))
        .ok_or(FrameError::LengthOverflow)?;
    if length > MAX_FRAME_BYTES {
        return Err(FrameError::FrameTooLarge);
    }
    Ok(length)
}
