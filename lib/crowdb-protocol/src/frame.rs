// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Public self-validating chunk frame encoding and location arithmetic.

use std::ops::Range;

use thiserror::Error;

use crate::common::ChunkId;

pub const FRAME_HEADER_PREFIX_BYTES: usize = 14;
const FRAME_HEADER_PREFIX_BYTES_U16: u16 = 14;
pub const FRAME_FOOTER_BYTES: usize = 20;
pub const MAX_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_FRAME_PAYLOAD_BYTES: usize = MAX_FRAME_BYTES - FRAME_HEADER_PREFIX_BYTES - FRAME_FOOTER_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum FrameMagic {
    RepoSmallV1 = 0x0101,
    RepoLargeV1 = 0x0201,
    StreamV1 = 0x0301,
    BtreePageV1 = 0x0401,
    PageIndexV1 = 0x0501,
}

impl TryFrom<u16> for FrameMagic {
    type Error = FrameError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0x0101 => Ok(Self::RepoSmallV1),
            0x0201 => Ok(Self::RepoLargeV1),
            0x0301 => Ok(Self::StreamV1),
            0x0401 => Ok(Self::BtreePageV1),
            0x0501 => Ok(Self::PageIndexV1),
            _ => Err(FrameError::UnknownMagic(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeaderPrefix {
    pub magic: FrameMagic,
    pub payload_offset: u16,
    pub payload_size: u16,
    pub write_time_ms: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("frame is incomplete: requires {required_bytes} bytes")]
    Incomplete { required_bytes: usize },
    #[error("unknown frame magic {0:#06x}")]
    UnknownMagic(u16),
    #[error("frame payload is too large")]
    PayloadTooLarge,
    #[error("frame payload offset {offset} is invalid")]
    InvalidPayloadOffset { offset: u16 },
    #[error("frame length overflows")]
    LengthOverflow,
    #[error("frame length exceeds 64 KiB")]
    FrameTooLarge,
    #[error("frame chunk ID does not match the expected chunk")]
    ChunkIdMismatch,
    #[error("frame CRC32C does not match")]
    ChecksumMismatch,
    #[error("location range is invalid")]
    InvalidLocationRange,
    #[error("locations are not physically contiguous")]
    NonContiguousLocation,
}

pub struct ParsedFrame<'a> {
    pub header: FrameHeaderPrefix,
    pub payload: &'a [u8],
    pub chunk_id: ChunkId,
    pub physical_length: usize,
}

/// Encode a canonical v1 frame without header extensions.
///
/// # Errors
///
/// Returns [`FrameError::PayloadTooLarge`] when `payload` cannot fit one frame.
pub fn encode_frame(
    magic: FrameMagic,
    chunk_id: ChunkId,
    payload: &[u8],
    write_time_ms: u64,
) -> Result<Vec<u8>, FrameError> {
    let payload_size = u16::try_from(payload.len()).map_err(|_| FrameError::PayloadTooLarge)?;
    let header = FrameHeaderPrefix {
        magic,
        payload_offset: FRAME_HEADER_PREFIX_BYTES_U16,
        payload_size,
        write_time_ms,
    };
    let length = frame_length(header)?;
    let mut frame = Vec::with_capacity(length);
    write_header(&mut frame, header);
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&chunk_id.high.to_be_bytes());
    frame.extend_from_slice(&chunk_id.low.to_be_bytes());
    let checksum = crc32c(&frame);
    frame.splice(
        length - FRAME_FOOTER_BYTES..length - FRAME_FOOTER_BYTES,
        checksum.to_le_bytes(),
    );
    Ok(frame)
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
    if crc32c_frame_parts(&bytes[..footer_start], &bytes[footer_start + 4..length]) != checksum {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChunkLocation {
    pub chunk_id: ChunkId,
    pub frame_offset: u64,
    pub logical_length: u64,
}

impl ChunkLocation {
    /// Return the total bytes occupied by this location's frames.
    ///
    /// # Errors
    ///
    /// Returns an error if the calculated byte count overflows.
    pub fn physical_length(self) -> Result<u64, FrameError> {
        framed_physical_length(self.logical_length)
    }

    /// Return the exclusive physical end offset.
    ///
    /// # Errors
    ///
    /// Returns an error if the frame length or end offset overflows.
    pub fn end_offset(self) -> Result<u64, FrameError> {
        self.frame_offset
            .checked_add(self.physical_length()?)
            .ok_or(FrameError::LengthOverflow)
    }

    /// Map a logical subrange to the minimal complete-frame physical range.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid subrange or an arithmetic overflow.
    pub fn physical_range_for_subrange(self, range: Range<u64>) -> Result<Range<u64>, FrameError> {
        if range.start > range.end || range.end > self.logical_length {
            return Err(FrameError::InvalidLocationRange);
        }
        if range.is_empty() {
            return Ok(self.frame_offset..self.frame_offset);
        }
        let first = range.start / max_payload_u64();
        let last = (range.end - 1) / max_payload_u64();
        let start = self
            .frame_offset
            .checked_add(
                first
                    .checked_mul(MAX_FRAME_BYTES as u64)
                    .ok_or(FrameError::LengthOverflow)?,
            )
            .ok_or(FrameError::LengthOverflow)?;
        let end = if last + 1 == frame_count(self.logical_length) {
            self.end_offset()?
        } else {
            self.frame_offset
                .checked_add(
                    (last + 1)
                        .checked_mul(MAX_FRAME_BYTES as u64)
                        .ok_or(FrameError::LengthOverflow)?,
                )
                .ok_or(FrameError::LengthOverflow)?
        };
        Ok(start..end)
    }
}

/// Merge physically adjacent, frame-aligned locations.
///
/// # Errors
///
/// Returns an error when a location length or end offset overflows.
pub fn merge_adjacent_locations(locations: &[ChunkLocation]) -> Result<Vec<ChunkLocation>, FrameError> {
    let mut merged: Vec<ChunkLocation> = Vec::with_capacity(locations.len());
    for location in locations {
        if let Some(previous) = merged.last_mut() {
            if previous.chunk_id == location.chunk_id
                && previous.logical_length % max_payload_u64() == 0
                && previous.end_offset()? == location.frame_offset
            {
                previous.logical_length = previous
                    .logical_length
                    .checked_add(location.logical_length)
                    .ok_or(FrameError::LengthOverflow)?;
                continue;
            }
        }
        merged.push(*location);
    }
    Ok(merged)
}

/// Validate that every location has a nonempty logical payload.
///
/// # Errors
///
/// Returns [`FrameError::NonContiguousLocation`] for an empty location.
pub fn validate_contiguous_locations(locations: &[ChunkLocation]) -> Result<(), FrameError> {
    for pair in locations.windows(2) {
        if pair[0].logical_length == 0 || pair[1].logical_length == 0 {
            return Err(FrameError::NonContiguousLocation);
        }
    }
    Ok(())
}

fn framed_physical_length(logical_length: u64) -> Result<u64, FrameError> {
    if logical_length == 0 {
        return Ok(0);
    }
    let full = logical_length / max_payload_u64();
    let tail = logical_length % max_payload_u64();
    let full_bytes = full
        .checked_mul(MAX_FRAME_BYTES as u64)
        .ok_or(FrameError::LengthOverflow)?;
    if tail == 0 {
        Ok(full_bytes)
    } else {
        full_bytes
            .checked_add(FRAME_HEADER_PREFIX_BYTES as u64)
            .and_then(|value| value.checked_add(tail))
            .and_then(|value| value.checked_add(FRAME_FOOTER_BYTES as u64))
            .ok_or(FrameError::LengthOverflow)
    }
}

fn frame_count(logical_length: u64) -> u64 {
    logical_length.div_ceil(max_payload_u64())
}

const fn max_payload_u64() -> u64 {
    MAX_FRAME_PAYLOAD_BYTES as u64
}

fn write_header(frame: &mut Vec<u8>, header: FrameHeaderPrefix) {
    frame.extend_from_slice(&(header.magic as u16).to_le_bytes());
    frame.extend_from_slice(&header.payload_offset.to_le_bytes());
    frame.extend_from_slice(&header.payload_size.to_le_bytes());
    frame.extend_from_slice(&header.write_time_ms.to_le_bytes());
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = 0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82F6_3B78 & (0_u32.wrapping_sub(crc & 1)));
        }
    }
    crc
}

fn crc32c_frame_parts(prefix: &[u8], chunk_id: &[u8]) -> u32 {
    let mut bytes = Vec::with_capacity(prefix.len() + chunk_id.len());
    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(chunk_id);
    crc32c(&bytes)
}
