// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Public self-validating chunk frame encoding and location arithmetic.

use thiserror::Error;

use crate::common::ChunkId;

pub const FRAME_HEADER_PREFIX_BYTES: usize = 14;
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
    RepoSmallV2 = 0x0102,
    RepoLargeV2 = 0x0202,
    StreamV2 = 0x0302,
    BtreePageV2 = 0x0402,
    PageIndexV2 = 0x0502,
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
            0x0102 => Ok(Self::RepoSmallV2),
            0x0202 => Ok(Self::RepoLargeV2),
            0x0302 => Ok(Self::StreamV2),
            0x0402 => Ok(Self::BtreePageV2),
            0x0502 => Ok(Self::PageIndexV2),
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
    #[error("frame header or footer region has an invalid length")]
    InvalidRegionLength,
    #[error("frame chunk ID does not match the expected chunk")]
    ChunkIdMismatch,
    #[error("frame CRC32C does not match")]
    ChecksumMismatch,
    #[error("location range is invalid")]
    InvalidLocationRange,
    #[error("locations are not physically contiguous")]
    NonContiguousLocation,
    #[error("frame role or extension is invalid")]
    InvalidRole,
    #[error("append group is inconsistent or incomplete")]
    InvalidGroup,
}

pub struct ParsedFrame<'a> {
    pub header: FrameHeaderPrefix,
    pub role: FrameRole,
    pub payload: &'a [u8],
    pub chunk_id: ChunkId,
    pub physical_length: usize,
}

/// Verified frame metadata when the physical bytes span several buffers.
pub struct ParsedFrameViews {
    pub header: FrameHeaderPrefix,
    pub role: FrameRole,
    pub chunk_id: ChunkId,
    pub physical_length: usize,
}

mod encoding;
mod geometry;
mod group;
mod parsing;
mod revision;

pub use encoding::*;
pub use geometry::*;
pub use group::*;
pub use parsing::*;
pub use revision::*;
