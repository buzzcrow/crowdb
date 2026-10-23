// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    ChunkLocation, FrameError, FrameHeaderPrefix, FrameMagic, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES,
    MAX_FRAME_BYTES,
};

pub const FRAME_V2_HEADER_BYTES: usize = 15;
pub const FRAME_GROUP_HEADER_BYTES: usize = 27;
pub const MAX_FRAME_V2_PAYLOAD_BYTES: usize = MAX_FRAME_BYTES - FRAME_V2_HEADER_BYTES - FRAME_FOOTER_BYTES;
pub const MAX_FRAME_GROUP_PAYLOAD_BYTES: usize =
    MAX_FRAME_BYTES - FRAME_GROUP_HEADER_BYTES - FRAME_FOOTER_BYTES;

/// Stream grouping is part of the checked header, never an ordinary object's payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameRole {
    Ordinary,
    Head { logical_length: u64, frame_count: u32 },
    Follower { head_offset: u64, ordinal: u32 },
}

/// Parse the bounded extension after the fixed prefix has established frame size.
pub(super) fn parse_role(header: FrameHeaderPrefix, bytes: &[u8]) -> Result<FrameRole, FrameError> {
    let size = usize::from(header.payload_offset);
    if bytes.len() < size {
        return Err(FrameError::Incomplete { required_bytes: size });
    }
    match (size, bytes[FRAME_HEADER_PREFIX_BYTES]) {
        (FRAME_V2_HEADER_BYTES, 0) if header.magic != FrameMagic::StreamV2 => Ok(FrameRole::Ordinary),
        (FRAME_GROUP_HEADER_BYTES, role @ (1 | 2)) if header.magic == FrameMagic::StreamV2 => {
            let value = u64::from_le_bytes(bytes[15..23].try_into().map_err(|_| FrameError::InvalidRole)?);
            let count = u32::from_le_bytes(bytes[23..27].try_into().map_err(|_| FrameError::InvalidRole)?);
            if count == 0 || (role == 1 && value == 0) {
                return Err(FrameError::InvalidGroup);
            }
            if role == 1 {
                Ok(FrameRole::Head {
                    logical_length: value,
                    frame_count: count,
                })
            } else {
                Ok(FrameRole::Follower {
                    head_offset: value,
                    ordinal: count,
                })
            }
        }
        _ => Err(FrameError::InvalidRole),
    }
}

/// Canonical framing geometry carried by the owning location or verified frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameLayout {
    OrdinaryV2,
    StreamGroupV2,
}

impl FrameLayout {
    #[must_use]
    pub const fn header_bytes(self) -> usize {
        match self {
            Self::OrdinaryV2 => FRAME_V2_HEADER_BYTES,
            Self::StreamGroupV2 => FRAME_GROUP_HEADER_BYTES,
        }
    }

    #[must_use]
    pub const fn payload_capacity(self) -> u64 {
        (MAX_FRAME_BYTES - self.header_bytes() - FRAME_FOOTER_BYTES) as u64
    }

    /// # Errors
    /// Returns an error when the physical length overflows.
    pub fn physical_length(self, logical_length: u64) -> Result<u64, FrameError> {
        let count = logical_length.div_ceil(self.payload_capacity());
        count
            .checked_mul((self.header_bytes() + FRAME_FOOTER_BYTES) as u64)
            .and_then(|overhead| logical_length.checked_add(overhead))
            .ok_or(FrameError::LengthOverflow)
    }

    /// Map a logical subrange to its smallest complete-frame range.
    /// # Errors
    /// Returns an error for an invalid range or arithmetic overflow.
    pub fn physical_range(
        self,
        location: ChunkLocation,
        range: std::ops::Range<u64>,
    ) -> Result<std::ops::Range<u64>, FrameError> {
        if range.start > range.end || range.end > location.logical_length {
            return Err(FrameError::InvalidLocationRange);
        }
        if range.is_empty() {
            return Ok(location.frame_offset..location.frame_offset);
        }
        let first = range.start / self.payload_capacity();
        let last = (range.end - 1) / self.payload_capacity();
        let start = first
            .checked_mul(MAX_FRAME_BYTES as u64)
            .ok_or(FrameError::LengthOverflow)?;
        let end = if last + 1 == location.logical_length.div_ceil(self.payload_capacity()) {
            self.physical_length(location.logical_length)?
        } else {
            (last + 1)
                .checked_mul(MAX_FRAME_BYTES as u64)
                .ok_or(FrameError::LengthOverflow)?
        };
        Ok(location
            .frame_offset
            .checked_add(start)
            .ok_or(FrameError::LengthOverflow)?
            ..location
                .frame_offset
                .checked_add(end)
                .ok_or(FrameError::LengthOverflow)?)
    }
}
