// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Public self-validating chunk frame encoding and location arithmetic.

use std::ops::Range;

use super::{
    ChunkId, FrameError, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES, MAX_FRAME_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
};

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

/// Return the physical bytes required by the consecutive canonical frames for
/// one logical payload sequence.
///
/// # Errors
///
/// Returns an error when the result overflows.
pub fn framed_physical_length(logical_length: u64) -> Result<u64, FrameError> {
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
