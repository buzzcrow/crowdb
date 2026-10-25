// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;

use super::encoding::{crc32c_parts, write_header_region};
use super::{
    parse_frame, set_frame_chunk_id, ChunkId, FrameError, FrameHeaderPrefix, FrameLayout, FrameMagic,
    FrameRole, ParsedFrame, FRAME_FOOTER_BYTES, FRAME_GROUP_HEADER_BYTES, MAX_FRAME_GROUP_PAYLOAD_BYTES,
};

/// A whole append validated from a head and all its bounded followers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedAppendGroup {
    pub logical_length: u64,
    pub physical_length: u64,
    pub frame_count: u32,
}

/// Encode a Stream append without copying payload bytes. Each group remains a
/// single validity unit even when a `DiskIO` request fragments these ordered views.
/// # Errors
/// Returns an error for empty appends, excessive frame counts, or offset overflow.
pub fn encode_append_group_views(
    chunk_id: ChunkId,
    head_offset: u64,
    payload: &Bytes,
    write_time_ms: u64,
) -> Result<Vec<Bytes>, FrameError> {
    let logical_length = u64::try_from(payload.len()).map_err(|_| FrameError::LengthOverflow)?;
    let count = payload.len().div_ceil(MAX_FRAME_GROUP_PAYLOAD_BYTES);
    let frame_count = u32::try_from(count).map_err(|_| FrameError::LengthOverflow)?;
    if count == 0 {
        return Err(FrameError::InvalidGroup);
    }
    head_offset
        .checked_add(FrameLayout::StreamGroupV2.physical_length(logical_length)?)
        .ok_or(FrameError::LengthOverflow)?;
    let mut views = Vec::with_capacity(count.checked_mul(3).ok_or(FrameError::LengthOverflow)?);
    for (ordinal, start) in (0..payload.len())
        .step_by(MAX_FRAME_GROUP_PAYLOAD_BYTES)
        .enumerate()
    {
        let end = start
            .saturating_add(MAX_FRAME_GROUP_PAYLOAD_BYTES)
            .min(payload.len());
        let part = payload.slice(start..end);
        let mut header = [0; FRAME_GROUP_HEADER_BYTES];
        write_header_region(
            &mut header,
            FrameHeaderPrefix {
                magic: FrameMagic::StreamV2,
                payload_offset: 27,
                payload_size: u16::try_from(part.len()).map_err(|_| FrameError::PayloadTooLarge)?,
                write_time_ms,
            },
        );
        header[14] = if ordinal == 0 { 1 } else { 2 };
        let value = if ordinal == 0 { logical_length } else { head_offset };
        let number = if ordinal == 0 {
            frame_count
        } else {
            u32::try_from(ordinal).map_err(|_| FrameError::LengthOverflow)?
        };
        header[15..23].copy_from_slice(&value.to_le_bytes());
        header[23..27].copy_from_slice(&number.to_le_bytes());
        let mut footer = [0; FRAME_FOOTER_BYTES];
        footer[..4].copy_from_slice(&crc32c_parts([header.as_slice(), part.as_ref()]).to_le_bytes());
        set_frame_chunk_id(chunk_id, &mut footer)?;
        views.extend([
            Bytes::copy_from_slice(&header),
            part,
            Bytes::copy_from_slice(&footer),
        ]);
    }
    Ok(views)
}

/// Bounded state for scans that cannot retain a whole append in memory. Only
/// `finish` grants a complete append boundary; individual valid CRCs do not.
pub struct AppendGroupValidator {
    head_offset: u64,
    next_offset: u64,
    logical_length: u64,
    frame_count: u32,
    observed: u32,
    write_time_ms: u64,
    chunk_id: ChunkId,
}

impl AppendGroupValidator {
    /// # Errors
    /// Returns an error unless the first frame is a canonical Stream group head.
    pub fn new(head_offset: u64, head: &ParsedFrame<'_>) -> Result<Self, FrameError> {
        let FrameRole::Head {
            logical_length,
            frame_count,
        } = head.role
        else {
            return Err(FrameError::InvalidGroup);
        };
        if head.header.magic != FrameMagic::StreamV2
            || u64::from(frame_count) != logical_length.div_ceil(MAX_FRAME_GROUP_PAYLOAD_BYTES as u64)
        {
            return Err(FrameError::InvalidGroup);
        }
        head_offset
            .checked_add(FrameLayout::StreamGroupV2.physical_length(logical_length)?)
            .ok_or(FrameError::LengthOverflow)?;
        let mut validator = Self {
            head_offset,
            next_offset: head_offset,
            logical_length,
            frame_count,
            observed: 0,
            write_time_ms: head.header.write_time_ms,
            chunk_id: head.chunk_id,
        };
        validator.push(head_offset, head)?;
        Ok(validator)
    }

    /// # Errors
    /// Rejects gaps, wrong group identity/order, excess frames and noncanonical lengths.
    pub fn push(&mut self, offset: u64, frame: &ParsedFrame<'_>) -> Result<(), FrameError> {
        let remaining = self
            .logical_length
            .saturating_sub(u64::from(self.observed) * MAX_FRAME_GROUP_PAYLOAD_BYTES as u64);
        let expected_role = if self.observed == 0 {
            FrameRole::Head {
                logical_length: self.logical_length,
                frame_count: self.frame_count,
            }
        } else {
            FrameRole::Follower {
                head_offset: self.head_offset,
                ordinal: self.observed,
            }
        };
        if self.observed >= self.frame_count
            || offset != self.next_offset
            || frame.header.magic != FrameMagic::StreamV2
            || frame.role != expected_role
            || frame.chunk_id != self.chunk_id
            || frame.header.payload_offset as usize != FRAME_GROUP_HEADER_BYTES
            || frame.physical_length != FRAME_GROUP_HEADER_BYTES + frame.payload.len() + FRAME_FOOTER_BYTES
            || frame.header.write_time_ms != self.write_time_ms
            || frame.payload.len() as u64 != remaining.min(MAX_FRAME_GROUP_PAYLOAD_BYTES as u64)
        {
            return Err(FrameError::InvalidGroup);
        }
        self.next_offset = offset
            .checked_add(frame.physical_length as u64)
            .ok_or(FrameError::LengthOverflow)?;
        self.observed += 1;
        Ok(())
    }

    /// # Errors
    /// Returns an error if any declared follower is still missing.
    pub fn finish(self) -> Result<ParsedAppendGroup, FrameError> {
        if self.observed != self.frame_count {
            return Err(FrameError::InvalidGroup);
        }
        Ok(ParsedAppendGroup {
            logical_length: self.logical_length,
            physical_length: self.next_offset - self.head_offset,
            frame_count: self.frame_count,
        })
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.observed == self.frame_count
    }
}

/// Validate exactly one whole append, leaving any following bytes to the caller.
/// # Errors
/// Returns an error for incomplete or inconsistent groups or frame verification failure.
pub fn parse_append_group(
    bytes: &[u8],
    chunk_id: ChunkId,
    head_offset: u64,
) -> Result<ParsedAppendGroup, FrameError> {
    let head = parse_frame(bytes, chunk_id)?;
    let mut cursor = head.physical_length;
    let mut validator = AppendGroupValidator::new(head_offset, &head)?;
    while !validator.is_complete() {
        let frame = parse_frame(&bytes[cursor..], chunk_id)?;
        validator.push(
            head_offset
                .checked_add(cursor as u64)
                .ok_or(FrameError::LengthOverflow)?,
            &frame,
        )?;
        cursor = cursor
            .checked_add(frame.physical_length)
            .ok_or(FrameError::LengthOverflow)?;
    }
    validator.finish()
}
