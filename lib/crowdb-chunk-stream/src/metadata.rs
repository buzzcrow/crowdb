// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{FrameLayout, FRAME_FOOTER_BYTES, FRAME_GROUP_HEADER_BYTES, MAX_FRAME_BYTES};

use crate::{Result, StreamError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtentLocation {
    pub chunk_id: ChunkId,
    pub frame_offset: u64,
    pub frame_length: u32,
    pub payload_offset: u32,
    pub available: u64,
    pub group_offset: u64,
    pub group_length: u64,
    pub ordinal: u32,
}

pub(crate) fn resolve_group(
    chunk_id: ChunkId,
    logical_start: u64,
    logical_end: u64,
    group_offset: u64,
    logical_offset: u64,
) -> Result<ExtentLocation> {
    if !(logical_start..logical_end).contains(&logical_offset) {
        return Err(StreamError::Corruption(
            "logical offset is outside append group".into(),
        ));
    }
    let delta = logical_offset - logical_start;
    let capacity = FrameLayout::StreamGroupV2.payload_capacity();
    let ordinal = delta / capacity;
    let frame_logical_start = ordinal * capacity;
    let payload_length = (logical_end - logical_start - frame_logical_start).min(capacity);
    let within = delta % capacity;
    let frame_offset = group_offset
        .checked_add(ordinal * MAX_FRAME_BYTES as u64)
        .ok_or_else(|| StreamError::Corruption("stream frame offset overflows".into()))?;
    let payload_offset = u32::try_from(FRAME_GROUP_HEADER_BYTES as u64 + within)
        .map_err(|_| StreamError::Corruption("physical extent payload offset overflows".into()))?;
    Ok(ExtentLocation {
        chunk_id,
        frame_offset,
        frame_length: u32::try_from(payload_length + (FRAME_GROUP_HEADER_BYTES + FRAME_FOOTER_BYTES) as u64)
            .map_err(|_| StreamError::Corruption("stream frame length exceeds u32".into()))?,
        payload_offset,
        available: payload_length - within,
        group_offset,
        group_length: logical_end - logical_start,
        ordinal: u32::try_from(ordinal)
            .map_err(|_| StreamError::Corruption("stream frame ordinal exceeds u32".into()))?,
    })
}
