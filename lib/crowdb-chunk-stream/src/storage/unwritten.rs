// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reserve an unwritten header inside initialized physical capacity.
use crowdb_protocol::frame::FRAME_HEADER_PREFIX_BYTES;

use super::StreamMirrorWrite;
use crate::{Result, StreamError};

pub(crate) fn padded_end(complete_end: u64, physical_limit: u64) -> Result<u64> {
    let remaining = physical_limit
        .checked_sub(complete_end)
        .ok_or_else(|| StreamError::InvalidRequest("complete append exceeds physical chunk limit".into()))?;
    Ok(complete_end + remaining.min(FRAME_HEADER_PREFIX_BYTES as u64))
}

pub(crate) fn validate_write_bounds(write: StreamMirrorWrite<'_>, physical_limit: u64) -> Result<u64> {
    let length = write
        .images
        .iter()
        .try_fold(0_u64, |sum, image| sum.checked_add(image.data.len() as u64))
        .ok_or_else(|| StreamError::InvalidRequest("stream image length overflows".into()))?;
    let end = write
        .physical_offset
        .checked_add(length)
        .ok_or_else(|| StreamError::InvalidRequest("stream image end overflows".into()))?;
    if write.complete_end <= write.physical_offset || end != write.complete_end || end > physical_limit {
        return Err(StreamError::InvalidRequest(
            "stream images must cover only their complete append range".into(),
        ));
    }
    Ok(end)
}
