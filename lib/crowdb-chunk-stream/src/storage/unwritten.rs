// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit unwritten headers keep recycled allocation bytes out of tail authority.

use bytes::Bytes;
use crowdb_protocol::frame::FRAME_HEADER_PREFIX_BYTES;

use super::StreamMirrorWrite;
use crate::{Result, StreamError};

pub(crate) const EMPTY_TAIL: [u8; FRAME_HEADER_PREFIX_BYTES] = [0; FRAME_HEADER_PREFIX_BYTES];

pub(crate) fn padded_end(complete_end: u64, physical_limit: u64) -> Result<u64> {
    let remaining = physical_limit
        .checked_sub(complete_end)
        .ok_or_else(|| StreamError::InvalidRequest("complete append exceeds physical chunk limit".into()))?;
    Ok(complete_end + remaining.min(FRAME_HEADER_PREFIX_BYTES as u64))
}

pub(crate) fn append_tail_view(views: &mut Vec<Bytes>, complete_end: u64, physical_limit: u64) -> Result<()> {
    let length = usize::try_from(padded_end(complete_end, physical_limit)? - complete_end)
        .expect("bounded unwritten header");
    if length > 0 {
        views.push(Bytes::from_static(&EMPTY_TAIL).slice(..length));
    }
    Ok(())
}

pub(crate) fn validate_write_tail(write: StreamMirrorWrite<'_>, physical_limit: u64) -> Result<u64> {
    let length = write
        .images
        .iter()
        .try_fold(0_u64, |sum, image| sum.checked_add(image.data.len() as u64))
        .ok_or_else(|| StreamError::InvalidRequest("stream image length overflows".into()))?;
    let end = write
        .physical_offset
        .checked_add(length)
        .ok_or_else(|| StreamError::InvalidRequest("stream image end overflows".into()))?;
    if write.complete_end <= write.physical_offset || end != padded_end(write.complete_end, physical_limit)? {
        return Err(StreamError::InvalidRequest(
            "stream images omit the required unwritten tail header".into(),
        ));
    }
    let mut remaining = usize::try_from(end - write.complete_end).expect("bounded unwritten header");
    for image in write.images.iter().rev() {
        let take = remaining.min(image.data.len());
        if image.data[image.data.len() - take..]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(StreamError::InvalidRequest(
                "unwritten tail header contains nonzero bytes".into(),
            ));
        }
        remaining -= take;
        if remaining == 0 {
            break;
        }
    }
    Ok(end)
}
