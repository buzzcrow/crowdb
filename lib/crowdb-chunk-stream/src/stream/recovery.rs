// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Rebuild mappings for durable frames beyond the last published directory.

use super::{Extent, StreamChunkStore};
use crate::{ActiveChunkDescriptor, Result, StreamError};
use crowdb_protocol::frame::{
    frame_length, parse_frame, parse_header, FrameMagic, FRAME_HEADER_PREFIX_BYTES,
};

pub(super) async fn recover_active_frames(
    chunks: &dyn StreamChunkStore,
    active: &ActiveChunkDescriptor,
    durable_offset: u64,
    extents: &mut Vec<Extent>,
) -> Result<u64> {
    let mut physical = active.acknowledged_cursor;
    let mut logical = active.logical_start;
    while physical < durable_offset {
        if durable_offset - physical < FRAME_HEADER_PREFIX_BYTES as u64 {
            return Err(StreamError::Corruption(
                "durable stream tail ends inside a frame header".into(),
            ));
        }
        let prefix = chunks
            .read(active.chunk_id, physical, FRAME_HEADER_PREFIX_BYTES)
            .await?;
        let header = parse_header(&prefix).map_err(|error| frame_error(&error))?;
        let length = frame_length(header).map_err(|error| frame_error(&error))?;
        if length as u64 > durable_offset - physical {
            return Err(StreamError::Corruption(
                "durable stream tail ends inside a frame".into(),
            ));
        }
        let frame = chunks
            .read_verified_frame(active.chunk_id, physical, length)
            .await?;
        let parsed = parse_frame(&frame, active.chunk_id).map_err(|error| frame_error(&error))?;
        if parsed.header.magic != FrameMagic::StreamV1 {
            return Err(StreamError::Corruption(
                "durable stream tail has the wrong frame kind".into(),
            ));
        }
        if parsed.payload.is_empty() {
            return Err(StreamError::Corruption(
                "recovered stream frame has an empty payload".into(),
            ));
        }
        let end = logical
            .checked_add(parsed.payload.len() as u64)
            .ok_or_else(|| StreamError::Corruption("recovered logical stream tail overflows".into()))?;
        extents.push(Extent {
            chunk_id: active.chunk_id,
            logical_start: logical,
            logical_end: end,
            frame_start: physical,
            frame_length: u32::try_from(length)
                .map_err(|_| frame_error(&crowdb_protocol::frame::FrameError::FrameTooLarge))?,
        });
        physical += length as u64;
        logical = end;
    }
    Ok(logical)
}

fn frame_error(error: &crowdb_protocol::frame::FrameError) -> StreamError {
    StreamError::Corruption(format!("invalid recovered stream frame: {error}"))
}
