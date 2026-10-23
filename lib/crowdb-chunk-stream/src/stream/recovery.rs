// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Rebuild complete grouped locations beyond the last directory checkpoint.

use super::{Extent, StreamChunkStore};
use crate::{ActiveChunkDescriptor, Result, StreamError};
use crowdb_protocol::frame::{
    FramePrefixScanner, FrameScanConfig, FrameScanMode, FrameScanStop, MAX_FRAME_SCAN_EXTENTS,
    MAX_FRAME_SCAN_INPUT_BYTES,
};

pub(super) async fn recover_active_frames(
    chunks: &dyn StreamChunkStore,
    active: &ActiveChunkDescriptor,
    durable_offset: u64,
    extents: &mut Vec<Extent>,
) -> Result<u64> {
    let mut scanner = FramePrefixScanner::new(FrameScanConfig {
        chunk_id: active.chunk_id,
        mode: FrameScanMode::Grouped,
        physical_start: active.acknowledged_cursor,
        physical_limit: durable_offset,
        required_end: durable_offset,
        logical_start: active.logical_start,
        max_append_bytes: active.capacity,
    })
    .map_err(|error| frame_error(&error))?;
    let mut physical = active.acknowledged_cursor;
    while physical < durable_offset {
        let length = usize::try_from((durable_offset - physical).min(MAX_FRAME_SCAN_INPUT_BYTES as u64))
            .expect("bounded read window");
        let bytes = chunks.read(active.chunk_id, physical, length).await?;
        if bytes.len() != length {
            return Err(StreamError::ReadUnavailable("short grouped recovery read".into()));
        }
        let mut consumed = 0;
        while consumed < bytes.len() && scanner.stop() == FrameScanStop::Running {
            let batch = scanner
                .push(
                    physical + consumed as u64,
                    &bytes[consumed..],
                    MAX_FRAME_SCAN_EXTENTS,
                )
                .map_err(|error| frame_error(&error))?;
            consumed += batch.consumed;
            for extent in batch.extents {
                extents.push(Extent {
                    chunk_id: active.chunk_id,
                    logical_start: extent.logical_offset,
                    logical_end: extent.logical_offset + extent.logical_length,
                    frame_start: extent.physical_offset,
                    frame_length: u32::try_from(extent.physical_length)
                        .map_err(|_| StreamError::Corruption("grouped location exceeds u32".into()))?,
                });
            }
        }
        if consumed < bytes.len() {
            return Err(StreamError::Corruption(
                "grouped recovery stopped before the required checkpoint".into(),
            ));
        }
        physical += consumed as u64;
    }
    Ok(scanner.finish().map_err(|error| frame_error(&error))?.logical_end)
}

fn frame_error(error: &crowdb_protocol::frame::FrameScanError) -> StreamError {
    StreamError::Corruption(format!("invalid recovered stream group: {error}"))
}
