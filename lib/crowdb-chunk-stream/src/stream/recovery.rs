// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Rebuild complete grouped locations beyond the last directory checkpoint.

use super::Extent;
use crate::{Result, StreamError};

pub(super) fn append_recovered(
    chunk_id: crate::ChunkId,
    recovered: Vec<crowdb_protocol::frame::ScannedFrameExtent>,
    extents: &mut Vec<Extent>,
) -> Result<()> {
    for extent in recovered {
        extents.push(Extent {
            chunk_id,
            logical_start: extent.logical_offset,
            logical_end: extent.logical_offset + extent.logical_length,
            frame_start: extent.physical_offset,
            frame_length: u32::try_from(extent.physical_length)
                .map_err(|_| StreamError::Corruption("recovered group exceeds u32".into()))?,
        });
    }
    Ok(())
}
