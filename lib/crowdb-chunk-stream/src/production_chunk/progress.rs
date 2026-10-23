// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Admission and physical progress are separate from persisted layout checkpoints.

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, Strip};
use crowdb_protocol::frame::{parse_header, FrameMagic};
use std::sync::atomic::Ordering;

use super::ChunkStateView;
use crate::{Result, StreamError};

pub(super) fn ensure_mirror_copies(strip: &ChunkStrip, required: u32) -> Result<()> {
    let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
        return Err(StreamError::Corruption(
            "stream chunk strip is not mirrored".into(),
        ));
    };
    if mirror.segments.len() != required as usize {
        return Err(StreamError::ReadUnavailable(format!(
            "stream strip requires {required} synchronized replicas; layout has {}",
            mirror.segments.len()
        )));
    }
    Ok(())
}

pub(super) fn write_cursor(state: &ChunkStateView) -> u64 {
    state.synchronized_cursor.load(Ordering::Acquire)
}

pub(super) fn admit_write(
    state: &ChunkStateView,
    chunk: &Chunk,
    writer_epoch: u64,
    physical_offset: u64,
    data: &Bytes,
) -> Result<u64> {
    if chunk.writer_epoch != writer_epoch
        || state.sealed.load(Ordering::Acquire)
        || write_cursor(state) != physical_offset
    {
        return Err(StreamError::StaleWriter);
    }
    let data_end = physical_offset
        .checked_add(data.len() as u64)
        .ok_or_else(|| StreamError::InvalidRequest("stream write cursor overflows".into()))?;
    let header = parse_header(data).map_err(|error| StreamError::InvalidRequest(error.to_string()))?;
    if header.magic != FrameMagic::StreamV2 || data.get(14) != Some(&1) {
        return Err(StreamError::InvalidRequest(
            "stream writes require complete append groups".into(),
        ));
    }
    if !state.writer_started.load(Ordering::Acquire)
        && (physical_offset != 0
            || state.synchronized_cursor.load(Ordering::Acquire) != 0
            || state
                .fresh_grouped_allocation
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_err())
    {
        return Err(StreamError::InvalidRequest(
            "first grouped write requires this owner's fresh allocation".into(),
        ));
    }
    Ok(data_end)
}
