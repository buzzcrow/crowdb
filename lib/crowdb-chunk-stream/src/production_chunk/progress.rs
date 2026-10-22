// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Admission and physical progress are separate from persisted layout checkpoints.

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::Chunk;
use crowdb_protocol::frame::{parse_header, FrameMagic};
use std::sync::atomic::Ordering;

use super::ChunkStateView;
use crate::{Result, StreamError};

pub(super) fn write_cursor(state: &ChunkStateView) -> u64 {
    if state.grouped_progress.load(Ordering::Acquire) {
        state.synchronized_cursor.load(Ordering::Acquire)
    } else {
        state.persisted_cursor.load(Ordering::Acquire)
    }
}

pub(super) fn admit_write(
    state: &ChunkStateView,
    chunk: &Chunk,
    writer_epoch: u64,
    physical_offset: u64,
    data: &Bytes,
) -> Result<(bool, u64)> {
    if chunk.writer_epoch != writer_epoch
        || state.sealed.load(Ordering::Acquire)
        || write_cursor(state) != physical_offset
    {
        return Err(StreamError::StaleWriter);
    }
    let data_end = physical_offset
        .checked_add(data.len() as u64)
        .ok_or_else(|| StreamError::InvalidRequest("stream write cursor overflows".into()))?;
    let grouped = parse_header(data).is_ok_and(|header| header.magic == FrameMagic::StreamV2);
    let grouped_progress = state.grouped_progress.load(Ordering::Acquire);
    if grouped_progress && !grouped {
        return Err(StreamError::InvalidRequest(
            "grouped stream writes require one frame revision".into(),
        ));
    }
    if grouped && !grouped_progress {
        if physical_offset != 0
            || state.synchronized_cursor.load(Ordering::Acquire) != 0
            || state
                .fresh_grouped_allocation
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(StreamError::InvalidRequest(
                "first grouped write requires this owner's fresh allocation".into(),
            ));
        }
    } else if !grouped {
        state.fresh_grouped_allocation.store(false, Ordering::Release);
    }
    Ok((grouped, data_end))
}
