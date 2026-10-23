// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Admission and physical progress are separate from persisted layout checkpoints.

use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, Strip};
use crowdb_protocol::frame::{parse_header, FrameMagic};
use std::sync::atomic::Ordering;

use super::ChunkStateView;
use crate::{Result, StreamError, StreamMirrorWrite};

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
    write: &StreamMirrorWrite<'_>,
) -> Result<u64> {
    if chunk.writer_epoch != writer_epoch
        || state.sealed.load(Ordering::Acquire)
        || write_cursor(state) != write.physical_offset
    {
        return Err(StreamError::StaleWriter);
    }
    let length = write
        .images
        .iter()
        .try_fold(0_u64, |sum, image| sum.checked_add(image.data.len() as u64))
        .ok_or_else(|| StreamError::InvalidRequest("stream write length overflows".into()))?;
    let data_end = write
        .physical_offset
        .checked_add(length)
        .ok_or_else(|| StreamError::InvalidRequest("stream write cursor overflows".into()))?;
    if length == 0 || data_end != write.complete_end {
        return Err(StreamError::InvalidRequest(
            "stream images do not end at the complete append boundary".into(),
        ));
    }
    let mut prefix = [0; 15];
    let mut copied = 0;
    for image in write.images {
        let count = (prefix.len() - copied).min(image.data.len());
        prefix[copied..copied + count].copy_from_slice(&image.data[..count]);
        copied += count;
        if copied == prefix.len() {
            break;
        }
    }
    let header =
        parse_header(&prefix[..copied]).map_err(|error| StreamError::InvalidRequest(error.to_string()))?;
    if header.magic != FrameMagic::StreamV2 || copied < 15 || prefix[14] != 1 {
        return Err(StreamError::InvalidRequest(
            "stream writes require complete append groups".into(),
        ));
    }
    if !state.writer_started.load(Ordering::Acquire)
        && (write.physical_offset != 0
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
