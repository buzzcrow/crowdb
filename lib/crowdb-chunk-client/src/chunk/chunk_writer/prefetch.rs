// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Known-size strip planning and revision-aware metadata append.

use crowdb_protocol::chunkdb::rpc::{AppendChunkRequest, Chunk, Strip, StripType};

use crate::traits::ChunkAllocator;
use crate::{IoError, Result};

/// Number of strips not yet allocated for a known-size object.
pub(super) fn compute_strips_remaining(object_size: Option<u64>, chunk: &Chunk) -> Option<usize> {
    let total = object_size?;
    let strip_data_capacity = u64::from(chunk.strips.first()?.capacity) * 1024;
    let total_strips = total.div_ceil(strip_data_capacity.max(1)) as usize;
    Some(total_strips.saturating_sub(chunk.strips.len()))
}

/// Append strips and merge the incremental response into the local chunk.
/// A stale revision carries the current full chunk; retry once with that
/// revision so concurrent metadata changes do not duplicate an append.
pub(super) async fn append_strips(
    chunkdb: &dyn ChunkAllocator,
    mut chunk: Chunk,
    strip_count: u32,
) -> Result<Chunk> {
    let chunk_id = chunk
        .id
        .ok_or_else(|| IoError::AllocationFailed("append_chunk: chunk missing id".into()))?;
    let unit_count = chunk
        .strips
        .first()
        .and_then(|strip| match strip.strip.as_ref() {
            Some(Strip::EcStrip(ec)) => ec.segments.first(),
            Some(Strip::MirrorStrip(mirror)) => mirror.segments.first(),
            None => None,
        })
        .map(|segment| segment.unit_count)
        .filter(|count| *count > 0)
        .ok_or_else(|| {
            IoError::AllocationFailed("append_chunk: existing strip has no segment geometry".into())
        })?;
    let (strip_type, data_num, code_num, copy_count) =
        match chunk.strips.last().and_then(|strip| strip.strip.as_ref()) {
            Some(Strip::MirrorStrip(mirror)) => (
                StripType::Mirror as i32,
                0,
                0,
                u32::try_from(mirror.segments.len()).unwrap_or(u32::MAX),
            ),
            Some(Strip::EcStrip(ec)) => (StripType::Ec as i32, ec.data_num, ec.code_num, 0),
            None => {
                return Err(IoError::AllocationFailed(
                    "append_chunk: missing strip layout".into(),
                ))
            }
        };
    for attempt in 0..2 {
        let resp = chunkdb
            .append_chunk(AppendChunkRequest {
                chunk_id: Some(chunk_id),
                modify_ts: chunk.modify_ts,
                strip_size: unit_count,
                strip_count,
                strip_type,
                data_num,
                code_num,
                copy_count,
            })
            .await?;
        if let Some(current) = resp.chunk {
            if current.id != Some(chunk_id) {
                return Err(IoError::AllocationFailed(
                    "append_chunk refresh returned a different chunk".into(),
                ));
            }
            chunk = current;
            if attempt == 0 {
                continue;
            }
            return Err(IoError::AllocationFailed(
                "append_chunk revision changed twice".into(),
            ));
        }
        if resp.strips.is_empty() {
            return Err(IoError::AllocationFailed(
                "append_chunk response missing appended strips".into(),
            ));
        }
        chunk.modify_ts = resp.modify_ts;
        chunk.capacity = chunk
            .capacity
            .saturating_add(resp.strips.iter().map(|strip| strip.capacity).sum::<u32>());
        chunk.strips.extend(resp.strips);
        return Ok(chunk);
    }
    unreachable!()
}
