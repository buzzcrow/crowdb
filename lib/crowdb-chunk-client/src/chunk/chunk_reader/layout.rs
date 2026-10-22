// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::KIB;
use crate::{ReadError, ReadResult};
use crowdb_protocol::chunkdb::rpc::Chunk;

pub(super) fn validate_strip_layout(chunk: &Chunk) -> ReadResult<()> {
    let mut previous_end = 0;
    for (index, strip) in chunk.strips.iter().enumerate() {
        let start = u64::from(strip.chunk_offset) * KIB;
        let end = strip_end(strip)?;
        if strip.capacity == 0 || strip.unit_kb == 0 || end <= start || (index > 0 && start < previous_end) {
            return Err(ReadError::InvalidLocations(format!(
                "chunk strip {} has invalid or overlapping geometry",
                strip.strip_sequence
            )));
        }
        previous_end = end;
    }
    Ok(())
}

pub(super) fn strip_end(strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip) -> ReadResult<u64> {
    u64::from(strip.chunk_offset)
        .checked_add(u64::from(strip.capacity))
        .and_then(|kib| kib.checked_mul(KIB))
        .ok_or_else(|| ReadError::InvalidLocations("strip interval overflows".into()))
}

pub(super) fn strip_read_parts(
    strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
    start: u64,
    end: u64,
) -> ReadResult<Vec<(u64, u64)>> {
    let Some(crowdb_protocol::chunkdb::rpc::Strip::EcStrip(ec)) = strip.strip.as_ref() else {
        return Ok(vec![(start, end)]);
    };
    let first = ec
        .segments
        .first()
        .ok_or_else(|| ReadError::InvalidLocations("EC strip has no segments".into()))?;
    let shard_bytes = u64::from(first.unit_count)
        .checked_mul(u64::from(strip.unit_kb))
        .and_then(|value| value.checked_mul(KIB))
        .ok_or_else(|| ReadError::InvalidLocations("EC shard size overflows".into()))?;
    if shard_bytes == 0 {
        return Err(ReadError::InvalidLocations("EC shard size is zero".into()));
    }
    let strip_start = u64::from(strip.chunk_offset) * KIB;
    let mut parts = Vec::new();
    let mut cursor = start;
    while cursor < end {
        let local = cursor - strip_start;
        let next_boundary = strip_start.saturating_add(
            (local / shard_bytes)
                .saturating_add(1)
                .saturating_mul(shard_bytes),
        );
        let part_end = end.min(next_boundary);
        parts.push((cursor, part_end));
        cursor = part_end;
    }
    Ok(parts)
}
