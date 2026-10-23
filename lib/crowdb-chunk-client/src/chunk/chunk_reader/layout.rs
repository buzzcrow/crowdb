// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Layout geometry and failure observations.

use super::KIB;
use crate::{ReadError, ReadResult};
use crowdb_protocol::chunkdb::rpc::{Chunk, Location};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;

pub(super) struct StripFailureObservation {
    pub(super) strip_sequence: u32,
    pub(super) failed_segments: Vec<Segment>,
    pub(super) served_segments: Vec<Segment>,
}

pub(super) fn mark_served_segments_corrupt(
    mut observations: Vec<StripFailureObservation>,
) -> Vec<StripFailureObservation> {
    let serving_count: usize = observations
        .iter()
        .map(|observation| observation.served_segments.len())
        .sum();
    if serving_count != 1 {
        return Vec::new();
    }
    for observation in &mut observations {
        for segment in std::mem::take(&mut observation.served_segments) {
            if !observation.failed_segments.contains(&segment) {
                observation.failed_segments.push(segment);
            }
        }
    }
    observations.retain(|observation| !observation.failed_segments.is_empty());
    observations
}

pub(super) fn segment_identity(segment: &Segment) -> (u64, u64, u32, u64, u64) {
    let disk = segment.disk_id.unwrap_or_default();
    (
        disk.high,
        disk.low,
        segment.zone_index,
        segment.unit_offset,
        segment.allocation_ts,
    )
}

pub(super) fn failure_operation_id(
    chunk_id: ChunkId,
    strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip,
) -> ChunkId {
    let mut high = chunk_id.high ^ 0x111f_a11e_d000_0001;
    let mut low = chunk_id.low ^ u64::from(strip.strip_sequence);
    for segment in &strip.unavailable_segments {
        let disk = segment.disk_id.unwrap_or_default();
        high = high.rotate_left(13) ^ disk.high ^ segment.unit_offset;
        low = low.rotate_left(17) ^ disk.low ^ segment.allocation_ts;
    }
    ChunkId { high, low }
}

pub(super) fn normalize_locations(locations: &[Location]) -> ReadResult<(Vec<Location>, u64)> {
    let mut locations: Vec<_> = locations
        .iter()
        .filter(|location| location.logical_length != 0 && location.length != 0)
        .cloned()
        .collect();
    locations.sort_unstable_by_key(|location| location.logical_offset);
    let mut cursor = 0u64;
    for location in &locations {
        if location.chunk_id.is_none() || location.logical_length > location.length {
            return Err(ReadError::InvalidLocations(
                "location is missing a chunk or logical length exceeds physical length".into(),
            ));
        }
        if location.logical_offset != cursor {
            return Err(ReadError::InvalidLocations(format!(
                "logical locations are not contiguous at byte {cursor}"
            )));
        }
        location
            .offset
            .checked_add(location.length)
            .ok_or_else(|| ReadError::InvalidLocations("physical location overflows".into()))?;
        cursor = cursor
            .checked_add(location.logical_length)
            .ok_or_else(|| ReadError::InvalidLocations("object length overflows".into()))?;
    }
    Ok((locations, cursor))
}

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
