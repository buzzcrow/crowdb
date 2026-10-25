// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Validate replacement coverage before publishing any physical layout.

use super::handler::{extract_segments, LifecycleError};
use crowdb_protocol::{
    chunkdb::rpc::{ChunkStrip, Strip},
    common::ChunkId,
};
use std::collections::HashSet;

pub(super) fn validate_replacement_geometry(
    old: &[ChunkStrip],
    replacement: &[ChunkStrip],
    chunk_id: &ChunkId,
) -> Result<(), LifecycleError> {
    let old_capacity: u32 = old.iter().map(|strip| strip.capacity).sum();
    let new_capacity: u32 = replacement.iter().map(|strip| strip.capacity).sum();
    if old_capacity != new_capacity
        || old[0].chunk_offset != replacement[0].chunk_offset
        || old[0].strip_sequence != replacement[0].strip_sequence
    {
        return Err(LifecycleError::InvalidRequest(
            "replacement must preserve first offset, first sequence, and total capacity".into(),
        ));
    }
    if replacement
        .iter()
        .flat_map(extract_segments)
        .any(|segment| segment.owner_chunk.as_ref() != Some(chunk_id) || segment.unit_count == 0)
    {
        return Err(LifecycleError::InvalidRequest(
            "replacement segments must be non-empty and owned by the chunk".into(),
        ));
    }
    let mut expected_offset = replacement[0].chunk_offset;
    for strip in replacement {
        if strip.capacity == 0 || strip.unit_kb == 0 || strip.chunk_offset != expected_offset {
            return Err(LifecycleError::InvalidRequest(
                "replacement strips must be non-empty and contiguous".into(),
            ));
        }
        expected_offset = expected_offset
            .checked_add(strip.capacity)
            .ok_or_else(|| LifecycleError::InvalidRequest("replacement strip range overflows u32".into()))?;
        let segments = extract_segments(strip);
        validate_physical_ranges(&segments)?;
        let shape_is_valid = match &strip.strip {
            Some(Strip::MirrorStrip(_)) => {
                !segments.is_empty()
                    && segments
                        .iter()
                        .all(|segment| segment.unit_count.saturating_mul(strip.unit_kb) == strip.capacity)
            }
            Some(Strip::EcStrip(ec)) => {
                ec.data_num > 0
                    && ec.code_num > 0
                    && segments.len()
                        == usize::try_from(ec.data_num.saturating_add(ec.code_num)).unwrap_or(usize::MAX)
                    && segments.iter().all(|segment| {
                        segment
                            .unit_count
                            .saturating_mul(strip.unit_kb)
                            .saturating_mul(ec.data_num)
                            == strip.capacity
                    })
            }
            None => false,
        };
        if !shape_is_valid {
            return Err(LifecycleError::InvalidRequest(
                "replacement strip has invalid mirror or EC geometry".into(),
            ));
        }
        let segment_set: HashSet<_> = segments.into_iter().collect();
        if strip
            .unavailable_segments
            .iter()
            .any(|segment| !segment_set.contains(segment))
        {
            return Err(LifecycleError::InvalidRequest(
                "unavailable replicas must belong to the replacement strip".into(),
            ));
        }
    }
    Ok(())
}

fn validate_physical_ranges(
    segments: &[crowdb_protocol::diskdb::rpc::Segment],
) -> Result<(), LifecycleError> {
    let mut ranges = Vec::with_capacity(segments.len());
    for segment in segments {
        let disk = segment.disk_id.ok_or_else(|| {
            LifecycleError::InvalidRequest("replacement segment has no disk identity".into())
        })?;
        let end = segment
            .unit_offset
            .checked_add(u64::from(segment.unit_count))
            .ok_or_else(|| LifecycleError::InvalidRequest("replacement segment range overflows".into()))?;
        ranges.push((disk.high, disk.low, segment.zone_index, segment.unit_offset, end));
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| {
        let (left, right) = (&pair[0], &pair[1]);
        (left.0, left.1, left.2) == (right.0, right.1, right.2) && left.4 > right.3
    }) {
        return Err(LifecycleError::InvalidRequest(
            "replacement copies overlap the same physical allocation".into(),
        ));
    }
    Ok(())
}
