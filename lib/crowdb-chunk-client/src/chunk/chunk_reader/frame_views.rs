// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Physical payload views preserving allocation ownership.

use super::ReadRangeData;
use crate::{ReadError, ReadResult};
use crowdb_rpc_ffi::OwnedPayload;
use std::ops::Range;

pub(super) fn extract_physical_range(
    ranges: &[ReadRangeData],
    wanted: Range<u64>,
) -> ReadResult<OwnedPayload> {
    let first = ranges.partition_point(|range| range.end <= wanted.start);
    if let Some(range) = ranges.get(first) {
        if range.start <= wanted.start && range.end >= wanted.end {
            let offset = usize::try_from(wanted.start - range.start)
                .map_err(|_| ReadError::InvalidLocations("physical frame offset overflows".into()))?;
            let length = usize::try_from(wanted.end - wanted.start)
                .map_err(|_| ReadError::InvalidLocations("physical frame length overflows".into()))?;
            return Ok(range.data.slice(offset..offset + length));
        }
    }
    let mut output = Vec::with_capacity(usize::try_from(wanted.end - wanted.start).unwrap_or(usize::MAX));
    let mut cursor = wanted.start;
    for range in &ranges[first..] {
        if range.end <= cursor || range.start >= wanted.end {
            continue;
        }
        let start = cursor.max(range.start);
        if start != cursor {
            return Err(ReadError::InvalidLocations(
                "physical frame data has a gap".into(),
            ));
        }
        let end = wanted.end.min(range.end);
        let offset = usize::try_from(start - range.start)
            .map_err(|_| ReadError::InvalidLocations("physical frame offset overflows".into()))?;
        let length = usize::try_from(end - start)
            .map_err(|_| ReadError::InvalidLocations("physical frame length overflows".into()))?;
        output.extend_from_slice(&range.data[offset..offset + length]);
        cursor = end;
        if cursor == wanted.end {
            break;
        }
    }
    if cursor != wanted.end {
        return Err(ReadError::InvalidLocations(
            "physical frame data is incomplete".into(),
        ));
    }
    Ok(OwnedPayload::from(output))
}

pub(super) fn extract_physical_views(
    ranges: &[ReadRangeData],
    wanted: Range<u64>,
) -> ReadResult<Vec<OwnedPayload>> {
    let first = ranges.partition_point(|range| range.end <= wanted.start);
    let mut output = Vec::new();
    let mut cursor = wanted.start;
    for range in &ranges[first..] {
        if range.start >= wanted.end {
            break;
        }
        if range.end <= cursor {
            continue;
        }
        if range.start > cursor {
            return Err(ReadError::InvalidLocations(
                "physical frame data has a gap".into(),
            ));
        }
        let end = wanted.end.min(range.end);
        let offset = usize::try_from(cursor - range.start)
            .map_err(|_| ReadError::InvalidLocations("physical frame offset overflows".into()))?;
        let length = usize::try_from(end - cursor)
            .map_err(|_| ReadError::InvalidLocations("physical frame length overflows".into()))?;
        output.push(range.data.slice(offset..offset + length));
        cursor = end;
        if cursor == wanted.end {
            break;
        }
    }
    if cursor != wanted.end {
        return Err(ReadError::InvalidLocations(
            "physical frame data is incomplete".into(),
        ));
    }
    Ok(output)
}

pub(super) fn slice_views(views: &[OwnedPayload], wanted: Range<usize>) -> ReadResult<Vec<OwnedPayload>> {
    let mut output = Vec::new();
    let mut cursor = 0_usize;
    let mut covered = wanted.start;
    for view in views {
        let view_end = cursor.saturating_add(view.len());
        if view_end > covered && cursor < wanted.end {
            let start = covered.saturating_sub(cursor);
            let end = (wanted.end - cursor).min(view.len());
            output.push(view.slice(start..end));
            covered += end - start;
        }
        cursor = view_end;
        if covered == wanted.end {
            break;
        }
    }
    if covered != wanted.end {
        return Err(ReadError::InvalidLocations(
            "frame payload views are incomplete".into(),
        ));
    }
    Ok(output)
}
