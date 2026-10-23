// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{StreamExtentPage, StreamManifest};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{FrameLayout, FRAME_FOOTER_BYTES, FRAME_GROUP_HEADER_BYTES, MAX_FRAME_BYTES};

use crate::{Result, StreamError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtentLocation {
    pub chunk_id: ChunkId,
    pub frame_offset: u64,
    pub frame_length: u32,
    pub payload_offset: u32,
    pub available: u64,
    pub group_offset: u64,
    pub group_length: u64,
    pub ordinal: u32,
}

/// Validates a complete manifest generation and returns its durable tail.
///
/// # Errors
///
/// Returns [`StreamError::Corruption`] for inconsistent identities, fences,
/// arrays, coverage, cursors, or arithmetic overflow.
pub fn validate_manifest(manifest: &StreamManifest, pages: &[StreamExtentPage]) -> Result<u64> {
    if manifest
        .active
        .as_ref()
        .is_some_and(|active| !manifest.purpose.matches(active.chunk_id))
        || pages
            .iter()
            .flat_map(|page| &page.chunk_ids)
            .any(|id| !manifest.purpose.matches(*id))
    {
        return Err(StreamError::Corruption(
            "stream manifest references a different chunk purpose".into(),
        ));
    }
    if manifest.trim_offset > manifest.sealed_tail {
        return Err(StreamError::Corruption("trim offset exceeds sealed tail".into()));
    }
    if manifest.extent_pages.len() != pages.len() {
        return Err(StreamError::Corruption(
            "extent page directory is incomplete".into(),
        ));
    }

    let mut expected = manifest
        .extent_pages
        .first()
        .map_or(manifest.sealed_tail, |fence| fence.first_logical);
    if expected > manifest.trim_offset {
        return Err(StreamError::Corruption(
            "sealed extent coverage begins after the trim point".into(),
        ));
    }
    for (fence, page) in manifest.extent_pages.iter().zip(pages) {
        validate_extent_page(page)?;
        let first = *page
            .logical_offsets
            .first()
            .ok_or_else(|| StreamError::Corruption("extent page has no first offset".into()))?;
        let end = *page
            .logical_offsets
            .last()
            .ok_or_else(|| StreamError::Corruption("extent page has no end offset".into()))?;
        if fence.page_index != page.page_index
            || fence.first_logical != first
            || fence.end_logical != end
            || first != expected
            || page.stream_name != manifest.stream_name
            || page.writer_epoch != manifest.writer_epoch
            || page.generation != manifest.generation
        {
            return Err(StreamError::Corruption(
                "extent page fence or identity mismatch".into(),
            ));
        }
        expected = end;
    }
    if expected != manifest.sealed_tail {
        return Err(StreamError::Corruption(
            "sealed extents do not cover sealed tail".into(),
        ));
    }

    let tail = if let Some(active) = &manifest.active {
        if active.logical_start != manifest.sealed_tail
            || active.acknowledged_cursor < active.physical_start
            || active.acknowledged_cursor > active.capacity
        {
            return Err(StreamError::Corruption(
                "active chunk descriptor is invalid".into(),
            ));
        }
        active
            .logical_start
            .checked_add(active.acknowledged_cursor - active.physical_start)
            .ok_or_else(|| StreamError::Corruption("active logical tail overflows".into()))?
    } else {
        manifest.sealed_tail
    };
    if manifest.trim_offset > tail {
        return Err(StreamError::Corruption("trim offset exceeds durable tail".into()));
    }
    Ok(tail)
}

/// Resolves one logical offset within a validated extent page.
///
/// # Errors
///
/// Returns an error if the page is malformed, the offset is outside it, or
/// physical address arithmetic overflows.
pub fn resolve_extent(page: &StreamExtentPage, logical_offset: u64) -> Result<ExtentLocation> {
    validate_extent_page(page)?;
    resolve_validated_extent(page, logical_offset)
}

/// The immutable page must have passed validation before entering the read cache.
pub(crate) fn resolve_validated_extent(
    page: &StreamExtentPage,
    logical_offset: u64,
) -> Result<ExtentLocation> {
    let index = page
        .logical_offsets
        .partition_point(|offset| *offset <= logical_offset);
    if index == 0 || index >= page.logical_offsets.len() {
        return Err(StreamError::InvalidRequest(
            "logical offset is outside extent page".into(),
        ));
    }
    let extent = index - 1;
    let logical_start = page.logical_offsets[extent];
    let logical_end = page.logical_offsets[extent + 1];
    if logical_offset >= logical_end {
        return Err(StreamError::InvalidRequest(
            "logical offset is outside extent page".into(),
        ));
    }
    resolve_group(
        page.chunk_ids[extent],
        logical_start,
        logical_end,
        page.physical_offsets[extent],
        logical_offset,
    )
}

pub(crate) fn resolve_group(
    chunk_id: ChunkId,
    logical_start: u64,
    logical_end: u64,
    group_offset: u64,
    logical_offset: u64,
) -> Result<ExtentLocation> {
    if !(logical_start..logical_end).contains(&logical_offset) {
        return Err(StreamError::Corruption(
            "logical offset is outside append group".into(),
        ));
    }
    let delta = logical_offset - logical_start;
    let capacity = FrameLayout::StreamGroupV2.payload_capacity();
    let ordinal = delta / capacity;
    let frame_logical_start = ordinal * capacity;
    let payload_length = (logical_end - logical_start - frame_logical_start).min(capacity);
    let within = delta % capacity;
    let frame_offset = group_offset
        .checked_add(ordinal * MAX_FRAME_BYTES as u64)
        .ok_or_else(|| StreamError::Corruption("stream frame offset overflows".into()))?;
    let payload_offset = u32::try_from(FRAME_GROUP_HEADER_BYTES as u64 + within)
        .map_err(|_| StreamError::Corruption("physical extent payload offset overflows".into()))?;
    Ok(ExtentLocation {
        chunk_id,
        frame_offset,
        frame_length: u32::try_from(payload_length + (FRAME_GROUP_HEADER_BYTES + FRAME_FOOTER_BYTES) as u64)
            .map_err(|_| StreamError::Corruption("stream frame length exceeds u32".into()))?,
        payload_offset,
        available: payload_length - within,
        group_offset,
        group_length: logical_end - logical_start,
        ordinal: u32::try_from(ordinal)
            .map_err(|_| StreamError::Corruption("stream frame ordinal exceeds u32".into()))?,
    })
}

pub(crate) fn validate_extent_page(page: &StreamExtentPage) -> Result<()> {
    let count = page.chunk_ids.len();
    if count == 0
        || page.physical_offsets.len() != count
        || page.frame_lengths.len() != count
        || page.logical_offsets.len() != count + 1
    {
        return Err(StreamError::Corruption(
            "extent arrays have invalid lengths".into(),
        ));
    }
    for offsets in page.logical_offsets.windows(2) {
        if offsets[0] >= offsets[1] {
            return Err(StreamError::Corruption(
                "logical extents are empty, overlapping, or unordered".into(),
            ));
        }
    }
    for index in 0..count {
        let expected = FrameLayout::StreamGroupV2
            .physical_length(page.logical_offsets[index + 1] - page.logical_offsets[index])
            .map_err(|error| StreamError::Corruption(error.to_string()))?;
        if expected != u64::from(page.frame_lengths[index]) {
            return Err(StreamError::Corruption(
                "append group geometry differs from location".into(),
            ));
        }
        page.physical_offsets[index]
            .checked_add(u64::from(page.frame_lengths[index]))
            .ok_or_else(|| StreamError::Corruption("physical extent end overflows".into()))?;
    }
    Ok(())
}
