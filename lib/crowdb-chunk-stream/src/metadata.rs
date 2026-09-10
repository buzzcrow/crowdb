// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{StreamExtentPage, StreamManifest};
use crowdb_protocol::common::ChunkId;

use crate::{Result, StreamError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtentLocation {
    pub chunk_id: ChunkId,
    pub physical_offset: u64,
    pub available: u64,
}

pub fn validate_manifest(manifest: &StreamManifest, pages: &[StreamExtentPage]) -> Result<u64> {
    if manifest.trim_offset > manifest.sealed_tail {
        return Err(StreamError::Corruption("trim offset exceeds sealed tail".into()));
    }
    if manifest.extent_pages.len() != pages.len() {
        return Err(StreamError::Corruption(
            "extent page directory is incomplete".into(),
        ));
    }

    let mut expected = 0;
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

pub fn resolve_extent(page: &StreamExtentPage, logical_offset: u64) -> Result<ExtentLocation> {
    validate_extent_page(page)?;
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
    let delta = logical_offset - logical_start;
    let physical_offset = page.physical_offsets[extent]
        .checked_add(delta)
        .ok_or_else(|| StreamError::Corruption("physical extent offset overflows".into()))?;
    Ok(ExtentLocation {
        chunk_id: page.chunk_ids[extent],
        physical_offset,
        available: logical_end - logical_offset,
    })
}

fn validate_extent_page(page: &StreamExtentPage) -> Result<()> {
    let count = page.chunk_ids.len();
    if count == 0 || page.physical_offsets.len() != count || page.logical_offsets.len() != count + 1 {
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
        let length = page.logical_offsets[index + 1] - page.logical_offsets[index];
        page.physical_offsets[index]
            .checked_add(length)
            .ok_or_else(|| StreamError::Corruption("physical extent end overflows".into()))?;
    }
    Ok(())
}
