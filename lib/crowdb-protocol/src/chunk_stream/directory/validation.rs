// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashSet;

use super::{
    StreamDirectoryError, StreamDirectoryHead, StreamDirectoryPage, StreamDirectoryRef, StreamExtentLocation,
    MAX_STREAM_CHUNK_PHYSICAL_BYTES, MAX_STREAM_DIRECTORY_CHILDREN, MAX_STREAM_DIRECTORY_HEIGHT,
    MAX_STREAM_DIRECTORY_LOCATIONS,
};

pub(super) struct PageBounds {
    pub level: u8,
    pub first_logical: u64,
    pub end_logical: u64,
    pub extent_count: u64,
}

impl StreamExtentLocation {
    /// # Errors
    /// Rejects empty or overflowing logical intervals.
    pub fn end_logical(self) -> Result<u64, StreamDirectoryError> {
        if self.location.logical_length == 0 {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        self.logical_start
            .checked_add(self.location.logical_length)
            .ok_or(StreamDirectoryError::InvalidBounds)
    }

    /// # Errors
    /// Rejects overflowing physical intervals or locations outside the chunk limit.
    pub fn end_physical(self) -> Result<u64, StreamDirectoryError> {
        let length = self
            .format
            .layout()
            .physical_length(self.location.logical_length)
            .map_err(|_| StreamDirectoryError::InvalidBounds)?;
        self.location
            .frame_offset
            .checked_add(length)
            .filter(|end| *end <= MAX_STREAM_CHUNK_PHYSICAL_BYTES)
            .ok_or(StreamDirectoryError::InvalidBounds)
    }
}

impl StreamDirectoryRef {
    /// # Errors
    /// Rejects missing identity, impossible height/count or empty logical fences.
    pub fn validate(self) -> Result<(), StreamDirectoryError> {
        if self.id.0 == [0; 32]
            || self.level > MAX_STREAM_DIRECTORY_HEIGHT
            || self.first_logical >= self.end_logical
            || self.extent_count == 0
            || self.extent_count > self.end_logical - self.first_logical
            || (self.level == 0 && self.extent_count > MAX_STREAM_DIRECTORY_LOCATIONS as u64)
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        Ok(())
    }
}

impl StreamDirectoryPage {
    #[must_use]
    pub fn stream_name(&self) -> super::StreamName {
        match self {
            Self::Leaf(page) => page.stream_name,
            Self::Branch(page) => page.stream_name,
        }
    }

    #[must_use]
    pub fn purpose(&self) -> super::StreamPurpose {
        match self {
            Self::Leaf(page) => page.purpose,
            Self::Branch(page) => page.purpose,
        }
    }

    pub(super) fn bounds(&self) -> Result<PageBounds, StreamDirectoryError> {
        match self {
            Self::Leaf(page) => leaf_bounds(page),
            Self::Branch(page) => branch_bounds(page),
        }
    }
}

fn leaf_bounds(page: &super::StreamDirectoryLeaf) -> Result<PageBounds, StreamDirectoryError> {
    if page.locations.is_empty() || page.locations.len() > MAX_STREAM_DIRECTORY_LOCATIONS {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    let first = page.locations[0].logical_start;
    let mut end = first;
    let mut previous = None;
    let mut chunks = HashSet::new();
    for extent in &page.locations {
        if !page.purpose.matches(extent.location.chunk_id) {
            return Err(StreamDirectoryError::WrongPurpose);
        }
        if extent.logical_start != end {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        end = extent.end_logical()?;
        let physical_end = extent.end_physical()?;
        if let Some((chunk, physical, format)) = previous {
            if chunk == extent.location.chunk_id {
                if extent.location.frame_offset != physical || extent.format != format {
                    return Err(StreamDirectoryError::InvalidBounds);
                }
            } else if extent.location.frame_offset != 0 || chunks.contains(&extent.location.chunk_id) {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        chunks.insert(extent.location.chunk_id);
        previous = Some((extent.location.chunk_id, physical_end, extent.format));
    }
    Ok(PageBounds {
        level: 0,
        first_logical: first,
        end_logical: end,
        extent_count: page.locations.len() as u64,
    })
}

fn branch_bounds(page: &super::StreamDirectoryBranch) -> Result<PageBounds, StreamDirectoryError> {
    if page.children.is_empty()
        || page.children.len() > MAX_STREAM_DIRECTORY_CHILDREN
        || page.level == 0
        || page.level > MAX_STREAM_DIRECTORY_HEIGHT
    {
        return Err(StreamDirectoryError::InvalidBounds);
    }
    let first = page.children[0].first_logical;
    let mut end = first;
    let mut count = 0_u64;
    let mut ids = HashSet::new();
    for child in &page.children {
        child.validate()?;
        if child.level + 1 != page.level || child.first_logical != end || !ids.insert(child.id) {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        count = count
            .checked_add(child.extent_count)
            .ok_or(StreamDirectoryError::InvalidBounds)?;
        end = child.end_logical;
    }
    Ok(PageBounds {
        level: page.level,
        first_logical: first,
        end_logical: end,
        extent_count: count,
    })
}

impl StreamDirectoryHead {
    #[must_use]
    pub const fn identity(&self) -> super::StreamRootIdentity {
        super::StreamRootIdentity {
            writer_epoch: self.writer_epoch,
            generation: self.generation,
        }
    }

    /// # Errors
    /// Rejects invalid scope, directory fences, progress or attachment checkpoints.
    pub fn validate(&self) -> Result<(), StreamDirectoryError> {
        if self.metadata_group_id == 0
            || self.writer_epoch == 0
            || self.generation == 0
            || self.owner_session == [0; 16]
            || self.trim_offset > self.published_tail
            || self.previous.is_some_and(|previous| {
                previous.generation >= self.generation
                    || previous.writer_epoch > self.writer_epoch
                    || previous.generation == 0
                    || previous.writer_epoch == 0
            })
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        match self.root {
            Some(root) => {
                root.validate()?;
                if root.first_logical > self.trim_offset || root.end_logical != self.published_tail {
                    return Err(StreamDirectoryError::InvalidBounds);
                }
            }
            None if self.trim_offset != self.published_tail => {
                return Err(StreamDirectoryError::InvalidBounds)
            }
            None => {}
        }
        if let Some(active) = self.active {
            if self.closed || !self.purpose.matches(active.chunk_id) {
                return Err(StreamDirectoryError::WrongPurpose);
            }
            if active.logical_start > active.checkpoint_logical
                || active.checkpoint_logical != self.published_tail
                || active.checkpoint_physical > MAX_STREAM_CHUNK_PHYSICAL_BYTES
            {
                return Err(StreamDirectoryError::InvalidBounds);
            }
            let delta = active.checkpoint_logical - active.logical_start;
            let minimum = crate::frame::FrameLayout::StreamGroupV2
                .physical_length(delta)
                .map_err(|_| StreamDirectoryError::InvalidBounds)?;
            if active.checkpoint_physical < minimum || (delta == 0 && active.checkpoint_physical != 0) {
                return Err(StreamDirectoryError::InvalidBounds);
            }
        }
        Ok(())
    }
}
