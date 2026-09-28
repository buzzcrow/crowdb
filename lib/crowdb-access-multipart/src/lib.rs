// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Storage-backed multipart composition shared by access protocols.
//!
//! Part bytes remain in their original chunks. A completed object contains
//! the selected parts' locations with adjusted logical offsets, while its
//! composite `ETag` uses only the parts' previously recorded raw MD5 digests.

use std::fmt::Write as _;

use crowdb_protocol::chunkdb::rpc::Location;
use md5::{Digest, Md5};

mod state;

pub use state::{reserve_part_accounting, validate_selected_parts, PartAccounting, SelectedPart, StateError};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ComposeError {
    #[error("multipart part count exceeds 10000")]
    TooManyParts,
    #[error("multipart part locations do not cover its logical length")]
    InvalidLocations,
    #[error("multipart object length exceeds its limit")]
    LengthLimit,
    #[error("multipart location offset overflow")]
    OffsetOverflow,
    #[error("multipart completion has no parts")]
    Empty,
}

/// A completed metadata-only multipart object.
pub struct ComposedObject {
    pub locations: Vec<Location>,
    pub length: u64,
    pub etag: String,
}

/// Incrementally composes selected parts in completion order.
pub struct MultipartComposer {
    locations: Vec<Location>,
    length: u64,
    max_length: u64,
    part_count: u16,
    md5: Md5,
}

impl MultipartComposer {
    #[must_use]
    pub fn new(max_length: u64) -> Self {
        Self {
            locations: Vec::new(),
            length: 0,
            max_length,
            part_count: 0,
            md5: Md5::new(),
        }
    }

    /// Adds one already durable part without reading its data.
    ///
    /// # Errors
    /// Rejects invalid or noncontiguous part locations, offset overflow,
    /// too many parts, or an object that exceeds `max_length`. On error the
    /// composer remains unchanged.
    pub fn push(
        &mut self,
        length: u64,
        raw_md5: [u8; 16],
        locations: &[Location],
    ) -> Result<(), ComposeError> {
        if self.part_count >= 10_000 {
            return Err(ComposeError::TooManyParts);
        }
        let next_length = self
            .length
            .checked_add(length)
            .ok_or(ComposeError::OffsetOverflow)?;
        if next_length > self.max_length {
            return Err(ComposeError::LengthLimit);
        }
        let mut cursor = 0_u64;
        for location in locations {
            if location.chunk_id.is_none()
                || location.length == 0
                || location.logical_length == 0
                || location.logical_offset != cursor
                || location.offset.checked_add(location.length).is_none()
            {
                return Err(ComposeError::InvalidLocations);
            }
            cursor = cursor
                .checked_add(location.logical_length)
                .ok_or(ComposeError::OffsetOverflow)?;
            self.length
                .checked_add(location.logical_offset)
                .ok_or(ComposeError::OffsetOverflow)?;
        }
        if cursor != length {
            return Err(ComposeError::InvalidLocations);
        }
        for location in locations {
            let mut adjusted = location.clone();
            adjusted.logical_offset += self.length;
            self.locations.push(adjusted);
        }
        self.md5.update(raw_md5);
        self.length = next_length;
        self.part_count += 1;
        Ok(())
    }

    /// Completes the composite `ETag` from the selected raw part MD5 values.
    ///
    /// # Errors
    /// Rejects a completion with no selected parts.
    pub fn finish(self) -> Result<ComposedObject, ComposeError> {
        if self.part_count == 0 {
            return Err(ComposeError::Empty);
        }
        let mut etag = String::with_capacity(40);
        for byte in self.md5.finalize() {
            write!(&mut etag, "{byte:02x}").expect("string write cannot fail");
        }
        write!(&mut etag, "-{}", self.part_count).expect("string write cannot fail");
        Ok(ComposedObject {
            locations: self.locations,
            length: self.length,
            etag,
        })
    }
}
