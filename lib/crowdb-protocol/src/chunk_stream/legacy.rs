// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Borrowed access to frozen legacy columns during resumable conversion.

use super::{
    ActiveChunkDescriptor, StreamDirectoryError, StreamExtentLocation, StreamExtentPageFence,
    StreamFrameFormat, StreamName, StreamPurpose,
};
use crate::common::ChunkId;
use crate::frame::{ChunkLocation, FrameLayout};

mod columns;
use columns::{number, LegacyColumns};

pub struct LegacyManifestView<'a> {
    pub purpose: StreamPurpose,
    pub stream_name: StreamName,
    pub metadata_group_id: u64,
    pub writer_epoch: u64,
    pub generation: u64,
    pub trim_offset: u64,
    pub sealed_tail: u64,
    pub active: Option<ActiveChunkDescriptor>,
    pub previous_generation: Option<u64>,
    pub closed: bool,
    fences: &'a [u8],
}

impl<'a> LegacyManifestView<'a> {
    /// # Errors
    /// Rejects nonlegacy records, malformed columns/flags and trailing bytes.
    /// No array is allocated and no fence history is iterated to select a page.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, StreamDirectoryError> {
        let body = if bytes.starts_with(b"CSTR") {
            super::record::decode_envelope(1, bytes)
                .map_err(|error| StreamDirectoryError::Codec(error.to_string()))?
        } else {
            bytes
        };
        let mut columns = LegacyColumns::new(body);
        let purpose = match columns.u32()? {
            0 => StreamPurpose::Wal,
            1 => StreamPurpose::Stream,
            _ => return Err(StreamDirectoryError::WrongPurpose),
        };
        let stream_name = columns.name()?;
        let metadata_group_id = columns.u64()?;
        let writer_epoch = columns.u64()?;
        let generation = columns.u64()?;
        let trim_offset = columns.u64()?;
        let sealed_tail = columns.u64()?;
        let active = if columns.boolean()? {
            Some(ActiveChunkDescriptor {
                chunk_id: columns.chunk()?,
                physical_start: columns.u64()?,
                logical_start: columns.u64()?,
                acknowledged_cursor: columns.u64()?,
                capacity: columns.u64()?,
            })
        } else {
            None
        };
        let fences = columns.sequence(24, None)?;
        let previous_generation = if columns.boolean()? {
            Some(columns.u64()?)
        } else {
            None
        };
        let closed = columns.boolean()?;
        columns.finish()?;
        Ok(Self {
            purpose,
            stream_name,
            metadata_group_id,
            writer_epoch,
            generation,
            trim_offset,
            sealed_tail,
            active,
            previous_generation,
            closed,
            fences,
        })
    }

    #[must_use]
    pub fn fence_count(&self) -> usize {
        self.fences.len() / 24
    }

    /// # Errors
    /// Rejects an out-of-bounds index; returns only this fence's fixed-size fields.
    pub fn fence(&self, index: usize) -> Result<StreamExtentPageFence, StreamDirectoryError> {
        let bytes = columns::element(self.fences, index, 24)?;
        Ok(StreamExtentPageFence {
            page_index: number(&bytes[..8])?,
            first_logical: number(&bytes[8..16])?,
            end_logical: number(&bytes[16..])?,
        })
    }
}

pub struct LegacyExtentPageView<'a> {
    pub stream_name: StreamName,
    pub writer_epoch: u64,
    pub generation: u64,
    pub page_index: u64,
    chunks: &'a [u8],
    logical: &'a [u8],
    physical: &'a [u8],
    lengths: &'a [u8],
}

impl<'a> LegacyExtentPageView<'a> {
    /// # Errors
    /// Explicit raw legacy namespace: arbitrary stream bytes are never sniffed as
    /// an envelope. Rejects invented/unequal array lengths and trailing bytes.
    pub fn from_raw(bytes: &'a [u8]) -> Result<Self, StreamDirectoryError> {
        let mut columns = LegacyColumns::new(bytes);
        let stream_name = columns.name()?;
        let writer_epoch = columns.u64()?;
        let generation = columns.u64()?;
        let page_index = columns.u64()?;
        let chunks = columns.sequence(16, None)?;
        let count = chunks.len() / 16;
        if count == 0 {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        let logical_count = count.checked_add(1).ok_or(StreamDirectoryError::InvalidBounds)?;
        let logical = columns.sequence(8, Some(logical_count))?;
        let physical = columns.sequence(8, Some(count))?;
        let lengths = columns.sequence(4, Some(count))?;
        columns.finish()?;
        Ok(Self {
            stream_name,
            writer_epoch,
            generation,
            page_index,
            chunks,
            logical,
            physical,
            lengths,
        })
    }

    #[must_use]
    pub fn extent_count(&self) -> usize {
        self.chunks.len() / 16
    }

    /// # Errors
    /// Validates one retained per-frame location without rebuilding/copying the arrays.
    pub fn location(&self, index: usize) -> Result<StreamExtentLocation, StreamDirectoryError> {
        let bytes = columns::element(self.chunks, index, 16)?;
        let chunk_id = ChunkId {
            high: number(&bytes[..8])?,
            low: number(&bytes[8..])?,
        };
        let logical_start = number(columns::element(self.logical, index, 8)?)?;
        let logical_end = number(columns::element(
            self.logical,
            index.checked_add(1).ok_or(StreamDirectoryError::InvalidBounds)?,
            8,
        )?)?;
        let logical_length = logical_end
            .checked_sub(logical_start)
            .filter(|length| *length > 0 && *length <= FrameLayout::Legacy.payload_capacity())
            .ok_or(StreamDirectoryError::InvalidBounds)?;
        let physical = number(columns::element(self.physical, index, 8)?)?;
        let length = u32::from_le_bytes(
            columns::element(self.lengths, index, 4)?
                .try_into()
                .map_err(|_| StreamDirectoryError::InvalidBounds)?,
        );
        if FrameLayout::Legacy
            .physical_length(logical_length)
            .map_err(|_| StreamDirectoryError::InvalidBounds)?
            != u64::from(length)
        {
            return Err(StreamDirectoryError::InvalidBounds);
        }
        let location = StreamExtentLocation {
            logical_start,
            location: ChunkLocation {
                chunk_id,
                frame_offset: physical,
                logical_length,
            },
            format: StreamFrameFormat::LegacyV1,
        };
        location.end_physical()?;
        Ok(location)
    }
}
