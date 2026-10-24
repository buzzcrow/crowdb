// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordered per-chunk Stream metadata and complete-append mappings.

use super::{StreamName, StreamPurpose};
use crate::common::ChunkId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod codec;
mod mappings;
pub use codec::{decode_stream_chunk_record, encode_stream_chunk_record};
pub use mappings::{StreamMapping, StreamMappingIndex, StreamMappings};

pub const MAX_STREAM_CHUNK_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_STREAM_CHUNK_RECORD_BYTES: usize = 24 * 1024 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StreamChunkRecordError {
    #[error("invalid Stream chunk record: {0}")]
    Invalid(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamChunkState {
    Candidate,
    Active,
    Sealed,
    Deleting,
}

/// Logical ranges are assigned only when a candidate becomes attached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamChunkRecord {
    pub stream_id: StreamName,
    pub purpose: StreamPurpose,
    pub chunk_id: ChunkId,
    pub state: StreamChunkState,
    pub logical_start: Option<u64>,
    pub logical_end: Option<u64>,
    pub sequence: u64,
    pub writer_epoch: u64,
    pub generation: u64,
    pub owner_session: [u8; 16],
    pub trim_offset: u64,
    pub closed: bool,
    pub mappings: StreamMappings,
}

impl StreamChunkRecord {
    /// # Errors
    /// Rejects malformed scope, state, coverage and physical geometry.
    pub fn validate(&self) -> Result<StreamMappingIndex, StreamChunkRecordError> {
        use StreamChunkRecordError::Invalid;
        if !self.purpose.matches(self.chunk_id) || self.writer_epoch == 0 || self.owner_session == [0; 16] {
            return Err(Invalid("identity or purpose"));
        }
        if self.state == StreamChunkState::Candidate {
            if self.logical_start.is_some()
                || self.logical_end.is_some()
                || self.closed
                || self.generation != 0
                || self.mappings.count() != 0
                || self.trim_offset != 0
            {
                return Err(Invalid("candidate has attached progress"));
            }
            return self.mappings.index(0, 0);
        }
        let start = self.logical_start.ok_or(Invalid("missing logical start"))?;
        let end = self.logical_end.ok_or(Invalid("missing logical end"))?;
        if start > end
            || self.generation == 0
            || self.trim_offset > end
            || (self.closed && self.state == StreamChunkState::Active)
        {
            return Err(Invalid("logical bounds or state"));
        }
        self.mappings.index(start, end)
    }
}
