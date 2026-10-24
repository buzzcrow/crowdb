// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded views of the already-published stream state; no chunk reads.

use crate::ChunkId;
use crate::{Result, StreamError};
use crowdb_protocol::chunk_stream::{StreamChunkState, StreamName};

use super::ChunkStream;

#[derive(Clone, Debug)]
pub struct StreamMetadataObservation {
    pub generation: u64,
    pub writer_epoch: u64,
    pub metadata_group_id: u64,
    pub trim_offset: u64,
    pub published_tail: u64,
    pub closed: bool,
    pub chunk: StreamChunkRecordObservation,
    pub offset: usize,
    pub next_offset: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamChunkRecordObservation {
    pub stream_id: StreamName,
    pub chunk_id: ChunkId,
    pub state: StreamChunkState,
    pub logical_start: u64,
    pub logical_end: u64,
    pub mapping_count: u32,
    pub mapping_bytes: usize,
}

impl ChunkStream {
    /// Copies the published tail `StreamChunkRecord` without reading chunk data.
    ///
    /// # Errors
    /// Rejects continuations without a generation, stale generations and invalid offsets.
    pub fn observe_metadata(
        &self,
        generation: Option<u64>,
        offset: usize,
    ) -> Result<StreamMetadataObservation> {
        let state = self.state.load();
        if generation.is_some_and(|value| value != state.generation) {
            return Err(StreamError::StaleWriter);
        }
        if offset != 0 {
            return Err(StreamError::InvalidRequest(
                "the tail chunk record has no continuation".into(),
            ));
        }
        Ok(StreamMetadataObservation {
            generation: state.generation,
            writer_epoch: state.writer_epoch,
            metadata_group_id: state.metadata_group_id,
            trim_offset: state.trim_offset,
            published_tail: state.published_tail,
            closed: state.closed,
            chunk: StreamChunkRecordObservation {
                stream_id: state.stream_name,
                chunk_id: state.snapshot.record().chunk_id,
                state: state.snapshot.record().state,
                logical_start: state
                    .snapshot
                    .record()
                    .logical_start
                    .ok_or_else(|| StreamError::Corruption("attached record lacks logical start".into()))?,
                logical_end: state.published_tail,
                mapping_count: state.snapshot.record().mappings.count(),
                mapping_bytes: state.snapshot.record().mappings.encoded_len(),
            },
            offset,
            next_offset: None,
        })
    }
}
