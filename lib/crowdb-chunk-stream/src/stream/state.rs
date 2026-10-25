// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Mutable owner progress and a bounded captured view of the tail chunk record.

use super::StreamChunkRecordSnapshot;
use crate::{Result, StreamChunkStore, StreamError};
use crowdb_protocol::chunk_stream::{
    ActiveChunkDescriptor, StreamChunkRecord, StreamChunkState, StreamName, StreamPurpose,
};

pub(super) struct StreamViewState {
    pub(super) purpose: StreamPurpose,
    pub(super) stream_name: StreamName,
    pub(super) metadata_group_id: u64,
    pub(super) writer_epoch: u64,
    pub(super) generation: u64,
    pub(super) trim_offset: u64,
    pub(super) published_tail: u64,
    pub(super) closed: bool,
    pub(super) snapshot: StreamChunkRecordSnapshot,
}

impl StreamViewState {
    pub(super) fn new(snapshot: StreamChunkRecordSnapshot, group: u64) -> Self {
        let record = snapshot.record();
        Self {
            purpose: record.purpose,
            stream_name: record.stream_id,
            metadata_group_id: group,
            writer_epoch: record.writer_epoch,
            generation: record.generation,
            trim_offset: record.trim_offset,
            published_tail: record.logical_end.expect("validated attached record"),
            closed: record.closed,
            snapshot,
        }
    }
}

pub(super) struct WriterProgress {
    pub(super) purpose: StreamPurpose,
    pub(super) generation: u64,
    pub(super) trim_offset: u64,
    pub(super) logical_tail: u64,
    pub(super) active: Option<ActiveChunkDescriptor>,
    pub(super) closed: bool,
}

impl WriterProgress {
    pub(super) fn new(record: &StreamChunkRecord, active: Option<ActiveChunkDescriptor>) -> Self {
        Self {
            purpose: record.purpose,
            generation: record.generation,
            trim_offset: record.trim_offset,
            logical_tail: record.logical_end.expect("validated attached record"),
            active,
            closed: record.closed,
        }
    }
}

pub(super) async fn load_active(
    snapshot: &StreamChunkRecordSnapshot,
    chunks: &dyn StreamChunkStore,
) -> Result<Option<ActiveChunkDescriptor>> {
    let record = snapshot.record();
    if record.state != StreamChunkState::Active {
        return Ok(None);
    }
    let capacity = chunks.current_capacity(record.chunk_id).await?;
    let physical = snapshot.index().physical_end();
    if physical > capacity {
        return Err(StreamError::Corruption(
            "Stream checkpoint exceeds chunk capacity".into(),
        ));
    }
    Ok(Some(ActiveChunkDescriptor {
        chunk_id: record.chunk_id,
        logical_start: record.logical_end.expect("validated"),
        physical_start: physical,
        acknowledged_cursor: physical,
        capacity,
    }))
}
