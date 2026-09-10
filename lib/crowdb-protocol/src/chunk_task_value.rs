// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Flatbuffer persistence codec for chunk task values.

use flatbuffers::FlatBufferBuilder;

use crate::chunk_task::{ChunkTaskState, ChunkTaskValue};
use crate::chunk_task_fb::{
    fbchunk_task_value_buffer_has_identifier, finish_fbchunk_task_value_buffer, FBChunkTaskState,
    FBChunkTaskValue, FBChunkTaskValueArgs, FBInt128,
};
use crate::common::ChunkId;

const MAX_LAST_ERROR_BYTES: usize = 1_024;

#[derive(Debug, thiserror::Error)]
pub enum ChunkTaskValueError {
    #[error("task value has no CTSK identifier")]
    Identifier,
    #[error("invalid task flatbuffer")]
    InvalidFlatbuffer,
    #[error("task value is missing {0}")]
    Missing(&'static str),
    #[error("task value has unknown state {0}")]
    UnknownState(u8),
}

#[must_use]
pub fn encode_chunk_task_value(value: &ChunkTaskValue) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let payload = builder.create_vector(&value.payload);
    let error = truncate_utf8(&value.last_error, MAX_LAST_ERROR_BYTES);
    let last_error = (!error.is_empty()).then(|| builder.create_string(error));
    let task_id = fb_id(value.task_id);
    let partition_id = fb_id(value.partition_id);
    let operation_id = fb_id(value.operation_id);
    let task = FBChunkTaskValue::create(
        &mut builder,
        &FBChunkTaskValueArgs {
            schema_version: value.schema_version,
            task_id: Some(&task_id),
            partition_id: Some(&partition_id),
            kind: value.kind,
            kind_version: value.kind_version,
            state: encode_state(value.state),
            priority: value.priority,
            revision: value.revision,
            operation_id: Some(&operation_id),
            source_revision: value.source_revision,
            created_at_ms: value.created_at_ms,
            updated_at_ms: value.updated_at_ms,
            eligible_at_ms: value.eligible_at_ms,
            attempt: value.attempt,
            max_attempts: value.max_attempts,
            estimated_queue_bytes: value.estimated_queue_bytes,
            claim_owner: value.claim_owner,
            claim_generation: value.claim_generation,
            claim_deadline_ms: value.claim_deadline_ms,
            last_error_code: value.last_error_code,
            last_error,
            payload: Some(payload),
        },
    );
    finish_fbchunk_task_value_buffer(&mut builder, task);
    builder.finished_data().to_vec()
}

/// Decode and verify a persistent task value.
///
/// # Errors
/// Returns [`ChunkTaskValueError`] when the identifier, `FlatBuffer`, required
/// identifiers, or task state is invalid.
pub fn decode_chunk_task_value(bytes: &[u8]) -> Result<ChunkTaskValue, ChunkTaskValueError> {
    if bytes.len() < 8 || !fbchunk_task_value_buffer_has_identifier(bytes) {
        return Err(ChunkTaskValueError::Identifier);
    }
    let value = flatbuffers::root::<FBChunkTaskValue<'_>>(bytes)
        .map_err(|_| ChunkTaskValueError::InvalidFlatbuffer)?;
    Ok(ChunkTaskValue {
        schema_version: value.schema_version(),
        task_id: decode_id(value.task_id(), "task_id")?,
        partition_id: decode_id(value.partition_id(), "partition_id")?,
        kind: value.kind(),
        kind_version: value.kind_version(),
        state: decode_state(value.state())?,
        priority: value.priority(),
        revision: value.revision(),
        operation_id: decode_id(value.operation_id(), "operation_id")?,
        source_revision: value.source_revision(),
        created_at_ms: value.created_at_ms(),
        updated_at_ms: value.updated_at_ms(),
        eligible_at_ms: value.eligible_at_ms(),
        attempt: value.attempt(),
        max_attempts: value.max_attempts(),
        estimated_queue_bytes: value.estimated_queue_bytes(),
        claim_owner: value.claim_owner(),
        claim_generation: value.claim_generation(),
        claim_deadline_ms: value.claim_deadline_ms(),
        last_error_code: value.last_error_code(),
        last_error: value.last_error().unwrap_or_default().to_owned(),
        payload: value
            .payload()
            .map_or_else(Vec::new, |payload| payload.bytes().to_vec()),
    })
}

fn fb_id(value: ChunkId) -> FBInt128 {
    FBInt128::new(value.high, value.low)
}

fn decode_id(value: Option<&FBInt128>, field: &'static str) -> Result<ChunkId, ChunkTaskValueError> {
    value
        .map(|id| ChunkId {
            high: id.high(),
            low: id.low(),
        })
        .ok_or(ChunkTaskValueError::Missing(field))
}

const fn encode_state(state: ChunkTaskState) -> FBChunkTaskState {
    match state {
        ChunkTaskState::Pending => FBChunkTaskState::Pending,
        ChunkTaskState::Running => FBChunkTaskState::Running,
        ChunkTaskState::RetryWait => FBChunkTaskState::RetryWait,
        ChunkTaskState::Completed => FBChunkTaskState::Completed,
        ChunkTaskState::Failed => FBChunkTaskState::Failed,
        ChunkTaskState::Cancelled => FBChunkTaskState::Cancelled,
    }
}

fn decode_state(state: FBChunkTaskState) -> Result<ChunkTaskState, ChunkTaskValueError> {
    match state {
        FBChunkTaskState::Pending => Ok(ChunkTaskState::Pending),
        FBChunkTaskState::Running => Ok(ChunkTaskState::Running),
        FBChunkTaskState::RetryWait => Ok(ChunkTaskState::RetryWait),
        FBChunkTaskState::Completed => Ok(ChunkTaskState::Completed),
        FBChunkTaskState::Failed => Ok(ChunkTaskState::Failed),
        FBChunkTaskState::Cancelled => Ok(ChunkTaskState::Cancelled),
        other => Err(ChunkTaskValueError::UnknownState(other.0)),
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}
