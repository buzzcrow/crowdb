// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable admission for exact-segment relocation handoffs.

use std::sync::Arc;

use crate::task::{TaskAdmission, TaskManager, TaskManagerError};
use crowdb_protocol::chunk_task::{
    relocation_operation_id, ChunkTaskState, ChunkTaskValue, RelocateSegmentTaskDisposition,
    RelocateSegmentTaskPayload, CHUNK_TASK_SCHEMA_VERSION, RELOCATE_SEGMENT_KIND_VERSION,
    TASK_KIND_RELOCATE_SEGMENT,
};
use crowdb_protocol::chunkdb::rpc::{RelocateSegmentHandoffRequest, RelocationHandoffDisposition};

#[derive(Debug, thiserror::Error)]
pub enum RelocationAdmissionError {
    #[error("relocation handoff is missing {0}")]
    Missing(&'static str),
    #[error("relocation handoff geometry or ownership is invalid")]
    InvalidGeometry,
    #[error("relocation operation identity conflicts with its durable task")]
    IdentityConflict,
    #[error(transparent)]
    Manager(#[from] TaskManagerError),
    #[error("relocation task checkpoint is invalid: {0}")]
    Checkpoint(String),
}

pub struct RelocationCoordinator {
    manager: Arc<TaskManager>,
}

impl RelocationCoordinator {
    #[must_use]
    pub fn new(manager: Arc<TaskManager>) -> Self {
        Self { manager }
    }

    /// Persist ownership of the tentative target before acknowledging it.
    pub async fn admit(
        &self,
        request: &RelocateSegmentHandoffRequest,
        now_ms: u64,
    ) -> Result<RelocationHandoffDisposition, RelocationAdmissionError> {
        let payload = validate_request(request)?;
        let task = make_task(&payload, now_ms)?;
        let stored = match self.manager.admit(task).await? {
            TaskAdmission::Created(task) | TaskAdmission::Existing(task) => task,
        };
        let stored_payload = decode_payload(&stored.payload)?;
        if stored_payload.operation_id != payload.operation_id
            || stored_payload.chunk_id != payload.chunk_id
            || stored_payload.source != payload.source
            || stored_payload.target != payload.target
        {
            return Err(RelocationAdmissionError::IdentityConflict);
        }
        if matches!(stored.state, ChunkTaskState::Failed | ChunkTaskState::Cancelled) {
            return Ok(RelocationHandoffDisposition::Rejected);
        }
        Ok(to_handoff_disposition(stored_payload.disposition))
    }
}

pub(crate) fn decode_payload(bytes: &[u8]) -> Result<RelocateSegmentTaskPayload, RelocationAdmissionError> {
    serde_json::from_slice(bytes).map_err(|error| RelocationAdmissionError::Checkpoint(error.to_string()))
}

fn validate_request(
    request: &RelocateSegmentHandoffRequest,
) -> Result<RelocateSegmentTaskPayload, RelocationAdmissionError> {
    let operation_id = request
        .operation_id
        .ok_or(RelocationAdmissionError::Missing("operation_id"))?;
    let chunk_id = request
        .chunk_id
        .ok_or(RelocationAdmissionError::Missing("chunk_id"))?;
    let source = request
        .source
        .ok_or(RelocationAdmissionError::Missing("source"))?;
    let target = request
        .target
        .ok_or(RelocationAdmissionError::Missing("target"))?;
    if source == target
        || relocation_operation_id(&source) != Some(operation_id)
        || source.disk_id.is_none()
        || target.disk_id.is_none()
        || source.owner_chunk != Some(chunk_id)
        || target.owner_chunk != Some(chunk_id)
        || source.unit_count == 0
        || source.unit_count != target.unit_count
        || source.allocation_ts == 0
        || target.allocation_ts == 0
    {
        return Err(RelocationAdmissionError::InvalidGeometry);
    }
    Ok(RelocateSegmentTaskPayload {
        operation_id,
        chunk_id,
        source,
        target,
        disposition: RelocateSegmentTaskDisposition::Accepted,
        expected_modify_ts: None,
        strip_index: None,
        source_free_not_before_ms: 0,
    })
}

fn make_task(
    payload: &RelocateSegmentTaskPayload,
    now_ms: u64,
) -> Result<ChunkTaskValue, RelocationAdmissionError> {
    Ok(ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id: payload.operation_id,
        partition_id: payload.chunk_id,
        kind: TASK_KIND_RELOCATE_SEGMENT,
        kind_version: RELOCATE_SEGMENT_KIND_VERSION,
        state: ChunkTaskState::Pending,
        priority: u8::MAX - 1,
        revision: 1,
        operation_id: payload.operation_id,
        source_revision: 0,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        eligible_at_ms: now_ms,
        attempt: 0,
        max_attempts: u32::MAX,
        estimated_queue_bytes: u64::from(payload.target.unit_count),
        claim_owner: 0,
        claim_generation: 0,
        claim_deadline_ms: 0,
        last_error_code: 0,
        last_error: String::new(),
        payload: serde_json::to_vec(payload)
            .map_err(|error| RelocationAdmissionError::Checkpoint(error.to_string()))?,
    })
}

fn to_handoff_disposition(disposition: RelocateSegmentTaskDisposition) -> RelocationHandoffDisposition {
    match disposition {
        RelocateSegmentTaskDisposition::Accepted => RelocationHandoffDisposition::Accepted,
        RelocateSegmentTaskDisposition::Published => RelocationHandoffDisposition::Published,
        RelocateSegmentTaskDisposition::Stale => RelocationHandoffDisposition::Stale,
        RelocateSegmentTaskDisposition::Rejected => RelocationHandoffDisposition::Rejected,
    }
}
