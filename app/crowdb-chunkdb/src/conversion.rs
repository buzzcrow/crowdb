// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Mirror-to-EC task payload and foreground no-reread coordination.

use std::sync::Arc;

use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, CHUNK_TASK_SCHEMA_VERSION, TASK_KIND_MIRROR_TO_EC,
};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip, EcState, Strip};
use crowdb_protocol::common::ChunkId;
use serde::{Deserialize, Serialize};

use crate::lifecycle::{LifecycleError, LifecycleHandler};
use crate::task::{TaskStore, TaskStoreError};

pub const MIRROR_TO_EC_TASK_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MirrorToEcPhase {
    Discovered,
    Allocated,
    Durable,
    Published,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MirrorToEcTaskV1 {
    pub chunk_id: ChunkId,
    pub expected_modify_ts: u64,
    pub start_index: u32,
    pub old_strips: Vec<ChunkStrip>,
    pub data_num: u32,
    pub code_num: u32,
    pub replacement_strip: Option<ChunkStrip>,
    pub phase: MirrorToEcPhase,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedConversion {
    pub task_id: ChunkId,
    pub operation_id: ChunkId,
    pub replacement_strip: ChunkStrip,
}

#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    TaskStore(#[from] TaskStoreError),
    #[error("conversion task payload is invalid: {0}")]
    Payload(String),
    #[error("conversion task conflicts with the requested mirror range")]
    Conflict,
    #[error("conversion client does not own the task claim")]
    StaleClaim,
}

/// Coordinates client-side no-reread conversion with durable task takeover.
pub struct ConversionCoordinator {
    lifecycle: Arc<LifecycleHandler>,
    tasks: Arc<TaskStore>,
}

impl ConversionCoordinator {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>, tasks: Arc<TaskStore>) -> Self {
        Self { lifecycle, tasks }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prepare(
        &self,
        chunk_id: ChunkId,
        expected_modify_ts: u64,
        start_index: u32,
        old_strips: Vec<ChunkStrip>,
        data_num: u32,
        code_num: u32,
        client_owner: u64,
        claim_lease_ms: u64,
        now_ms: u64,
    ) -> Result<PreparedConversion, ConversionError> {
        let chunk = self.lifecycle.query_chunk(&chunk_id).await?;
        validate_source(&chunk, expected_modify_ts, start_index, &old_strips)?;
        let task_id = conversion_task_id(&old_strips, data_num, code_num)?;
        let operation_id = conversion_operation_id(chunk_id, task_id);
        if let Some(existing) = self
            .tasks
            .get(&chunk_id, TASK_KIND_MIRROR_TO_EC, &task_id)
            .await?
        {
            let payload = decode_payload(&existing.payload)?;
            if !matches_request(
                &payload,
                chunk_id,
                expected_modify_ts,
                start_index,
                &old_strips,
                data_num,
                code_num,
            ) {
                return Err(ConversionError::Conflict);
            }
            if existing.state == ChunkTaskState::Completed {
                let replacement_strip = payload.replacement_strip.ok_or(ConversionError::Conflict)?;
                return Ok(PreparedConversion {
                    task_id,
                    operation_id,
                    replacement_strip,
                });
            }
            if existing.state != ChunkTaskState::Running || existing.claim_owner != client_owner {
                return Err(ConversionError::StaleClaim);
            }
            if let Some(replacement_strip) = payload.replacement_strip {
                return Ok(PreparedConversion {
                    task_id,
                    operation_id,
                    replacement_strip,
                });
            }
            return self
                .allocate_and_checkpoint(existing, payload, operation_id, now_ms)
                .await;
        }

        let payload = MirrorToEcTaskV1 {
            chunk_id,
            expected_modify_ts,
            start_index,
            old_strips,
            data_num,
            code_num,
            replacement_strip: None,
            phase: MirrorToEcPhase::Discovered,
        };
        let task = make_client_task(
            task_id,
            operation_id,
            &payload,
            client_owner,
            claim_lease_ms,
            now_ms,
        )?;
        self.tasks.write_transition(None, &task).await?;

        self.allocate_and_checkpoint(task, payload, operation_id, now_ms)
            .await
    }

    async fn allocate_and_checkpoint(
        &self,
        task: ChunkTaskValue,
        mut payload: MirrorToEcTaskV1,
        operation_id: ChunkId,
        now_ms: u64,
    ) -> Result<PreparedConversion, ConversionError> {
        let replacement = self
            .lifecycle
            .allocate_conversion_strip(
                &payload.chunk_id,
                &payload.old_strips,
                payload.data_num,
                payload.code_num,
            )
            .await?;
        payload.replacement_strip = Some(replacement.clone());
        payload.phase = MirrorToEcPhase::Allocated;
        let mut allocated_task = task.clone();
        allocated_task.revision = allocated_task.revision.saturating_add(1);
        allocated_task.updated_at_ms = now_ms;
        allocated_task.payload = encode_payload(&payload)?;
        if let Err(error) = self.tasks.write_transition(Some(&task), &allocated_task).await {
            let _ = self
                .lifecycle
                .discard_conversion_strip(&payload.chunk_id, &replacement)
                .await;
            return Err(error.into());
        }
        Ok(PreparedConversion {
            task_id: task.task_id,
            operation_id,
            replacement_strip: replacement,
        })
    }

    pub async fn complete(
        &self,
        chunk_id: ChunkId,
        task_id: ChunkId,
        client_owner: u64,
        now_ms: u64,
    ) -> Result<Chunk, ConversionError> {
        let task = self
            .tasks
            .get(&chunk_id, TASK_KIND_MIRROR_TO_EC, &task_id)
            .await?
            .ok_or(ConversionError::Conflict)?;
        if task.state == ChunkTaskState::Completed {
            return self.lifecycle.query_chunk(&chunk_id).await.map_err(Into::into);
        }
        if task.state != ChunkTaskState::Running || task.claim_owner != client_owner {
            return Err(ConversionError::StaleClaim);
        }
        let mut payload = decode_payload(&task.payload)?;
        let mut replacement = payload
            .replacement_strip
            .clone()
            .ok_or(ConversionError::Conflict)?;
        let Some(Strip::EcStrip(ec)) = &mut replacement.strip else {
            return Err(ConversionError::Payload("replacement is not EC".into()));
        };
        ec.ec_state = EcState::Parity as i32;
        let chunk = self
            .lifecycle
            .replace_chunk_strip_range(
                &chunk_id,
                payload.expected_modify_ts,
                payload.start_index,
                &payload.old_strips,
                std::slice::from_ref(&replacement),
                task.operation_id,
            )
            .await?;
        payload.replacement_strip = Some(replacement);
        payload.phase = MirrorToEcPhase::Published;
        let mut completed = task.clone();
        completed.state = ChunkTaskState::Completed;
        completed.revision = completed.revision.saturating_add(1);
        completed.updated_at_ms = now_ms;
        completed.claim_owner = 0;
        completed.claim_deadline_ms = 0;
        completed.payload = encode_payload(&payload)?;
        self.tasks.write_transition(Some(&task), &completed).await?;
        Ok(chunk)
    }
}

fn make_client_task(
    task_id: ChunkId,
    operation_id: ChunkId,
    payload: &MirrorToEcTaskV1,
    client_owner: u64,
    lease_ms: u64,
    now_ms: u64,
) -> Result<ChunkTaskValue, ConversionError> {
    Ok(ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id,
        partition_id: payload.chunk_id,
        kind: TASK_KIND_MIRROR_TO_EC,
        kind_version: MIRROR_TO_EC_TASK_VERSION,
        state: ChunkTaskState::Running,
        priority: u8::MAX,
        revision: 1,
        operation_id,
        source_revision: payload.expected_modify_ts,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        eligible_at_ms: 0,
        attempt: 1,
        max_attempts: 16,
        estimated_queue_bytes: payload
            .old_strips
            .iter()
            .map(|strip| u64::from(strip.capacity) * 1024)
            .sum(),
        claim_owner: client_owner,
        claim_generation: 1,
        claim_deadline_ms: now_ms.saturating_add(lease_ms.max(1)),
        last_error_code: 0,
        last_error: String::new(),
        payload: encode_payload(payload)?,
    })
}

fn validate_source(
    chunk: &Chunk,
    expected_modify_ts: u64,
    start_index: u32,
    old_strips: &[ChunkStrip],
) -> Result<(), ConversionError> {
    let start = usize::try_from(start_index).unwrap_or(usize::MAX);
    let end = start.saturating_add(old_strips.len());
    if old_strips.is_empty()
        || chunk.modify_ts != expected_modify_ts
        || chunk.strips.get(start..end) != Some(old_strips)
        || old_strips
            .iter()
            .any(|strip| !matches!(&strip.strip, Some(Strip::MirrorStrip(_))))
    {
        return Err(ConversionError::Conflict);
    }
    let closed = chunk.state == ChunkState::Sealed as i32
        || chunk
            .closed_strip_sequence
            .is_some_and(|sequence| old_strips.iter().all(|strip| strip.strip_sequence <= sequence));
    if !closed {
        return Err(ConversionError::Conflict);
    }
    Ok(())
}

fn conversion_task_id(
    old_strips: &[ChunkStrip],
    data_num: u32,
    code_num: u32,
) -> Result<ChunkId, ConversionError> {
    let first = old_strips.first().ok_or(ConversionError::Conflict)?;
    Ok(ChunkId {
        high: u64::from(first.strip_sequence),
        low: (u64::from(data_num) << 32) | u64::from(code_num),
    })
}

fn conversion_operation_id(chunk_id: ChunkId, task_id: ChunkId) -> ChunkId {
    ChunkId {
        high: chunk_id.high ^ task_id.low.rotate_left(17) ^ 0x93ec_0000_0000_0001,
        low: chunk_id.low ^ task_id.high.rotate_left(29) ^ 0xec93_0000_0000_0001,
    }
}

fn matches_request(
    payload: &MirrorToEcTaskV1,
    chunk_id: ChunkId,
    expected_modify_ts: u64,
    start_index: u32,
    old_strips: &[ChunkStrip],
    data_num: u32,
    code_num: u32,
) -> bool {
    payload.chunk_id == chunk_id
        && payload.expected_modify_ts == expected_modify_ts
        && payload.start_index == start_index
        && payload.old_strips == old_strips
        && payload.data_num == data_num
        && payload.code_num == code_num
}

pub fn encode_payload(payload: &MirrorToEcTaskV1) -> Result<Vec<u8>, ConversionError> {
    bincode::serialize(payload).map_err(|error| ConversionError::Payload(error.to_string()))
}

pub fn decode_payload(bytes: &[u8]) -> Result<MirrorToEcTaskV1, ConversionError> {
    bincode::deserialize(bytes).map_err(|error| ConversionError::Payload(error.to_string()))
}
