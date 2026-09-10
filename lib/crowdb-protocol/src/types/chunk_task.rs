// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned persistent task envelope shared by chunkdb task clients.

use serde::{Deserialize, Serialize};

use crate::common::ChunkId;

pub const CHUNK_TASK_SCHEMA_VERSION: u16 = 1;
pub const TASK_KIND_MIRROR_TO_EC: u16 = 1;
pub const TASK_KIND_REPAIR_STRIP: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum ChunkTaskState {
    #[default]
    Pending = 0,
    Running = 1,
    RetryWait = 2,
    Completed = 3,
    Failed = 4,
    Cancelled = 5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkTaskValue {
    pub schema_version: u16,
    pub task_id: ChunkId,
    pub partition_id: ChunkId,
    pub kind: u16,
    pub kind_version: u16,
    pub state: ChunkTaskState,
    pub priority: u8,
    pub revision: u64,
    pub operation_id: ChunkId,
    pub source_revision: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub eligible_at_ms: u64,
    pub attempt: u32,
    pub max_attempts: u32,
    pub estimated_queue_bytes: u64,
    pub claim_owner: u64,
    pub claim_generation: u64,
    pub claim_deadline_ms: u64,
    pub last_error_code: u16,
    pub last_error: String,
    pub payload: Vec<u8>,
}
