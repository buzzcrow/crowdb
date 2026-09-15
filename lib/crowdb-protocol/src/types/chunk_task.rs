// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Versioned persistent task envelope shared by chunkdb task clients.

use serde::{Deserialize, Serialize};

use crate::common::ChunkId;

pub const CHUNK_TASK_SCHEMA_VERSION: u16 = 1;
pub const TASK_KIND_MIRROR_TO_EC: u16 = 1;
pub const TASK_KIND_REPAIR_STRIP: u16 = 2;
pub const TASK_KIND_REPAIR_PLACEMENT: u16 = 3;
pub const TASK_KIND_RELOCATE_SEGMENT: u16 = 4;
/// The sole liveness task for an Active chunk.
pub const TASK_KIND_FINALIZE_CHUNK: u16 = 5;
pub const FINALIZE_CHUNK_KIND_VERSION: u16 = 1;
pub const PLACEMENT_REPAIR_KIND_VERSION: u16 = 1;
pub const RELOCATE_SEGMENT_KIND_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementRepairTaskPayload {
    pub chunk_id: ChunkId,
    pub strip_sequence: u32,
    pub placement_priority: i32,
    pub repair_rack: bool,
    pub repair_node: bool,
    pub repair_disk: bool,
    #[serde(default)]
    pub target: Option<RepairTargetCheckpoint>,
}

/// Durable target state for one repair fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairTargetPhase {
    Allocated,
    Copied,
    Published,
    Confirmed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairTargetCheckpoint {
    pub source: crate::diskdb::rpc::Segment,
    pub destination: crate::diskdb::rpc::Segment,
    pub phase: RepairTargetPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum RelocateSegmentTaskDisposition {
    #[default]
    Accepted = 0,
    Published = 1,
    Stale = 2,
    Rejected = 3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelocateSegmentTaskPayload {
    pub operation_id: ChunkId,
    pub chunk_id: ChunkId,
    pub source: crate::diskdb::rpc::Segment,
    pub target: crate::diskdb::rpc::Segment,
    pub disposition: RelocateSegmentTaskDisposition,
    #[serde(default)]
    pub expected_modify_ts: Option<u64>,
    #[serde(default)]
    pub strip_index: Option<u32>,
    #[serde(default)]
    pub source_free_not_before_ms: u64,
}

/// Deterministic relocation identity for one exact source incarnation.
#[must_use]
pub fn relocation_operation_id(source: &crate::diskdb::rpc::Segment) -> Option<ChunkId> {
    let disk = source.disk_id?;
    Some(ChunkId {
        high: disk.high ^ source.allocation_ts.rotate_left(17) ^ u64::from(source.zone_index),
        low: disk.low ^ source.unit_offset.rotate_left(29),
    })
}

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
