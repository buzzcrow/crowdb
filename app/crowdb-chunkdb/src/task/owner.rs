// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Resolves whether ChunkDB durable state owns one exact DiskDB segment.

use std::sync::Arc;

use crowdb_protocol::chunk_task::{
    ChunkTaskState, TASK_KIND_MIRROR_TO_EC, TASK_KIND_RELOCATE_SEGMENT, TASK_KIND_REPAIR_PLACEMENT,
    TASK_KIND_REPAIR_STRIP,
};
use crowdb_protocol::chunkdb::rpc::SegmentOwnerDisposition;
use crowdb_protocol::chunkdb::rpc::Strip;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;

use crate::conversion;
use crate::lifecycle::{LifecycleError, LifecycleHandler};
use crate::placement_repair;
use crate::repair;

use super::{TaskStore, TaskStoreError};

/// Errors are deliberately distinct from [`SegmentOwnerDisposition::Absent`]:
/// the DiskDB scanner retains a block when the owner cannot be queried.
#[derive(Debug, thiserror::Error)]
pub enum SegmentOwnerError {
    #[error("segment owner does not match queried chunk")]
    OwnerMismatch,
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Store(#[from] TaskStoreError),
    #[error("durable task checkpoint is invalid: {0}")]
    Checkpoint(String),
}

/// Resolves metadata references before looking at active durable task targets.
pub struct SegmentOwnerResolver {
    lifecycle: Arc<LifecycleHandler>,
    tasks: Arc<TaskStore>,
}

impl SegmentOwnerResolver {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>, tasks: Arc<TaskStore>) -> Self {
        Self { lifecycle, tasks }
    }

    /// Return the disposition for the exact DiskDB allocation incarnation.
    pub async fn resolve(
        &self,
        chunk_id: &ChunkId,
        segment: &Segment,
    ) -> Result<SegmentOwnerDisposition, SegmentOwnerError> {
        if segment.owner_chunk.as_ref() != Some(chunk_id) {
            return Err(SegmentOwnerError::OwnerMismatch);
        }
        match self.lifecycle.query_chunk(chunk_id).await {
            Ok(chunk)
                if chunk
                    .strips
                    .iter()
                    .flat_map(strip_segments)
                    .any(|current| current == *segment) =>
            {
                return Ok(SegmentOwnerDisposition::Referenced)
            }
            Ok(_) => {}
            Err(LifecycleError::ChunkNotFound) => return Ok(SegmentOwnerDisposition::Absent),
            Err(error) => return Err(error.into()),
        }

        for task in self.tasks.list_partition(chunk_id).await? {
            if !is_active(task.state) {
                continue;
            }
            if task_owns_segment(task.kind, &task.payload, segment)? {
                return Ok(SegmentOwnerDisposition::TaskPending);
            }
        }
        Ok(SegmentOwnerDisposition::Absent)
    }
}

fn is_active(state: ChunkTaskState) -> bool {
    matches!(
        state,
        ChunkTaskState::Pending | ChunkTaskState::Running | ChunkTaskState::RetryWait
    )
}

fn strip_segments(strip: &crowdb_protocol::chunkdb::rpc::ChunkStrip) -> Vec<Segment> {
    match &strip.strip {
        Some(Strip::MirrorStrip(mirror)) => mirror.segments.clone(),
        Some(Strip::EcStrip(ec)) => ec.segments.clone(),
        None => Vec::new(),
    }
}

fn task_owns_segment(kind: u16, payload: &[u8], segment: &Segment) -> Result<bool, SegmentOwnerError> {
    match kind {
        TASK_KIND_MIRROR_TO_EC => conversion::decode_payload(payload)
            .map(|checkpoint| {
                checkpoint
                    .replacement_strip
                    .as_ref()
                    .is_some_and(|strip| strip_segments(strip).contains(segment))
            })
            .map_err(|error| SegmentOwnerError::Checkpoint(error.to_string())),
        TASK_KIND_REPAIR_STRIP => repair::decode_payload(payload)
            .map(|checkpoint| {
                checkpoint
                    .targets
                    .iter()
                    .any(|target| target.destination == *segment)
            })
            .map_err(|error| SegmentOwnerError::Checkpoint(error.to_string())),
        TASK_KIND_REPAIR_PLACEMENT => placement_repair::decode_payload(payload)
            .map(|checkpoint| {
                checkpoint
                    .target
                    .is_some_and(|target| target.destination == *segment)
            })
            .map_err(|error| SegmentOwnerError::Checkpoint(error.to_string())),
        TASK_KIND_RELOCATE_SEGMENT => {
            serde_json::from_slice::<crowdb_protocol::chunk_task::RelocateSegmentTaskPayload>(payload)
                .map(|checkpoint| checkpoint.target == *segment)
                .map_err(|error| SegmentOwnerError::Checkpoint(error.to_string()))
        }
        _ => Ok(false),
    }
}
