// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fenced publication of a copied relocation target.

use std::sync::Arc;

use crowdb_protocol::chunk_task::{
    ChunkTaskValue, RelocateSegmentTaskDisposition, RelocateSegmentTaskPayload,
    RELOCATE_SEGMENT_KIND_VERSION, TASK_KIND_RELOCATE_SEGMENT,
};
use crowdb_protocol::chunkdb::rpc::{ChunkStrip, Strip};

use crate::allocator::assess_physical_placement;
use crate::lifecycle::{LifecycleError, LifecycleHandler};
use crate::relocation::{decode_payload, RelocationAdmissionError};

use super::executor::TaskFuture;
use super::{TaskHandler, TaskManager, TaskOutcome};

const RETRY_DELAY_MS: u64 = 1_000;

#[derive(Debug, thiserror::Error)]
pub enum RelocateSegmentTaskError {
    #[error(transparent)]
    Admission(#[from] RelocationAdmissionError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error("relocation checkpoint persistence failed: {0}")]
    Checkpoint(String),
}

pub struct RelocateSegmentTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
    manager: Arc<TaskManager>,
}

enum Execution {
    Complete,
    Retry,
}

impl RelocateSegmentTaskHandler {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>, manager: Arc<TaskManager>) -> Self {
        Self { lifecycle, manager }
    }

    async fn execute_once(&self, task: &ChunkTaskValue) -> Result<Execution, RelocateSegmentTaskError> {
        let mut payload = decode_payload(&task.payload)?;
        let chunk = match self.lifecycle.query_chunk(&payload.chunk_id).await {
            Ok(chunk) => chunk,
            Err(LifecycleError::ChunkNotFound) => {
                self.finish(task, &mut payload, RelocateSegmentTaskDisposition::Stale)
                    .await?;
                return Ok(Execution::Complete);
            }
            Err(error) => return Err(error.into()),
        };
        let source_location = find_segment(&chunk.strips, &payload.source);
        let target_location = find_segment(&chunk.strips, &payload.target);
        match (source_location, target_location) {
            (None, Some(_)) => {
                self.lifecycle
                    .confirm_tentative_segments(vec![payload.target])
                    .await?;
                if payload.source_free_not_before_ms == 0 {
                    payload.source_free_not_before_ms =
                        now_ms().saturating_add(self.lifecycle.layout_validity_ms());
                    self.checkpoint(task, &payload).await?;
                }
                if now_ms() < payload.source_free_not_before_ms {
                    return Ok(Execution::Retry);
                }
                self.finish(task, &mut payload, RelocateSegmentTaskDisposition::Published)
                    .await?;
                Ok(Execution::Complete)
            }
            (None, None) => {
                self.finish(task, &mut payload, RelocateSegmentTaskDisposition::Stale)
                    .await?;
                Ok(Execution::Complete)
            }
            (Some(_), Some(_)) => {
                self.finish(task, &mut payload, RelocateSegmentTaskDisposition::Rejected)
                    .await?;
                Ok(Execution::Complete)
            }
            (Some((strip_index, segment_index)), None) => {
                let old_strip = chunk.strips[strip_index].clone();
                let mut replacement = old_strip.clone();
                replace_segment(&mut replacement, segment_index, payload.target)?;
                if !self.assess_replacement(&old_strip, &mut replacement)? {
                    self.finish(task, &mut payload, RelocateSegmentTaskDisposition::Rejected)
                        .await?;
                    return Ok(Execution::Complete);
                }
                payload.expected_modify_ts = Some(chunk.modify_ts);
                payload.strip_index = Some(u32::try_from(strip_index).unwrap_or(u32::MAX));
                payload.source_free_not_before_ms =
                    now_ms().saturating_add(self.lifecycle.layout_validity_ms());
                self.checkpoint(task, &payload).await?;
                self.lifecycle
                    .publish_relocation_chunk_strip_range(
                        &payload.chunk_id,
                        chunk.modify_ts,
                        u32::try_from(strip_index).unwrap_or(u32::MAX),
                        std::slice::from_ref(&old_strip),
                        std::slice::from_ref(&replacement),
                        payload.operation_id,
                    )
                    .await?;
                self.lifecycle
                    .confirm_tentative_segments(vec![payload.target])
                    .await?;
                Ok(Execution::Retry)
            }
        }
    }

    async fn checkpoint(
        &self,
        task: &ChunkTaskValue,
        payload: &RelocateSegmentTaskPayload,
    ) -> Result<(), RelocateSegmentTaskError> {
        let bytes = serde_json::to_vec(payload)
            .map_err(|error| RelocateSegmentTaskError::Checkpoint(error.to_string()))?;
        self.manager
            .checkpoint_payload(task, bytes, now_ms())
            .await
            .map_err(|error| RelocateSegmentTaskError::Checkpoint(error.to_string()))?;
        Ok(())
    }

    fn assess_replacement(
        &self,
        current_strip: &ChunkStrip,
        replacement: &mut ChunkStrip,
    ) -> Result<bool, RelocateSegmentTaskError> {
        let (current_segments, loss_budget) = segments_and_loss_budget(current_strip)?;
        let (replacement_segments, replacement_budget) = segments_and_loss_budget(replacement)?;
        if loss_budget != replacement_budget {
            return Err(RelocateSegmentTaskError::Checkpoint(
                "relocation changed strip protection geometry".into(),
            ));
        }
        let topology = self.lifecycle.topology_snapshot();
        let usage_fresh = current_strip
            .placement_assessment
            .as_ref()
            .is_some_and(|assessment| assessment.usage_fresh);
        let current = assess_physical_placement(
            &topology,
            current_segments,
            loss_budget,
            topology.generation(),
            usage_fresh,
        );
        let next = assess_physical_placement(
            &topology,
            replacement_segments,
            loss_budget,
            topology.generation(),
            usage_fresh,
        );
        if weakens_protection(&current, &next) {
            return Ok(false);
        }
        replacement.placement_repair_required =
            !(next.rack_protected && next.node_protected && next.disk_protected);
        replacement.placement_assessment = Some(next);
        Ok(true)
    }

    async fn finish(
        &self,
        task: &ChunkTaskValue,
        payload: &mut RelocateSegmentTaskPayload,
        disposition: RelocateSegmentTaskDisposition,
    ) -> Result<(), RelocateSegmentTaskError> {
        payload.disposition = disposition;
        self.checkpoint(task, payload).await
    }
}

impl TaskHandler for RelocateSegmentTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_RELOCATE_SEGMENT
    }

    fn supports_version(&self, version: u16) -> bool {
        version == RELOCATE_SEGMENT_KIND_VERSION
    }

    fn execute<'a>(&'a self, task: &'a ChunkTaskValue) -> TaskFuture<'a> {
        Box::pin(async move {
            match self.execute_once(task).await {
                Ok(Execution::Complete) => TaskOutcome::Complete,
                Ok(Execution::Retry) => TaskOutcome::Retry {
                    delay_ms: RETRY_DELAY_MS,
                    error_code: 50,
                    error: "relocation awaiting publication grace".into(),
                },
                Err(error) => TaskOutcome::Retry {
                    delay_ms: RETRY_DELAY_MS,
                    error_code: 51,
                    error: error.to_string(),
                },
            }
        })
    }
}

fn find_segment(
    strips: &[ChunkStrip],
    segment: &crowdb_protocol::diskdb::rpc::Segment,
) -> Option<(usize, usize)> {
    strips.iter().enumerate().find_map(|(strip_index, strip)| {
        strip_segments(strip)
            .iter()
            .position(|candidate| candidate == segment)
            .map(|segment_index| (strip_index, segment_index))
    })
}

fn strip_segments(strip: &ChunkStrip) -> &[crowdb_protocol::diskdb::rpc::Segment] {
    match &strip.strip {
        Some(Strip::MirrorStrip(mirror)) => &mirror.segments,
        Some(Strip::EcStrip(ec)) => &ec.segments,
        None => &[],
    }
}

fn replace_segment(
    strip: &mut ChunkStrip,
    segment_index: usize,
    target: crowdb_protocol::diskdb::rpc::Segment,
) -> Result<(), RelocateSegmentTaskError> {
    let segments = match &mut strip.strip {
        Some(Strip::MirrorStrip(mirror)) => &mut mirror.segments,
        Some(Strip::EcStrip(ec)) => &mut ec.segments,
        None => {
            return Err(RelocateSegmentTaskError::Checkpoint(
                "source strip has no body".into(),
            ))
        }
    };
    let slot = segments
        .get_mut(segment_index)
        .ok_or_else(|| RelocateSegmentTaskError::Checkpoint("source segment index is stale".into()))?;
    *slot = target;
    Ok(())
}

fn segments_and_loss_budget(
    strip: &ChunkStrip,
) -> Result<(&[crowdb_protocol::diskdb::rpc::Segment], u32), RelocateSegmentTaskError> {
    match strip.strip.as_ref() {
        Some(Strip::MirrorStrip(mirror)) => Ok((
            &mirror.segments,
            u32::try_from(mirror.segments.len().saturating_sub(1)).unwrap_or(u32::MAX),
        )),
        Some(Strip::EcStrip(ec)) => Ok((&ec.segments, ec.code_num)),
        None => Err(RelocateSegmentTaskError::Checkpoint(
            "relocation strip has no body".into(),
        )),
    }
}

fn weakens_protection(
    current: &crowdb_protocol::chunkdb::rpc::PlacementAssessment,
    next: &crowdb_protocol::chunkdb::rpc::PlacementAssessment,
) -> bool {
    (current.rack_protected && !next.rack_protected)
        || (current.node_protected && !next.node_protected)
        || (current.disk_protected && !next.disk_protected)
        || next.max_fragments_per_rack > current.max_fragments_per_rack
        || next.max_fragments_per_node > current.max_fragments_per_node
        || next.max_fragments_per_disk > current.max_fragments_per_disk
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
