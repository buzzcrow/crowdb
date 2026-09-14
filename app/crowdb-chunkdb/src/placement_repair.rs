// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable reconciliation and one-fragment repair for degraded EC placement.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, PlacementRepairTaskPayload, CHUNK_TASK_SCHEMA_VERSION,
    PLACEMENT_REPAIR_KIND_VERSION, TASK_KIND_REPAIR_PLACEMENT,
};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, Strip};
use crowdb_protocol::common::ChunkId;
use serde_json::to_vec;

use crate::allocator::assess_physical_placement;
use crate::conversion::io::ConversionDiskIo;
use crate::lifecycle::{LifecycleError, LifecycleHandler};
use crate::metrics::PlacementMetrics;
use crate::task::executor::TaskFuture;
use crate::task::{TaskHandler, TaskOutcome, TaskStore, TaskStoreError};

const RETRY_DELAY_MS: u64 = 5_000;

#[derive(Debug, thiserror::Error)]
pub enum PlacementRepairError {
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Store(#[from] TaskStoreError),
    #[error("placement repair payload is invalid: {0}")]
    Payload(String),
}

/// Recreates a durable task for every EC strip carrying the repair marker.
pub struct PlacementRepairCoordinator {
    lifecycle: Arc<LifecycleHandler>,
    tasks: Arc<TaskStore>,
    wake: Option<Arc<tokio::sync::Notify>>,
    metrics: Option<Arc<PlacementMetrics>>,
    scan_cursor: ArcSwapOption<ChunkId>,
}

impl PlacementRepairCoordinator {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>, tasks: Arc<TaskStore>) -> Self {
        Self {
            lifecycle,
            tasks,
            wake: None,
            metrics: None,
            scan_cursor: ArcSwapOption::empty(),
        }
    }

    #[must_use]
    pub fn with_wake(mut self, wake: Arc<tokio::sync::Notify>) -> Self {
        self.wake = Some(wake);
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<PlacementMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub async fn admit_chunk(&self, chunk: &Chunk, now_ms: u64) -> Result<u64, PlacementRepairError> {
        let admitted = admit_placement_chunk(&self.tasks, chunk, now_ms).await?;
        if admitted != 0 {
            if let Some(metrics) = &self.metrics {
                metrics.admitted(admitted);
            }
            if let Some(wake) = &self.wake {
                wake.notify_one();
            }
        }
        Ok(admitted)
    }

    pub async fn scan_batch(&self, max_chunks: u32, now_ms: u64) -> Result<u64, PlacementRepairError> {
        let limit = max_chunks.max(1);
        let chunks = self
            .lifecycle
            .list_chunks(self.scan_cursor.load_full().as_deref(), limit)
            .await?;
        if chunks.is_empty() {
            self.scan_cursor.store(None);
            return Ok(0);
        }
        if chunks.len() < usize::try_from(limit).unwrap_or(usize::MAX) {
            self.scan_cursor.store(None);
        } else if let Some(last) = chunks.last().and_then(|chunk| chunk.id) {
            self.scan_cursor.store(Some(Arc::new(last)));
        }
        let mut admitted = 0;
        for chunk in chunks {
            admitted += self.admit_chunk(&chunk, now_ms).await?;
        }
        Ok(admitted)
    }
}

/// Admit one deterministic task per marked EC strip. It is shared by the
/// foreground publication path and the periodic crash-gap reconciler.
pub async fn admit_placement_chunk(
    tasks: &TaskStore,
    chunk: &Chunk,
    now_ms: u64,
) -> Result<u64, PlacementRepairError> {
    let chunk_id = chunk
        .id
        .ok_or_else(|| PlacementRepairError::Payload("chunk has no ID".into()))?;
    let mut admitted = 0;
    for strip in &chunk.strips {
        if !strip.placement_repair_required || !matches!(strip.strip, Some(Strip::EcStrip(_))) {
            continue;
        }
        let payload = payload_for_strip(chunk_id, strip)?;
        let task_id = placement_task_id(chunk_id, strip.strip_sequence);
        let existing = tasks.get(&chunk_id, TASK_KIND_REPAIR_PLACEMENT, &task_id).await?;
        if existing.as_ref().is_some_and(|task| {
            matches!(
                task.state,
                ChunkTaskState::Pending | ChunkTaskState::Running | ChunkTaskState::RetryWait
            )
        }) {
            continue;
        }
        let mut task = make_task(task_id, chunk.modify_ts, strip, &payload, now_ms)?;
        if let Some(ref previous) = existing {
            task.revision = previous.revision.saturating_add(1);
            task.created_at_ms = previous.created_at_ms;
            task.claim_generation = previous.claim_generation;
        }
        tasks.write_transition(existing.as_ref(), &task).await?;
        admitted += 1;
    }
    Ok(admitted)
}

/// Copies one existing EC fragment to a newly allocated destination and only
/// publishes the move when the verified assessment strictly improves.
pub struct PlacementRepairTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
    io: Arc<ConversionDiskIo>,
    metrics: Arc<PlacementMetrics>,
}

impl PlacementRepairTaskHandler {
    #[must_use]
    pub fn new(
        lifecycle: Arc<LifecycleHandler>,
        io: Arc<ConversionDiskIo>,
        metrics: Arc<PlacementMetrics>,
    ) -> Self {
        Self {
            lifecycle,
            io,
            metrics,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_once(&self, task: &ChunkTaskValue) -> Result<bool, PlacementRepairError> {
        let payload = decode_payload(&task.payload)?;
        let chunk = self.lifecycle.query_chunk(&payload.chunk_id).await?;
        let Some((index, strip)) = chunk
            .strips
            .iter()
            .enumerate()
            .find(|(_, strip)| strip.strip_sequence == payload.strip_sequence)
        else {
            return Ok(true);
        };
        if !strip.placement_repair_required {
            return Ok(true);
        }
        let Some(Strip::EcStrip(ec)) = strip.strip.as_ref() else {
            return Ok(true);
        };
        let snapshot = self.lifecycle.topology_snapshot();
        let current = assess_physical_placement(
            &snapshot,
            &ec.segments,
            ec.code_num,
            snapshot.generation(),
            strip
                .placement_assessment
                .as_ref()
                .is_some_and(|assessment| assessment.usage_fresh),
        );
        if current.rack_protected && current.node_protected && current.disk_protected {
            self.clear_marker(&chunk, index, strip, current, task.operation_id)
                .await?;
            return Ok(true);
        }
        let Some(source_index) = select_source(&snapshot, &ec.segments, ec.code_num) else {
            return Ok(false);
        };
        let source = ec.segments[source_index];
        let survivors: Vec<_> = ec
            .segments
            .iter()
            .enumerate()
            .filter_map(|(position, segment)| (position != source_index).then_some(*segment))
            .collect();
        let excluded: Vec<_> = ec.segments.iter().filter_map(|segment| segment.disk_id).collect();
        let destination = self
            .lifecycle
            .allocate_replacement_segment(&payload.chunk_id, &source, &survivors, &excluded)
            .await?;
        let unit_bytes = u64::from(strip.unit_kb).saturating_mul(1024);
        let copy = async {
            let bytes = self
                .io
                .read_segment(&source, unit_bytes)
                .await
                .map_err(|error| error.to_string())?;
            self.io
                .write_segment(&destination, unit_bytes, bytes)
                .await
                .map_err(|error| error.to_string())?;
            self.io
                .fsync_segment(&destination)
                .await
                .map_err(|error| error.to_string())
        }
        .await;
        if let Err(error) = copy {
            self.lifecycle
                .discard_replacement_segment(&payload.chunk_id, destination)
                .await?;
            return Err(PlacementRepairError::Payload(error));
        }
        let mut replacement = strip.clone();
        let Some(Strip::EcStrip(replacement_ec)) = replacement.strip.as_mut() else {
            return Ok(false);
        };
        replacement_ec.segments[source_index] = destination;
        let next = assess_physical_placement(
            &snapshot,
            &replacement_ec.segments,
            ec.code_num,
            snapshot.generation(),
            current.usage_fresh,
        );
        if !improves(&current, &next) {
            self.lifecycle
                .discard_replacement_segment(&payload.chunk_id, destination)
                .await?;
            return Ok(false);
        }
        replacement.placement_assessment = Some(next.clone());
        replacement.placement_repair_required =
            !(next.rack_protected && next.node_protected && next.disk_protected);
        self.lifecycle
            .replace_chunk_strip_range(
                &payload.chunk_id,
                chunk.modify_ts,
                u32::try_from(index).unwrap_or(u32::MAX),
                std::slice::from_ref(strip),
                std::slice::from_ref(&replacement),
                placement_operation_id(task.operation_id, strip.strip_sequence),
            )
            .await?;
        self.metrics.moved();
        Ok(!replacement.placement_repair_required)
    }

    async fn clear_marker(
        &self,
        chunk: &Chunk,
        index: usize,
        strip: &ChunkStrip,
        assessment: crowdb_protocol::chunkdb::rpc::PlacementAssessment,
        operation_id: ChunkId,
    ) -> Result<(), PlacementRepairError> {
        let mut replacement = strip.clone();
        replacement.placement_assessment = Some(assessment);
        replacement.placement_repair_required = false;
        self.lifecycle
            .replace_chunk_strip_range(
                &chunk
                    .id
                    .ok_or_else(|| PlacementRepairError::Payload("chunk has no ID".into()))?,
                chunk.modify_ts,
                u32::try_from(index).unwrap_or(u32::MAX),
                std::slice::from_ref(strip),
                std::slice::from_ref(&replacement),
                placement_operation_id(operation_id, strip.strip_sequence),
            )
            .await?;
        Ok(())
    }
}

impl TaskHandler for PlacementRepairTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_REPAIR_PLACEMENT
    }

    fn supports_version(&self, version: u16) -> bool {
        version == PLACEMENT_REPAIR_KIND_VERSION
    }

    fn execute<'a>(&'a self, task: &'a ChunkTaskValue) -> TaskFuture<'a> {
        Box::pin(async move {
            match self.execute_once(task).await {
                Ok(true) => {
                    self.metrics.completed();
                    TaskOutcome::Complete
                }
                Ok(false) => {
                    self.metrics.waiting();
                    TaskOutcome::Retry {
                        delay_ms: RETRY_DELAY_MS,
                        error_code: 40,
                        error: "topology cannot yet improve placement".into(),
                    }
                }
                Err(error) => {
                    self.metrics.failed();
                    TaskOutcome::Retry {
                        delay_ms: RETRY_DELAY_MS,
                        error_code: 41,
                        error: error.to_string(),
                    }
                }
            }
        })
    }
}

fn payload_for_strip(
    chunk_id: ChunkId,
    strip: &ChunkStrip,
) -> Result<PlacementRepairTaskPayload, PlacementRepairError> {
    let assessment = strip
        .placement_assessment
        .as_ref()
        .ok_or_else(|| PlacementRepairError::Payload("marked strip has no placement assessment".into()))?;
    Ok(PlacementRepairTaskPayload {
        chunk_id,
        strip_sequence: strip.strip_sequence,
        placement_priority: strip.placement_priority,
        repair_rack: !assessment.rack_protected,
        repair_node: !assessment.node_protected,
        repair_disk: !assessment.disk_protected,
    })
}

fn make_task(
    task_id: ChunkId,
    source_revision: u64,
    strip: &ChunkStrip,
    payload: &PlacementRepairTaskPayload,
    now_ms: u64,
) -> Result<ChunkTaskValue, PlacementRepairError> {
    let bytes = strip
        .strip
        .as_ref()
        .and_then(|strip| match strip {
            Strip::EcStrip(ec) => ec.segments.first(),
            Strip::MirrorStrip(_) => None,
        })
        .map_or(0, |segment| {
            u64::from(segment.unit_count) * u64::from(strip.unit_kb) * 1024
        });
    Ok(ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id,
        partition_id: payload.chunk_id,
        kind: TASK_KIND_REPAIR_PLACEMENT,
        kind_version: PLACEMENT_REPAIR_KIND_VERSION,
        state: ChunkTaskState::Pending,
        priority: u8::MAX - 1,
        revision: 1,
        operation_id: placement_operation_id(payload.chunk_id, payload.strip_sequence),
        source_revision,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        eligible_at_ms: now_ms,
        attempt: 0,
        max_attempts: u32::MAX,
        estimated_queue_bytes: bytes.saturating_mul(2),
        claim_owner: 0,
        claim_generation: 0,
        claim_deadline_ms: 0,
        last_error_code: 0,
        last_error: String::new(),
        payload: to_vec(payload).map_err(|error| PlacementRepairError::Payload(error.to_string()))?,
    })
}

fn decode_payload(bytes: &[u8]) -> Result<PlacementRepairTaskPayload, PlacementRepairError> {
    serde_json::from_slice(bytes).map_err(|error| PlacementRepairError::Payload(error.to_string()))
}

fn placement_task_id(chunk_id: ChunkId, strip_sequence: u32) -> ChunkId {
    ChunkId {
        high: chunk_id.high ^ 0x706c_6163_656d_656e,
        low: chunk_id.low ^ u64::from(strip_sequence),
    }
}

fn placement_operation_id(chunk_id: ChunkId, strip_sequence: u32) -> ChunkId {
    ChunkId {
        high: chunk_id.high ^ 0x7265_7061_6972_706c,
        low: chunk_id.low ^ u64::from(strip_sequence),
    }
}

fn select_source(
    snapshot: &crate::topology::TopologySnapshot,
    segments: &[crowdb_protocol::diskdb::rpc::Segment],
    loss_budget: u32,
) -> Option<usize> {
    let mut rack = std::collections::HashMap::<u64, u32>::new();
    let mut node = std::collections::HashMap::<u64, u32>::new();
    let mut disk = std::collections::HashMap::new();
    for segment in segments {
        let disk_id = segment.disk_id?;
        *disk.entry(disk_id).or_insert(0u32) += 1;
        let location = snapshot.disk_location(disk_id)?;
        *rack.entry(location.rack_id).or_insert(0u32) += 1;
        *node.entry(location.node_id).or_insert(0u32) += 1;
    }
    segments.iter().position(|segment| {
        segment.disk_id.is_some_and(|disk_id| {
            snapshot.disk_location(disk_id).is_some_and(|location| {
                rack.get(&location.rack_id).copied().unwrap_or(0) > loss_budget
                    || node.get(&location.node_id).copied().unwrap_or(0) > loss_budget
                    || disk.get(&disk_id).copied().unwrap_or(0) > loss_budget
            })
        })
    })
}

fn improves(
    current: &crowdb_protocol::chunkdb::rpc::PlacementAssessment,
    next: &crowdb_protocol::chunkdb::rpc::PlacementAssessment,
) -> bool {
    (next.rack_protected && !current.rack_protected)
        || (next.node_protected && !current.node_protected)
        || (next.disk_protected && !current.disk_protected)
        || (
            next.max_fragments_per_rack,
            next.max_fragments_per_node,
            next.max_fragments_per_disk,
        ) < (
            current.max_fragments_per_rack,
            current.max_fragments_per_node,
            current.max_fragments_per_disk,
        )
}
