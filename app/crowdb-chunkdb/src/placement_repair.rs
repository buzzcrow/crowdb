// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable reconciliation and one-fragment repair for degraded EC placement.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, PlacementRepairTaskPayload, RepairTargetCheckpoint, RepairTargetPhase,
    CHUNK_TASK_SCHEMA_VERSION, PLACEMENT_REPAIR_KIND_VERSION, TASK_KIND_REPAIR_PLACEMENT,
};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, Strip};
use crowdb_protocol::common::ChunkId;
use serde_json::to_vec;

use crate::allocator::{assess_physical_placement, AllocError};
use crate::conversion::io::ConversionDiskIo;
use crate::lifecycle::{LifecycleError, LifecycleHandler};
use crate::metrics::PlacementMetrics;
use crate::selector::PlacementError;
use crate::task::executor::TaskFuture;
use crate::task::{TaskHandler, TaskManager, TaskOutcome, TaskStore, TaskStoreError};

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
/// publishes a move that preserves every current protection guarantee.
pub struct PlacementRepairTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
    task_manager: Arc<TaskManager>,
    io: Arc<ConversionDiskIo>,
    metrics: Arc<PlacementMetrics>,
}

impl PlacementRepairTaskHandler {
    #[must_use]
    pub fn new(
        lifecycle: Arc<LifecycleHandler>,
        task_manager: Arc<TaskManager>,
        io: Arc<ConversionDiskIo>,
        metrics: Arc<PlacementMetrics>,
    ) -> Self {
        Self {
            lifecycle,
            task_manager,
            io,
            metrics,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_once(&self, task: &ChunkTaskValue) -> Result<bool, PlacementRepairError> {
        let mut payload = decode_payload(&task.payload)?;
        let chunk = self.lifecycle.query_chunk(&payload.chunk_id).await?;
        let Some((index, strip)) = chunk
            .strips
            .iter()
            .enumerate()
            .find(|(_, strip)| strip.strip_sequence == payload.strip_sequence)
        else {
            return Ok(true);
        };
        let Some(Strip::EcStrip(ec)) = strip.strip.as_ref() else {
            return Ok(true);
        };
        self.confirm_published_target(task, &mut payload, &ec.segments)
            .await?;
        // A placement task moves at most one fragment per execution. Once the
        // published target is confirmed, it must not be reused as the next
        // move's destination; persist that retirement before selecting a new
        // source so a retry or restart starts a fresh move.
        if payload
            .target
            .as_ref()
            .is_some_and(|target| target.phase == RepairTargetPhase::Confirmed)
        {
            payload.target = None;
            self.checkpoint(task, &payload).await?;
        }
        if !strip.placement_repair_required {
            return Ok(true);
        }
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
        let source_index = payload
            .target
            .as_ref()
            .and_then(|target| ec.segments.iter().position(|segment| *segment == target.source))
            .or_else(|| select_source(&snapshot, &ec.segments, ec.code_num));
        let Some(source_index) = source_index else {
            return Ok(false);
        };
        let source = ec.segments[source_index];
        let survivors: Vec<_> = ec
            .segments
            .iter()
            .enumerate()
            .filter_map(|(position, segment)| (position != source_index).then_some(*segment))
            .collect();
        let excluded = over_budget_disks(&ec.segments, ec.code_num);
        let destination = if let Some(target) = &payload.target {
            target.destination
        } else {
            let (exclude_racks, exclude_nodes) = over_budget_domains(&snapshot, &ec.segments, ec.code_num);
            let Some(target_disk_group) = select_target_disk_group(
                &snapshot,
                &ec.segments,
                &excluded,
                &exclude_racks,
                &exclude_nodes,
                strip.placement_priority,
            ) else {
                return Ok(false);
            };
            let destination = self
                .lifecycle
                .allocate_placement_replacement_segment(
                    &payload.chunk_id,
                    &source,
                    &survivors,
                    &excluded,
                    &exclude_racks,
                    &exclude_nodes,
                    target_disk_group,
                )
                .await?;
            payload.target = Some(RepairTargetCheckpoint {
                source,
                destination,
                phase: RepairTargetPhase::Allocated,
            });
            self.checkpoint(task, &payload).await?;
            destination
        };
        let unit_bytes = u64::from(strip.unit_kb).saturating_mul(1024);
        if payload
            .target
            .as_ref()
            .is_some_and(|target| target.phase == RepairTargetPhase::Allocated)
        {
            let bytes = self
                .io
                .read_segment(&source, unit_bytes)
                .await
                .map_err(|error| PlacementRepairError::Payload(error.to_string()))?;
            self.io
                .write_segment(&destination, unit_bytes, bytes)
                .await
                .map_err(|error| PlacementRepairError::Payload(error.to_string()))?;
            self.io
                .fsync_segment(&destination)
                .await
                .map_err(|error| PlacementRepairError::Payload(error.to_string()))?;
            if let Some(target) = &mut payload.target {
                target.phase = RepairTargetPhase::Copied;
            }
            self.checkpoint(task, &payload).await?;
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
        if weakens_protection(&current, &next) {
            self.lifecycle
                .discard_replacement_segment(&payload.chunk_id, destination)
                .await?;
            payload.target = None;
            self.checkpoint(task, &payload).await?;
            return Ok(false);
        }
        replacement.placement_assessment = Some(next.clone());
        replacement.placement_repair_required =
            !(next.rack_protected && next.node_protected && next.disk_protected);
        let replacement_segments = replacement_ec.segments.clone();
        self.lifecycle
            .publish_tentative_chunk_strip_range(
                &payload.chunk_id,
                chunk.modify_ts,
                u32::try_from(index).unwrap_or(u32::MAX),
                std::slice::from_ref(strip),
                std::slice::from_ref(&replacement),
                placement_operation_id(task.operation_id, strip.strip_sequence),
            )
            .await?;
        if let Some(target) = &mut payload.target {
            target.phase = RepairTargetPhase::Published;
        }
        self.checkpoint(task, &payload).await?;
        self.confirm_published_target(task, &mut payload, &replacement_segments)
            .await?;
        self.metrics.moved();
        Ok(!replacement.placement_repair_required)
    }

    async fn checkpoint(
        &self,
        task: &ChunkTaskValue,
        payload: &PlacementRepairTaskPayload,
    ) -> Result<(), PlacementRepairError> {
        self.task_manager
            .checkpoint_payload(
                task,
                to_vec(payload).map_err(|error| PlacementRepairError::Payload(error.to_string()))?,
                now_ms(),
            )
            .await
            .map_err(|error| PlacementRepairError::Payload(error.to_string()))?;
        Ok(())
    }

    async fn confirm_published_target(
        &self,
        task: &ChunkTaskValue,
        payload: &mut PlacementRepairTaskPayload,
        segments: &[crowdb_protocol::diskdb::rpc::Segment],
    ) -> Result<(), PlacementRepairError> {
        let Some(target) = payload.target.as_ref() else {
            return Ok(());
        };
        let became_published = target.phase != RepairTargetPhase::Confirmed
            && segments.contains(&target.destination)
            && !segments.contains(&target.source);
        if became_published {
            if let Some(target) = &mut payload.target {
                target.phase = RepairTargetPhase::Published;
            }
            self.checkpoint(task, payload).await?;
        }
        if payload
            .target
            .as_ref()
            .is_some_and(|target| target.phase == RepairTargetPhase::Published)
        {
            let Some(destination) = payload.target.as_ref().map(|target| target.destination) else {
                return Ok(());
            };
            self.lifecycle
                .confirm_tentative_segments(vec![destination])
                .await?;
            if let Some(target) = &mut payload.target {
                target.phase = RepairTargetPhase::Confirmed;
                self.checkpoint(task, payload).await?;
            }
        }
        Ok(())
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
                Err(error) if topology_waiting(&error) => {
                    self.metrics.waiting();
                    TaskOutcome::Retry {
                        delay_ms: RETRY_DELAY_MS,
                        error_code: 40,
                        error: error.to_string(),
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

/// Placement exclusions can leave no safe destination until topology changes.
/// Those errors are a retryable waiting condition, not a failed repair attempt.
fn topology_waiting(error: &PlacementRepairError) -> bool {
    matches!(
        error,
        PlacementRepairError::Lifecycle(LifecycleError::Allocation(AllocError::Placement(
            PlacementError::InsufficientNodes { .. }
                | PlacementError::InsufficientCapacity
                | PlacementError::NoHealthyDiskGroups
        )))
    )
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
        target: None,
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

pub(crate) fn decode_payload(bytes: &[u8]) -> Result<PlacementRepairTaskPayload, PlacementRepairError> {
    serde_json::from_slice(bytes).map_err(|error| PlacementRepairError::Payload(error.to_string()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
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

fn over_budget_domains(
    snapshot: &crate::topology::TopologySnapshot,
    segments: &[crowdb_protocol::diskdb::rpc::Segment],
    loss_budget: u32,
) -> (Vec<u64>, Vec<u64>) {
    let mut racks = std::collections::HashMap::<u64, u32>::new();
    let mut nodes = std::collections::HashMap::<u64, u32>::new();
    for segment in segments {
        let Some(disk_id) = segment.disk_id else {
            continue;
        };
        let Some(location) = snapshot.disk_location(disk_id) else {
            continue;
        };
        *racks.entry(location.rack_id).or_default() += 1;
        *nodes.entry(location.node_id).or_default() += 1;
    }
    (
        racks
            .into_iter()
            .filter_map(|(rack_id, count)| (count > loss_budget).then_some(rack_id))
            .collect(),
        nodes
            .into_iter()
            .filter_map(|(node_id, count)| (count > loss_budget).then_some(node_id))
            .collect(),
    )
}

fn over_budget_disks(
    segments: &[crowdb_protocol::diskdb::rpc::Segment],
    loss_budget: u32,
) -> Vec<crowdb_protocol::common::DiskId> {
    let mut disks = std::collections::HashMap::new();
    for disk_id in segments.iter().filter_map(|segment| segment.disk_id) {
        *disks.entry(disk_id).or_insert(0u32) += 1;
    }
    disks
        .into_iter()
        .filter_map(|(disk_id, count)| (count >= loss_budget).then_some(disk_id))
        .collect()
}

fn select_target_disk_group(
    snapshot: &crate::topology::TopologySnapshot,
    segments: &[crowdb_protocol::diskdb::rpc::Segment],
    excluded_disks: &[crowdb_protocol::common::DiskId],
    exclude_racks: &[u64],
    exclude_nodes: &[u64],
    priority: i32,
) -> Option<u64> {
    let mut racks = std::collections::HashMap::<u64, u32>::new();
    let mut nodes = std::collections::HashMap::<u64, u32>::new();
    let mut groups = std::collections::HashMap::<u64, u32>::new();
    for segment in segments {
        let disk_id = segment.disk_id?;
        let location = snapshot.disk_location(disk_id)?;
        *racks.entry(location.rack_id).or_default() += 1;
        *nodes.entry(location.node_id).or_default() += 1;
        *groups.entry(location.disk_group_id).or_default() += 1;
    }
    snapshot
        .healthy_disk_groups()
        .into_iter()
        .filter(|group| {
            !exclude_racks.contains(&group.rack_id)
                && !exclude_nodes.contains(&group.node_id)
                && group
                    .value
                    .disk_ids
                    .iter()
                    .any(|disk| !excluded_disks.contains(disk))
        })
        .min_by_key(|group| {
            let rack = racks.get(&group.rack_id).copied().unwrap_or(0);
            let node = nodes.get(&group.node_id).copied().unwrap_or(0);
            let disk_group = groups.get(&group.dg_id).copied().unwrap_or(0);
            let capacity = snapshot.capacity_score(group.dg_id, 0);
            if priority == crowdb_protocol::chunkdb::rpc::PlacementPriority::NodeFirst as i32 {
                (node, rack, disk_group, capacity, group.node_id, group.dg_id)
            } else {
                (rack, node, disk_group, capacity, group.node_id, group.dg_id)
            }
        })
        .map(|group| group.dg_id)
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
