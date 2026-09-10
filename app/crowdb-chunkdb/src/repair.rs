// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable admission and full-segment repair for read failures.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use crowdb_common::ec::{decode, EcScheme};
use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, CHUNK_TASK_SCHEMA_VERSION, TASK_KIND_REPAIR_STRIP,
};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, EcState, Strip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::conversion::io::ConversionDiskIo;
use crate::lifecycle::{LifecycleError, LifecycleHandler};
use crate::metrics::RepairMetrics;
use crate::task::executor::TaskFuture;
use crate::task::{TaskHandler, TaskOutcome, TaskStore, TaskStoreError};

pub const REPAIR_STRIP_TASK_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairStripTaskV1 {
    pub chunk_id: ChunkId,
    pub strip_sequence: u32,
    pub failed_segments: Vec<Segment>,
}

#[derive(Debug, thiserror::Error)]
pub enum RepairError {
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Store(#[from] TaskStoreError),
    #[error("repair task payload is invalid: {0}")]
    Payload(String),
}

/// Converts durable `unavailable_segments` markers into generic tasks.
pub struct RepairCoordinator {
    lifecycle: Arc<LifecycleHandler>,
    tasks: Arc<TaskStore>,
    wake: Option<Arc<tokio::sync::Notify>>,
    metrics: Option<Arc<RepairMetrics>>,
    scan_cursor: ArcSwapOption<ChunkId>,
}

impl RepairCoordinator {
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
    pub fn with_metrics(mut self, metrics: Arc<RepairMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub async fn admit_chunk(&self, chunk: &Chunk, now_ms: u64) -> Result<u64, RepairError> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| RepairError::Payload("chunk has no ID".into()))?;
        let mut accepted = 0u64;
        for strip in &chunk.strips {
            if strip.unavailable_segments.is_empty() {
                continue;
            }
            let mut failed_segments = strip.unavailable_segments.clone();
            failed_segments.sort_by_key(segment_identity);
            failed_segments.dedup();
            let task_id = repair_task_id(strip.strip_sequence, &failed_segments);
            let existing = self
                .tasks
                .get(&chunk_id, TASK_KIND_REPAIR_STRIP, &task_id)
                .await?;
            if existing.as_ref().is_some_and(|task| {
                matches!(
                    task.state,
                    ChunkTaskState::Pending | ChunkTaskState::Running | ChunkTaskState::RetryWait
                )
            }) {
                continue;
            }
            let payload = RepairStripTaskV1 {
                chunk_id,
                strip_sequence: strip.strip_sequence,
                failed_segments,
            };
            let shard_bytes = payload.failed_segments.first().map_or(0, |segment| {
                u64::from(segment.unit_count)
                    .saturating_mul(u64::from(strip.unit_kb))
                    .saturating_mul(1024)
            });
            let total_segments = match strip.strip.as_ref() {
                Some(Strip::MirrorStrip(mirror)) => mirror.segments.len(),
                Some(Strip::EcStrip(ec)) => ec.segments.len(),
                None => 0,
            };
            let io_count = total_segments.saturating_add(payload.failed_segments.len());
            let estimated_bytes = shard_bytes.saturating_mul(u64::try_from(io_count).unwrap_or(u64::MAX));
            let mut task = make_task(task_id, chunk.modify_ts, estimated_bytes, &payload, now_ms)?;
            if let Some(previous) = &existing {
                task.revision = previous.revision.saturating_add(1);
                task.created_at_ms = previous.created_at_ms;
                task.claim_generation = previous.claim_generation;
            }
            self.tasks.write_transition(existing.as_ref(), &task).await?;
            accepted = accepted.saturating_add(1);
        }
        if accepted != 0 {
            if let Some(metrics) = &self.metrics {
                metrics.admit(accepted);
            }
            if let Some(wake) = &self.wake {
                wake.notify_one();
            }
        }
        Ok(accepted)
    }

    pub async fn scan_batch(&self, max_chunks: u32, now_ms: u64) -> Result<u64, RepairError> {
        let limit = max_chunks.max(1);
        let start_after = self.scan_cursor.load_full();
        let chunks = self.lifecycle.list_chunks(start_after.as_deref(), limit).await?;
        if chunks.is_empty() {
            self.scan_cursor.store(None);
            return Ok(0);
        }
        if chunks.len() < usize::try_from(limit).unwrap_or(usize::MAX) {
            self.scan_cursor.store(None);
        } else if let Some(last) = chunks.last().and_then(|chunk| chunk.id) {
            self.scan_cursor.store(Some(Arc::new(last)));
        }
        let mut accepted = 0u64;
        for chunk in chunks {
            accepted = accepted.saturating_add(self.admit_chunk(&chunk, now_ms).await?);
        }
        Ok(accepted)
    }
}

pub struct RepairStripTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
    io: Arc<ConversionDiskIo>,
    memory: Arc<Semaphore>,
    memory_limit: usize,
    metrics: Arc<RepairMetrics>,
    permits: Arc<Semaphore>,
    allow_unsafe_placement: bool,
}

impl RepairStripTaskHandler {
    #[must_use]
    pub fn new(
        lifecycle: Arc<LifecycleHandler>,
        io: Arc<ConversionDiskIo>,
        memory_bytes: usize,
        max_concurrency: usize,
        allow_unsafe_placement: bool,
        metrics: Arc<RepairMetrics>,
    ) -> Self {
        metrics.set_memory_limit(memory_bytes);
        Self {
            lifecycle,
            io,
            memory: Arc::new(Semaphore::new(memory_bytes)),
            memory_limit: memory_bytes,
            metrics,
            permits: Arc::new(Semaphore::new(max_concurrency.max(1))),
            allow_unsafe_placement,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_once(&self, task: &ChunkTaskValue) -> Result<RepairStats, RepairRunError> {
        let payload = decode_payload(&task.payload)?;
        let chunk = self.lifecycle.query_chunk(&payload.chunk_id).await?;
        let Some((strip_index, strip)) = chunk
            .strips
            .iter()
            .enumerate()
            .find(|(_, strip)| strip.strip_sequence == payload.strip_sequence)
        else {
            return Ok(RepairStats::default());
        };
        let segments = strip_segments(strip)?;
        let unavailable: Vec<_> = strip
            .unavailable_segments
            .iter()
            .copied()
            .filter(|segment| segments.contains(segment))
            .collect();
        if unavailable.is_empty() {
            return Ok(RepairStats::default());
        }
        let unit_bytes = u64::from(strip.unit_kb)
            .checked_mul(1024)
            .ok_or_else(|| RepairRunError::Permanent("unit size overflows".into()))?;
        let shard_bytes = segment_size(&segments[0], unit_bytes)?;
        let required = shard_bytes
            .checked_mul(u64::try_from(segments.len().saturating_add(1)).unwrap_or(u64::MAX))
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| RepairRunError::Permanent("repair memory calculation overflows".into()))?;
        if required > self.memory_limit {
            return Err(RepairRunError::Retry(format!(
                "repair requires {required} bytes but the limit is {}",
                self.memory_limit
            )));
        }
        let permits = u32::try_from(required)
            .map_err(|_| RepairRunError::Permanent("repair memory exceeds semaphore".into()))?;
        let _reservation = self
            .memory
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| RepairRunError::Retry("repair memory budget closed".into()))?;
        self.metrics.reserve_memory(required);
        let _memory_metrics = RepairMemoryGuard {
            metrics: Arc::clone(&self.metrics),
            bytes: required,
        };

        let recovered = match strip.strip.as_ref() {
            Some(Strip::MirrorStrip(_)) => self.recover_mirror(strip, &segments).await?,
            Some(Strip::EcStrip(ec)) => self.recover_ec(strip, ec, &segments).await?,
            None => return Err(RepairRunError::Permanent("strip has no body".into())),
        };
        if !recovered.new_failures.is_empty() {
            self.persist_new_failures(
                &chunk,
                strip_index,
                strip,
                &recovered.new_failures,
                task.operation_id,
            )
            .await?;
            return Err(RepairRunError::Retry("new failed segments were persisted".into()));
        }

        let mut replacement = strip.clone();
        let mut surviving: Vec<_> = segments
            .iter()
            .copied()
            .filter(|segment| !unavailable.contains(segment))
            .collect();
        // Even when node anti-affinity is relaxed for an undersized test
        // cluster, never place two shards from one strip on the same disk.
        let mut excluded: Vec<_> = segments.iter().filter_map(|segment| segment.disk_id).collect();
        for failed in &unavailable {
            let index = segments
                .iter()
                .position(|segment| segment == failed)
                .ok_or_else(|| RepairRunError::Permanent("failed segment disappeared".into()))?;
            let new_segment = self
                .lifecycle
                .allocate_repair_segment(
                    &payload.chunk_id,
                    failed,
                    &surviving,
                    &excluded,
                    self.allow_unsafe_placement,
                )
                .await?;
            self.io
                .write_segment(&new_segment, unit_bytes, recovered.shards[index].clone())
                .await
                .map_err(|error| RepairRunError::Retry(error.to_string()))?;
            self.io
                .fsync_segment(&new_segment)
                .await
                .map_err(|error| RepairRunError::Retry(error.to_string()))?;
            replace_segment(&mut replacement, failed, new_segment)?;
            surviving.push(new_segment);
            if let Some(disk_id) = new_segment.disk_id {
                excluded.push(disk_id);
            }
        }
        replacement
            .unavailable_segments
            .retain(|segment| !unavailable.contains(segment));
        self.lifecycle
            .replace_chunk_strip_range(
                &payload.chunk_id,
                chunk.modify_ts,
                u32::try_from(strip_index).unwrap_or(u32::MAX),
                std::slice::from_ref(strip),
                std::slice::from_ref(&replacement),
                task.operation_id,
            )
            .await?;
        Ok(RepairStats {
            segments: u64::try_from(unavailable.len()).unwrap_or(u64::MAX),
            bytes: shard_bytes.saturating_mul(u64::try_from(unavailable.len()).unwrap_or(u64::MAX)),
        })
    }

    async fn recover_mirror(
        &self,
        strip: &ChunkStrip,
        segments: &[Segment],
    ) -> Result<RecoveredShards, RepairRunError> {
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut source = None;
        let mut new_failures = Vec::new();
        for segment in segments {
            if strip.unavailable_segments.contains(segment) {
                continue;
            }
            match self.io.read_segment(segment, unit_bytes).await {
                Ok(data) => {
                    source = Some(data);
                    break;
                }
                Err(_) => push_unique(&mut new_failures, *segment),
            }
        }
        if !new_failures.is_empty() {
            return Ok(RecoveredShards {
                shards: Vec::new(),
                new_failures,
            });
        }
        let source = source.ok_or_else(|| RepairRunError::Retry("no readable mirror remains".into()))?;
        Ok(RecoveredShards {
            shards: vec![source; segments.len()],
            new_failures,
        })
    }

    async fn recover_ec(
        &self,
        strip: &ChunkStrip,
        ec: &crowdb_protocol::chunkdb::rpc::EcStrip,
        segments: &[Segment],
    ) -> Result<RecoveredShards, RepairRunError> {
        if ec.ec_state != EcState::Parity as i32 {
            return Err(RepairRunError::Retry("EC parity is incomplete".into()));
        }
        let scheme = EcScheme::new(
            usize::try_from(ec.data_num).unwrap_or(usize::MAX),
            usize::try_from(ec.code_num).unwrap_or(usize::MAX),
        );
        if scheme.data_num == 0 || scheme.code_num == 0 || segments.len() != scheme.total_blocks() {
            return Err(RepairRunError::Permanent("invalid EC geometry".into()));
        }
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut shards = vec![None; segments.len()];
        let mut new_failures = Vec::new();
        for (index, segment) in segments.iter().enumerate() {
            if strip.unavailable_segments.contains(segment) {
                continue;
            }
            match self.io.read_segment(segment, unit_bytes).await {
                Ok(data) => shards[index] = Some(data.to_vec()),
                Err(_) => push_unique(&mut new_failures, *segment),
            }
        }
        if !new_failures.is_empty() {
            return Ok(RecoveredShards {
                shards: Vec::new(),
                new_failures,
            });
        }
        let missing = shards.iter().filter(|shard| shard.is_none()).count();
        if missing > scheme.code_num {
            return Err(RepairRunError::Retry(format!(
                "EC strip has {missing} unavailable shards and {} parity shards",
                scheme.code_num
            )));
        }
        let shards = decode(scheme, shards).map_err(|error| RepairRunError::Retry(error.to_string()))?;
        Ok(RecoveredShards {
            shards: shards.into_iter().map(Bytes::from).collect(),
            new_failures,
        })
    }

    async fn persist_new_failures(
        &self,
        chunk: &Chunk,
        strip_index: usize,
        strip: &ChunkStrip,
        failures: &[Segment],
        operation_id: ChunkId,
    ) -> Result<(), RepairRunError> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| RepairRunError::Permanent("chunk has no ID".into()))?;
        let mut replacement = strip.clone();
        for segment in failures {
            push_unique(&mut replacement.unavailable_segments, *segment);
        }
        replacement.unavailable_segments.sort_by_key(segment_identity);
        self.lifecycle
            .replace_chunk_strip_range(
                &chunk_id,
                chunk.modify_ts,
                u32::try_from(strip_index).unwrap_or(u32::MAX),
                std::slice::from_ref(strip),
                std::slice::from_ref(&replacement),
                failure_operation_id(operation_id, strip.strip_sequence),
            )
            .await?;
        Ok(())
    }
}

impl TaskHandler for RepairStripTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_REPAIR_STRIP
    }

    fn supports_version(&self, version: u16) -> bool {
        version == REPAIR_STRIP_TASK_VERSION
    }

    fn execute<'a>(&'a self, task: &'a ChunkTaskValue) -> TaskFuture<'a> {
        Box::pin(async move {
            let Ok(_permit) = self.permits.acquire().await else {
                return TaskOutcome::Retry {
                    delay_ms: 5_000,
                    error_code: 22,
                    error: "repair executor stopped".into(),
                };
            };
            self.metrics.start_attempt();
            match self.execute_once(task).await {
                Ok(stats) => {
                    self.metrics.finish_attempt(true, stats.segments, stats.bytes);
                    TaskOutcome::Complete
                }
                Err(RepairRunError::Permanent(error)) => {
                    self.metrics.finish_attempt(false, 0, 0);
                    TaskOutcome::Fail {
                        error_code: 20,
                        error,
                    }
                }
                Err(error) => {
                    self.metrics.finish_attempt(false, 0, 0);
                    TaskOutcome::Retry {
                        delay_ms: 5_000,
                        error_code: 21,
                        error: error.to_string(),
                    }
                }
            }
        })
    }
}

struct RecoveredShards {
    shards: Vec<Bytes>,
    new_failures: Vec<Segment>,
}

#[derive(Default)]
struct RepairStats {
    segments: u64,
    bytes: u64,
}

struct RepairMemoryGuard {
    metrics: Arc<RepairMetrics>,
    bytes: usize,
}

impl Drop for RepairMemoryGuard {
    fn drop(&mut self) {
        self.metrics.release_memory(self.bytes);
    }
}

#[derive(Debug, thiserror::Error)]
enum RepairRunError {
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Repair(#[from] RepairError),
    #[error("{0}")]
    Retry(String),
    #[error("{0}")]
    Permanent(String),
}

fn make_task(
    task_id: ChunkId,
    source_revision: u64,
    estimated_queue_bytes: u64,
    payload: &RepairStripTaskV1,
    now_ms: u64,
) -> Result<ChunkTaskValue, RepairError> {
    Ok(ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id,
        partition_id: payload.chunk_id,
        kind: TASK_KIND_REPAIR_STRIP,
        kind_version: REPAIR_STRIP_TASK_VERSION,
        state: ChunkTaskState::Pending,
        // Correctness repair outranks conversion, which only reclaims space.
        priority: u8::MAX,
        revision: 1,
        operation_id: repair_operation_id(payload.chunk_id, task_id),
        source_revision,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        eligible_at_ms: 0,
        attempt: 0,
        max_attempts: u32::MAX,
        estimated_queue_bytes,
        claim_owner: 0,
        claim_generation: 0,
        claim_deadline_ms: 0,
        last_error_code: 0,
        last_error: String::new(),
        payload: encode_payload(payload)?,
    })
}

fn strip_segments(strip: &ChunkStrip) -> Result<Vec<Segment>, RepairRunError> {
    match strip.strip.as_ref() {
        Some(Strip::MirrorStrip(mirror)) if !mirror.segments.is_empty() => Ok(mirror.segments.clone()),
        Some(Strip::EcStrip(ec)) if !ec.segments.is_empty() => Ok(ec.segments.clone()),
        _ => Err(RepairRunError::Permanent("strip has no segments".into())),
    }
}

fn replace_segment(
    strip: &mut ChunkStrip,
    old: &Segment,
    replacement: Segment,
) -> Result<(), RepairRunError> {
    let segments = match strip.strip.as_mut() {
        Some(Strip::MirrorStrip(mirror)) => &mut mirror.segments,
        Some(Strip::EcStrip(ec)) => &mut ec.segments,
        None => return Err(RepairRunError::Permanent("strip has no body".into())),
    };
    let segment = segments
        .iter_mut()
        .find(|segment| *segment == old)
        .ok_or_else(|| RepairRunError::Permanent("failed segment disappeared".into()))?;
    *segment = replacement;
    Ok(())
}

fn segment_size(segment: &Segment, unit_bytes: u64) -> Result<u64, RepairRunError> {
    u64::from(segment.unit_count)
        .checked_mul(unit_bytes)
        .ok_or_else(|| RepairRunError::Permanent("segment size overflows".into()))
}

fn push_unique(segments: &mut Vec<Segment>, segment: Segment) {
    if !segments.contains(&segment) {
        segments.push(segment);
    }
}

fn segment_identity(segment: &Segment) -> (u64, u64, u32, u64, u64) {
    let disk = segment.disk_id.unwrap_or_default();
    (
        disk.high,
        disk.low,
        segment.zone_index,
        segment.unit_offset,
        segment.allocation_ts,
    )
}

fn repair_task_id(strip_sequence: u32, segments: &[Segment]) -> ChunkId {
    let mut high = 0x1110_0000_0000_0002 ^ u64::from(strip_sequence);
    let mut low = 0xcbf2_9ce4_8422_2325_u64;
    for segment in segments {
        let disk = segment.disk_id.unwrap_or_default();
        high = high.rotate_left(11) ^ disk.high ^ segment.unit_offset;
        low = low.rotate_left(17) ^ disk.low ^ segment.allocation_ts ^ u64::from(segment.zone_index);
    }
    ChunkId { high, low }
}

fn repair_operation_id(chunk_id: ChunkId, task_id: ChunkId) -> ChunkId {
    ChunkId {
        high: chunk_id.high ^ task_id.low.rotate_left(7) ^ 0x1110_0a11_0000_0002,
        low: chunk_id.low ^ task_id.high.rotate_left(23) ^ 0x1110_0b11_0000_0002,
    }
}

fn failure_operation_id(operation_id: ChunkId, strip_sequence: u32) -> ChunkId {
    ChunkId {
        high: operation_id.high ^ 0xfa11_ed00_0000_0002,
        low: operation_id.low ^ u64::from(strip_sequence),
    }
}

pub fn encode_payload(payload: &RepairStripTaskV1) -> Result<Vec<u8>, RepairError> {
    bincode::serialize(payload).map_err(|error| RepairError::Payload(error.to_string()))
}

pub fn decode_payload(bytes: &[u8]) -> Result<RepairStripTaskV1, RepairError> {
    bincode::deserialize(bytes).map_err(|error| RepairError::Payload(error.to_string()))
}
