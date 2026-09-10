// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Mirror-to-EC task payload and foreground no-reread coordination.

pub mod io;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use crowdb_common::ec::{encode_parity_from_shards, EcScheme};
use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, CHUNK_TASK_SCHEMA_VERSION, TASK_KIND_MIRROR_TO_EC,
};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip, EcState, Strip};
use crowdb_protocol::common::ChunkId;
use serde::{Deserialize, Serialize};

use crate::lifecycle::ReservationRecovery;
use crate::lifecycle::{LifecycleError, LifecycleHandler};
use crate::metrics::ConversionMetrics;
use crate::task::executor::TaskFuture;
use crate::task::{TaskHandler, TaskOutcome, TaskStore, TaskStoreError};

use self::io::ConversionDiskIo;

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
    wake: Option<Arc<tokio::sync::Notify>>,
    data_num: u32,
    code_num: u32,
    min_mirror_strips: u32,
    min_age_ms: u64,
    scan_cursor: ArcSwapOption<ChunkId>,
    reservation_scan_cursor: ArcSwapOption<(ChunkId, ChunkId)>,
}

pub struct MirrorToEcTaskHandler {
    lifecycle: Arc<LifecycleHandler>,
    tasks: Arc<TaskStore>,
    io: Arc<ConversionDiskIo>,
    metrics: Arc<ConversionMetrics>,
    bandwidth: BandwidthLimiter,
    permits: Arc<tokio::sync::Semaphore>,
}

struct BandwidthLimiter {
    bytes_per_second: u64,
    epoch: Instant,
    next_available_ns: AtomicU64,
}

impl BandwidthLimiter {
    fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second: bytes_per_second.max(1),
            epoch: Instant::now(),
            next_available_ns: AtomicU64::new(0),
        }
    }

    async fn acquire(&self, bytes: u64) {
        let duration_ns = bytes
            .saturating_mul(1_000_000_000)
            .saturating_add(self.bytes_per_second - 1)
            / self.bytes_per_second;
        let now_ns = u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let start_ns = loop {
            let current = self.next_available_ns.load(Ordering::Acquire);
            let start = current.max(now_ns);
            let end = start.saturating_add(duration_ns);
            if self
                .next_available_ns
                .compare_exchange_weak(current, end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break start;
            }
        };
        if start_ns > now_ns {
            tokio::time::sleep(std::time::Duration::from_nanos(start_ns - now_ns)).await;
        }
    }
}

impl MirrorToEcTaskHandler {
    #[must_use]
    pub fn new(
        lifecycle: Arc<LifecycleHandler>,
        tasks: Arc<TaskStore>,
        io: Arc<ConversionDiskIo>,
        metrics: Arc<ConversionMetrics>,
        max_bandwidth_mbps: u64,
        max_concurrency: usize,
    ) -> Self {
        Self {
            lifecycle,
            tasks,
            io,
            metrics,
            bandwidth: BandwidthLimiter::new(max_bandwidth_mbps.saturating_mul(1024 * 1024)),
            permits: Arc::new(tokio::sync::Semaphore::new(max_concurrency.max(1))),
        }
    }

    async fn execute_once(&self, task: &ChunkTaskValue) -> Result<(), ConversionRunError> {
        let mut current = self
            .tasks
            .get(&task.partition_id, task.kind, &task.task_id)
            .await?
            .ok_or(ConversionRunError::Conflict)?;
        if current.state != ChunkTaskState::Running
            || current.claim_owner != task.claim_owner
            || current.claim_generation != task.claim_generation
        {
            return Err(ConversionRunError::Conflict);
        }
        let mut payload = decode_payload(&current.payload)?;
        let chunk = self.lifecycle.query_chunk(&payload.chunk_id).await?;
        if installed(&chunk, &payload) {
            return Ok(());
        }
        if validate_source(
            &chunk,
            payload.expected_modify_ts,
            payload.start_index,
            &payload.old_strips,
        )
        .is_err()
        {
            if let Some(start_index) = locate_source(&chunk, &payload.old_strips) {
                payload.expected_modify_ts = chunk.modify_ts;
                payload.start_index = start_index;
                current = self.checkpoint(&current, &payload).await?;
            } else {
                if payload.replacement_strip.is_some() {
                    self.abandon_replacement(&current, &mut payload).await?;
                }
                return Err(ConversionRunError::Conflict);
            }
        }
        if payload.replacement_strip.is_none() {
            let replacement = self
                .lifecycle
                .allocate_conversion_strip(
                    &payload.chunk_id,
                    &payload.old_strips,
                    payload.data_num,
                    payload.code_num,
                )
                .await?;
            payload.replacement_strip = Some(replacement);
            payload.phase = MirrorToEcPhase::Allocated;
            current = self.checkpoint(&current, &payload).await?;
        }

        let replacement = match self.encode_and_write(&payload).await {
            Ok(replacement) => replacement,
            Err(error) => {
                self.abandon_replacement(&current, &mut payload).await?;
                return Err(error);
            }
        };
        payload.replacement_strip = Some(replacement.clone());
        payload.phase = MirrorToEcPhase::Durable;
        let _durable = self.checkpoint(&current, &payload).await?;
        self.lifecycle
            .replace_chunk_strip_range(
                &payload.chunk_id,
                payload.expected_modify_ts,
                payload.start_index,
                &payload.old_strips,
                std::slice::from_ref(&replacement),
                current.operation_id,
            )
            .await?;
        Ok(())
    }

    async fn encode_and_write(&self, payload: &MirrorToEcTaskV1) -> Result<ChunkStrip, ConversionRunError> {
        let (planned_read_bytes, planned_write_bytes, _) = io_accounting(payload);
        self.bandwidth
            .acquire(planned_read_bytes.saturating_add(planned_write_bytes))
            .await;
        let mut data = Vec::with_capacity(payload.old_strips.len());
        for strip in &payload.old_strips {
            data.push(self.read_mirror(strip).await?);
        }
        let refs: Vec<&[u8]> = data.iter().map(Bytes::as_ref).collect();
        let parity = encode_parity_from_shards(
            EcScheme::new(
                usize::try_from(payload.data_num).unwrap_or(usize::MAX),
                usize::try_from(payload.code_num).unwrap_or(usize::MAX),
            ),
            &refs,
        )
        .map_err(|error| ConversionRunError::Retry(error.to_string()))?;
        let mut replacement = payload
            .replacement_strip
            .clone()
            .ok_or(ConversionRunError::Conflict)?;
        let Some(Strip::EcStrip(ec)) = &replacement.strip else {
            return Err(ConversionRunError::Conflict);
        };
        let mut shards = data;
        shards.extend(parity.into_iter().map(Bytes::from));
        if ec.segments.len() != shards.len() {
            return Err(ConversionRunError::Conflict);
        }
        let unit_bytes = u64::from(replacement.unit_kb) * 1024;
        self.write_and_sync(&ec.segments, unit_bytes, shards).await?;
        let Some(Strip::EcStrip(ec)) = &mut replacement.strip else {
            unreachable!("replacement type already checked");
        };
        ec.ec_state = EcState::Parity as i32;
        Ok(replacement)
    }

    async fn abandon_replacement(
        &self,
        current: &ChunkTaskValue,
        payload: &mut MirrorToEcTaskV1,
    ) -> Result<(), ConversionRunError> {
        // Clear the durable task reference first. DiskDB's tentative-block
        // scanner reclaims the now-unreferenced allocation; freeing here
        // could race a lease heartbeat that still carries the older payload.
        payload
            .replacement_strip
            .take()
            .ok_or(ConversionRunError::Conflict)?;
        payload.phase = MirrorToEcPhase::Discovered;
        self.checkpoint(current, payload).await?;
        Ok(())
    }

    async fn write_and_sync(
        &self,
        segments: &[crowdb_protocol::diskdb::rpc::Segment],
        unit_bytes: u64,
        shards: Vec<Bytes>,
    ) -> Result<(), ConversionRunError> {
        let writes = segments
            .iter()
            .zip(shards)
            .map(|(segment, shard)| self.io.write_segment(segment, unit_bytes, shard));
        for result in futures::future::join_all(writes).await {
            result.map_err(|error| ConversionRunError::Retry(error.to_string()))?;
        }
        let syncs = segments.iter().map(|segment| self.io.fsync_segment(segment));
        for result in futures::future::join_all(syncs).await {
            result.map_err(|error| ConversionRunError::Retry(error.to_string()))?;
        }
        Ok(())
    }

    async fn checkpoint(
        &self,
        previous: &ChunkTaskValue,
        payload: &MirrorToEcTaskV1,
    ) -> Result<ChunkTaskValue, ConversionRunError> {
        let stored = self
            .tasks
            .get(&previous.partition_id, previous.kind, &previous.task_id)
            .await?
            .ok_or(ConversionRunError::Conflict)?;
        if stored.state != ChunkTaskState::Running
            || stored.claim_owner != previous.claim_owner
            || stored.claim_generation != previous.claim_generation
        {
            return Err(ConversionRunError::Conflict);
        }
        let mut next = stored.clone();
        next.revision = next.revision.saturating_add(1);
        next.updated_at_ms = unix_time_ms();
        next.payload = encode_payload(payload)?;
        self.tasks.write_transition(Some(&stored), &next).await?;
        Ok(next)
    }

    async fn read_mirror(&self, strip: &ChunkStrip) -> Result<Bytes, ConversionRunError> {
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(ConversionRunError::Conflict);
        };
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let mut last_error = "mirror has no readable replica".to_string();
        for segment in &mirror.segments {
            if strip.unavailable_segments.contains(segment) {
                continue;
            }
            match self.io.read_segment(segment, unit_bytes).await {
                Ok(data) => return Ok(data),
                Err(error) => last_error = error.to_string(),
            }
        }
        Err(ConversionRunError::Retry(last_error))
    }
}

impl TaskHandler for MirrorToEcTaskHandler {
    fn kind(&self) -> u16 {
        TASK_KIND_MIRROR_TO_EC
    }

    fn supports_version(&self, version: u16) -> bool {
        version == MIRROR_TO_EC_TASK_VERSION
    }

    fn execute<'a>(&'a self, task: &'a ChunkTaskValue) -> TaskFuture<'a> {
        Box::pin(async move {
            let Ok(_permit) = self.permits.acquire().await else {
                return TaskOutcome::Retry {
                    delay_ms: 100,
                    error_code: 12,
                    error: "conversion executor stopped".into(),
                };
            };
            let accounting =
                decode_payload(&task.payload).map_or((0, 0, 0), |payload| io_accounting(&payload));
            self.metrics.start_attempt();
            let result = self.execute_once(task).await;
            self.metrics
                .finish_attempt(result.is_ok(), accounting.0, accounting.1, accounting.2);
            match result {
                Ok(()) => TaskOutcome::Complete,
                Err(ConversionRunError::Conflict) => {
                    tracing::warn!(task_id = ?task.task_id, "conversion source changed permanently");
                    TaskOutcome::Fail {
                        error_code: 10,
                        error: "conversion source changed".into(),
                    }
                }
                Err(error) => {
                    tracing::warn!(task_id = ?task.task_id, error = %error, "conversion attempt will retry");
                    TaskOutcome::Retry {
                        delay_ms: 100,
                        error_code: 11,
                        error: error.to_string(),
                    }
                }
            }
        })
    }
}

fn io_accounting(payload: &MirrorToEcTaskV1) -> (u64, u64, u64) {
    let read_bytes = payload
        .old_strips
        .iter()
        .map(|strip| u64::from(strip.capacity).saturating_mul(1024))
        .sum();
    let shard_bytes = payload
        .old_strips
        .first()
        .map_or(0, |strip| u64::from(strip.capacity).saturating_mul(1024));
    let write_bytes =
        shard_bytes.saturating_mul(u64::from(payload.data_num.saturating_add(payload.code_num)));
    let retired_segments = payload
        .old_strips
        .iter()
        .filter_map(|strip| match &strip.strip {
            Some(Strip::MirrorStrip(mirror)) => {
                Some(u64::try_from(mirror.segments.len()).unwrap_or(u64::MAX))
            }
            _ => None,
        })
        .sum();
    (read_bytes, write_bytes, retired_segments)
}

#[derive(Debug, thiserror::Error)]
enum ConversionRunError {
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Store(#[from] TaskStoreError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("conversion source changed")]
    Conflict,
    #[error("{0}")]
    Retry(String),
}

fn installed(chunk: &Chunk, payload: &MirrorToEcTaskV1) -> bool {
    payload.replacement_strip.as_ref().is_some_and(|replacement| {
        chunk.strips.iter().any(|strip| {
            strip.chunk_offset == replacement.chunk_offset
                && strip.strip_sequence == replacement.strip_sequence
                && strip == replacement
        })
    })
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

impl ConversionCoordinator {
    #[must_use]
    pub fn new(lifecycle: Arc<LifecycleHandler>, tasks: Arc<TaskStore>) -> Self {
        Self {
            lifecycle,
            tasks,
            wake: None,
            data_num: 8,
            code_num: 4,
            min_mirror_strips: 8,
            min_age_ms: 3_600_000,
            scan_cursor: ArcSwapOption::empty(),
            reservation_scan_cursor: ArcSwapOption::empty(),
        }
    }

    #[must_use]
    pub fn with_wake(mut self, wake: Arc<tokio::sync::Notify>) -> Self {
        self.wake = Some(wake);
        self
    }

    #[must_use]
    pub fn with_policy(
        mut self,
        data_num: u32,
        code_num: u32,
        min_mirror_strips: u32,
        min_age_ms: u64,
    ) -> Self {
        self.data_num = data_num;
        self.code_num = code_num;
        self.min_mirror_strips = min_mirror_strips;
        self.min_age_ms = min_age_ms;
        self
    }

    pub async fn reconcile_reservations(&self, max_groups: u32, now_ms: u64) -> Result<u64, ConversionError> {
        let cursor = self.reservation_scan_cursor.load_full();
        let groups = self
            .lifecycle
            .scan_reservation_groups_after(
                max_groups.max(1),
                cursor.as_deref().map(|(chunk_id, group_id)| (chunk_id, group_id)),
            )
            .await?;
        if let Some(group) = groups.last() {
            if let (Some(chunk_id), Some(group_id)) = (group.chunk_id, group.group_id) {
                self.reservation_scan_cursor
                    .store(Some(Arc::new((chunk_id, group_id))));
            }
        } else {
            self.reservation_scan_cursor.store(None);
        }
        let mut reconciled = 0_u64;
        for group in groups {
            if group.lease_deadline_ms > now_ms {
                continue;
            }
            let (Some(chunk_id), Some(group_id)) = (group.chunk_id, group.group_id) else {
                continue;
            };
            match self
                .lifecycle
                .recover_expired_reservation_group(&chunk_id, &group_id, now_ms)
                .await?
            {
                ReservationRecovery::Active => {}
                ReservationRecovery::Reconciled => {
                    reconciled = reconciled.saturating_add(1);
                }
                ReservationRecovery::CompleteConversion { chunk_id } => {
                    self.trigger_chunk(chunk_id, group.data_num, group.code_num, now_ms)
                        .await?;
                    self.lifecycle
                        .finish_reconciled_conversion_group(&chunk_id, &group_id)
                        .await?;
                    reconciled = reconciled.saturating_add(1);
                }
            }
        }
        Ok(reconciled)
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
        if let Some(wake) = &self.wake {
            wake.notify_one();
        }

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
        // The checkpoint response is ambiguous. Retain the tentative blocks:
        // recovery can reuse them if the task update committed, and DiskDB's
        // tentative scanner reclaims them if it did not.
        self.tasks.write_transition(Some(&task), &allocated_task).await?;
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

    pub async fn trigger_chunk(
        &self,
        chunk_id: ChunkId,
        data_num: u32,
        code_num: u32,
        now_ms: u64,
    ) -> Result<u64, ConversionError> {
        let chunk = self.lifecycle.query_chunk(&chunk_id).await?;
        self.admit_groups(&chunk, data_num, code_num, now_ms, false, 0)
            .await
    }

    pub async fn trigger_configured_chunk(
        &self,
        chunk_id: ChunkId,
        now_ms: u64,
    ) -> Result<u64, ConversionError> {
        self.trigger_chunk(chunk_id, self.data_num, self.code_num, now_ms)
            .await
    }

    pub async fn trigger_configured_batch(
        &self,
        sealed_only: bool,
        max_chunks: u32,
        now_ms: u64,
    ) -> Result<u64, ConversionError> {
        self.trigger_batch(
            sealed_only,
            max_chunks,
            self.data_num,
            self.code_num,
            self.min_mirror_strips,
            self.min_age_ms,
            now_ms,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn trigger_batch(
        &self,
        sealed_only: bool,
        max_chunks: u32,
        data_num: u32,
        code_num: u32,
        min_mirror_strips: u32,
        min_age_ms: u64,
        now_ms: u64,
    ) -> Result<u64, ConversionError> {
        let limit = if max_chunks == 0 { 256 } else { max_chunks };
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
        let mut accepted_chunks = 0_u64;
        for chunk in chunks {
            if sealed_only && chunk.state != ChunkState::Sealed as i32 {
                continue;
            }
            if chunk
                .strips
                .iter()
                .filter(|strip| matches!(&strip.strip, Some(Strip::MirrorStrip(_))))
                .count()
                < usize::try_from(min_mirror_strips).unwrap_or(usize::MAX)
            {
                continue;
            }
            let accepted_groups = self
                .admit_groups(&chunk, data_num, code_num, now_ms, true, min_age_ms)
                .await?;
            if accepted_groups > 0 {
                accepted_chunks = accepted_chunks.saturating_add(1);
            }
        }
        Ok(accepted_chunks)
    }

    async fn admit_groups(
        &self,
        chunk: &Chunk,
        data_num: u32,
        code_num: u32,
        now_ms: u64,
        enforce_age: bool,
        min_age_ms: u64,
    ) -> Result<u64, ConversionError> {
        let width = usize::try_from(data_num).unwrap_or(usize::MAX);
        if width == 0 || code_num == 0 {
            return Err(ConversionError::Payload("invalid EC scheme".into()));
        }
        let Some(chunk_id) = chunk.id else {
            return Err(ConversionError::Payload("chunk has no id".into()));
        };
        let mut accepted = 0_u64;
        let mut start = 0_usize;
        while start.saturating_add(width) <= chunk.strips.len() {
            let old = &chunk.strips[start..start + width];
            if !candidate_group(chunk, old, enforce_age, min_age_ms, now_ms) {
                start += 1;
                continue;
            }
            let task_id = conversion_task_id(old, data_num, code_num)?;
            if self
                .tasks
                .get(&chunk_id, TASK_KIND_MIRROR_TO_EC, &task_id)
                .await?
                .is_none()
            {
                let operation_id = conversion_operation_id(chunk_id, task_id);
                let payload = MirrorToEcTaskV1 {
                    chunk_id,
                    expected_modify_ts: chunk.modify_ts,
                    start_index: u32::try_from(start).unwrap_or(u32::MAX),
                    old_strips: old.to_vec(),
                    data_num,
                    code_num,
                    replacement_strip: None,
                    phase: MirrorToEcPhase::Discovered,
                };
                let mut task = make_client_task(task_id, operation_id, &payload, 0, 1, now_ms)?;
                task.state = ChunkTaskState::Pending;
                task.attempt = 0;
                task.claim_generation = 0;
                task.claim_deadline_ms = 0;
                task.priority = 128;
                self.tasks.write_transition(None, &task).await?;
                accepted = accepted.saturating_add(1);
            }
            start += width;
        }
        if accepted > 0 {
            if let Some(wake) = &self.wake {
                wake.notify_one();
            }
        }
        Ok(accepted)
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
    if !convertible_chunk_state(chunk.state)
        || old_strips.is_empty()
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

fn candidate_group(
    chunk: &Chunk,
    strips: &[ChunkStrip],
    enforce_age: bool,
    min_age_ms: u64,
    now_ms: u64,
) -> bool {
    if !convertible_chunk_state(chunk.state) {
        return false;
    }
    let Some(first) = strips.first() else {
        return false;
    };
    let mut expected_offset = first.chunk_offset;
    let geometry_matches =
        first.unit_kb > 0 && first.capacity > 0 && strips.iter().all(|strip| {
            let matches = strip.unit_kb == first.unit_kb
                && strip.capacity == first.capacity
                && strip.chunk_offset == expected_offset
                && matches!(&strip.strip, Some(Strip::MirrorStrip(mirror)) if !mirror.segments.is_empty());
            expected_offset = expected_offset.saturating_add(strip.capacity);
            matches
        });
    let closed = chunk.state == ChunkState::Sealed as i32
        || chunk
            .closed_strip_sequence
            .is_some_and(|sequence| strips.iter().all(|strip| strip.strip_sequence <= sequence));
    let old_enough = !enforce_age
        || strips
            .iter()
            .all(|strip| strip.sealed_ts_ms > 0 && strip.sealed_ts_ms.saturating_add(min_age_ms) <= now_ms);
    geometry_matches && closed && old_enough
}

fn convertible_chunk_state(state: i32) -> bool {
    state == ChunkState::Active as i32 || state == ChunkState::Sealed as i32
}

fn locate_source(chunk: &Chunk, old_strips: &[ChunkStrip]) -> Option<u32> {
    if old_strips.is_empty() {
        return None;
    }
    let start = chunk
        .strips
        .windows(old_strips.len())
        .position(|candidate| candidate == old_strips)?;
    let start_index = u32::try_from(start).ok()?;
    validate_source(chunk, chunk.modify_ts, start_index, old_strips)
        .is_ok()
        .then_some(start_index)
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
