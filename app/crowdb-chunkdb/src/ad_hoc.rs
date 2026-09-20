// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Process-local rendezvous for read-triggered EC repair. Durable task claims
//! remain the sole authority for allocating and publishing replacement blocks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_protocol::chunk_task::TASK_KIND_REPAIR_STRIP;
use crowdb_protocol::chunkdb::rpc::{
    AdHocEcRecoveryDisposition as Disposition, AdHocEcRecoveryRequest, AdHocEcRecoveryResponse, Chunk,
    ChunkStrip, EcState, Strip,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::ReadyChunkTaskKey;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

use crate::allocator::DiskdbClientPool;
use crate::lifecycle::LifecycleHandler;
use crate::metrics::RepairMetrics;
use crate::repair::RepairCoordinator;
use crate::task::{TaskExecutor, TaskManager, TaskStore};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct RecoveryKey {
    pub chunk_id: ChunkId,
    pub strip_sequence: u32,
    pub segment: Segment,
}

struct Entry {
    source_revision: u64,
    result: watch::Sender<Option<Bytes>>,
    _job: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
}

/// Shared between RPC admission and the durable repair executor. The map is
/// replaced with compare-and-swap, so admission has no explicit hot-path lock.
pub struct AdHocRecoveryShared {
    entries: ArcSwap<HashMap<RecoveryKey, Arc<Entry>>>,
    jobs: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    memory_limit: usize,
    metrics: Arc<RepairMetrics>,
}

impl AdHocRecoveryShared {
    pub fn new(max_jobs: usize, memory_bytes: usize, metrics: Arc<RepairMetrics>) -> Self {
        Self {
            entries: ArcSwap::from_pointee(HashMap::new()),
            jobs: Arc::new(Semaphore::new(max_jobs)),
            memory: Arc::new(Semaphore::new(memory_bytes)),
            memory_limit: memory_bytes,
            metrics,
        }
    }

    pub(crate) fn memory(&self) -> Arc<Semaphore> {
        Arc::clone(&self.memory)
    }
    pub(crate) fn memory_limit(&self) -> usize {
        self.memory_limit
    }

    fn join_or_insert(
        &self,
        key: RecoveryKey,
        revision: u64,
        reserve_bytes: usize,
    ) -> Option<(Arc<Entry>, bool)> {
        loop {
            let current = self.entries.load_full();
            if let Some(entry) = current.get(&key) {
                return (entry.source_revision == revision).then(|| (Arc::clone(entry), false));
            }
            let bytes = u32::try_from(reserve_bytes).ok()?;
            let job = Arc::clone(&self.jobs).try_acquire_owned().ok()?;
            let memory = Arc::clone(&self.memory).try_acquire_many_owned(bytes).ok()?;
            let (result, _) = watch::channel(None);
            let entry = Arc::new(Entry {
                source_revision: revision,
                result,
                _job: job,
                _memory: memory,
            });
            let mut next = (*current).clone();
            next.insert(key, Arc::clone(&entry));
            let previous = self.entries.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&previous, &current) {
                return Some((entry, true));
            }
        }
    }

    pub fn rebuilt(&self, key: RecoveryKey, bytes: Bytes) {
        if let Some(entry) = self.entries.load().get(&key) {
            entry.result.send_replace(Some(bytes));
        }
    }

    pub fn published(&self, key: RecoveryKey) {
        if self.entries.load().contains_key(&key) {
            self.metrics.ad_hoc_publish();
        }
    }

    fn remove(&self, key: RecoveryKey, entry: &Arc<Entry>) {
        loop {
            let current = self.entries.load_full();
            if !current.get(&key).is_some_and(|stored| Arc::ptr_eq(stored, entry)) {
                return;
            }
            let mut next = (*current).clone();
            next.remove(&key);
            if Arc::ptr_eq(&self.entries.compare_and_swap(&current, Arc::new(next)), &current) {
                return;
            }
        }
    }
}

pub struct AdHocRecoveryManager {
    shared: Arc<AdHocRecoveryShared>,
    lifecycle: Arc<LifecycleHandler>,
    diskdb: Arc<DiskdbClientPool>,
    coordinator: Arc<RepairCoordinator>,
    tasks: Arc<TaskStore>,
    manager: Arc<TaskManager>,
    executor: Arc<TaskExecutor>,
}

impl AdHocRecoveryManager {
    pub fn new(
        shared: Arc<AdHocRecoveryShared>,
        lifecycle: Arc<LifecycleHandler>,
        diskdb: Arc<DiskdbClientPool>,
        coordinator: Arc<RepairCoordinator>,
        tasks: Arc<TaskStore>,
        manager: Arc<TaskManager>,
        executor: Arc<TaskExecutor>,
    ) -> Self {
        Self {
            shared,
            lifecycle,
            diskdb,
            coordinator,
            tasks,
            manager,
            executor,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn request(
        self: &Arc<Self>,
        request: AdHocEcRecoveryRequest,
    ) -> Result<AdHocEcRecoveryResponse, String> {
        let chunk_id = request.chunk_id.ok_or("chunk ID is required")?;
        let segment = request.failed_segment.ok_or("failed segment is required")?;
        if request.version != 1 || request.operation_id.is_none() || segment.owner_chunk != Some(chunk_id) {
            return Err("invalid recovery request identity".into());
        }
        let key = RecoveryKey {
            chunk_id,
            strip_sequence: request.strip_sequence,
            segment,
        };
        let chunk = self
            .lifecycle
            .query_chunk(&chunk_id)
            .await
            .map_err(|error| error.to_string())?;
        let Some((index, strip)) = chunk
            .strips
            .iter()
            .enumerate()
            .find(|(_, strip)| strip.strip_sequence == key.strip_sequence)
        else {
            self.shared.metrics.ad_hoc_stale();
            return Ok(response(Disposition::Stale));
        };
        let ec = match strip.strip.as_ref() {
            Some(Strip::EcStrip(ec)) => {
                if !ec.segments.contains(&segment) {
                    return Ok(response(Disposition::Healed));
                }
                if ec.ec_state != EcState::Parity as i32 || ec.data_num == 0 || ec.code_num == 0 {
                    return Ok(response(Disposition::Incompatible));
                }
                if strip.unavailable_segments.len() >= ec.code_num as usize
                    && !strip.unavailable_segments.contains(&segment)
                {
                    return Ok(response(Disposition::InsufficientShards));
                }
                Some(ec)
            }
            Some(Strip::MirrorStrip(mirror)) if !request.request_full_block => {
                if !mirror.segments.contains(&segment) {
                    return Ok(response(Disposition::Healed));
                }
                None
            }
            _ => return Ok(response(Disposition::Incompatible)),
        };
        if chunk.modify_ts != request.expected_modify_ts {
            self.shared.metrics.ad_hoc_stale();
            return Ok(response(Disposition::Stale));
        }
        if !request.request_full_block {
            self.report_corruption(
                &chunk,
                index,
                strip,
                segment,
                request.operation_id.expect("validated"),
            )
            .await?;
            return Ok(response(Disposition::Marked));
        }
        debug_assert!(ec.is_some());
        if !strip.unavailable_segments.contains(&segment) {
            return Ok(response(Disposition::Stale));
        }
        let shard_bytes = usize::try_from(u64::from(segment.unit_count) * u64::from(strip.unit_kb) * 1024)
            .map_err(|_| "fragment size overflows")?;
        let reserve = shard_bytes;
        let Some((entry, elected)) = self.shared.join_or_insert(key, chunk.modify_ts, reserve) else {
            self.shared.metrics.ad_hoc_reject();
            self.shared.metrics.ad_hoc_fallback();
            return Ok(response(Disposition::Saturated));
        };
        if elected {
            self.shared.metrics.ad_hoc_start();
        }
        let mut receiver = entry.result.subscribe();
        if elected {
            let owner = Arc::clone(self);
            tokio::spawn(async move {
                owner.run_task(key, entry).await;
            });
        }
        let recovered = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(bytes) = receiver.borrow_and_update().clone() {
                    return Some(bytes);
                }
                if receiver.changed().await.is_err() {
                    return None;
                }
            }
        })
        .await
        .ok()
        .flatten();
        Ok(if let Some(bytes) = recovered {
            AdHocEcRecoveryResponse {
                disposition: if elected {
                    Disposition::Started
                } else {
                    self.shared.metrics.ad_hoc_join(bytes.len() as u64);
                    Disposition::Coalesced
                },
                data: bytes.to_vec(),
            }
        } else {
            self.shared.metrics.ad_hoc_fallback();
            response(Disposition::Saturated)
        })
    }

    async fn report_corruption(
        &self,
        chunk: &Chunk,
        index: usize,
        strip: &ChunkStrip,
        segment: Segment,
        operation_id: ChunkId,
    ) -> Result<(), String> {
        if strip.unavailable_segments.contains(&segment) {
            self.coordinator
                .admit_chunk(chunk, now_ms())
                .await
                .map_err(|error| error.to_string())?;
            return Ok(());
        }
        self.diskdb.mark_blocks_corrupt(vec![segment]).await?;
        let mut replacement = strip.clone();
        replacement.unavailable_segments.push(segment);
        replacement.unavailable_segments.sort_by_key(segment_identity);
        let updated = self
            .lifecycle
            .replace_chunk_strip_range(
                &chunk.id.ok_or("chunk ID missing")?,
                chunk.modify_ts,
                u32::try_from(index).unwrap_or(u32::MAX),
                std::slice::from_ref(strip),
                std::slice::from_ref(&replacement),
                operation_id,
            )
            .await
            .map_err(|error| error.to_string())?;
        self.coordinator
            .admit_chunk(&updated, now_ms())
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn run_task(self: Arc<Self>, key: RecoveryKey, entry: Arc<Entry>) {
        let result = async {
            let chunk = self.lifecycle.query_chunk(&key.chunk_id).await?;
            self.coordinator
                .admit_chunk(&chunk, now_ms())
                .await
                .map_err(|error| crate::lifecycle::LifecycleError::InvalidRequest(error.to_string()))?;
            Ok::<_, crate::lifecycle::LifecycleError>(chunk)
        }
        .await;
        if let Ok(chunk) = result {
            if let Some(strip) = chunk
                .strips
                .iter()
                .find(|strip| strip.strip_sequence == key.strip_sequence)
            {
                let mut failures = strip.unavailable_segments.clone();
                failures.sort_by_key(segment_identity);
                let task_id = crate::repair::repair_task_id(key.strip_sequence, &failures);
                if let Ok(Some(task)) = self
                    .tasks
                    .get(&key.chunk_id, TASK_KIND_REPAIR_STRIP, &task_id)
                    .await
                {
                    let index = ReadyChunkTaskKey {
                        priority_inverse: u8::MAX - task.priority,
                        eligible_at_ms: task.eligible_at_ms,
                        partition_id: task.partition_id,
                        kind: task.kind,
                        task_id,
                    };
                    if let Ok(Some(claim)) = self.manager.claim(&index, now_ms()).await {
                        let _ = self.executor.execute(claim).await;
                    } else {
                        let mut receiver = entry.result.subscribe();
                        let _ = tokio::time::timeout(Duration::from_secs(30), receiver.changed()).await;
                    }
                }
            }
        }
        self.shared.remove(key, &entry);
    }
}

fn response(disposition: Disposition) -> AdHocEcRecoveryResponse {
    AdHocEcRecoveryResponse {
        disposition,
        data: Vec::new(),
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
