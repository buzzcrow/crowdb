// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared admission budget and lock-free small-object routing snapshot.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::Location;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::config::SmallWritePolicy;
use crate::metrics::SmallWriteMetrics;
use crate::negative_list::FailedDiskList;
use crate::{ChunkAllocator, DiskWriter, IoError, Result};

use super::small_manager::{self, ManagerCommand};

pub(crate) struct PendingObject {
    pub fragments: Vec<Bytes>,
    pub len: usize,
    pub enqueued_at: Instant,
    pub completion: oneshot::Sender<Result<Vec<Location>>>,
    pub _reservation: ByteReservation,
}

pub(crate) struct ByteReservation {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<SmallWriteMetrics>,
    bytes: u64,
}

impl ByteReservation {
    fn new(permit: OwnedSemaphorePermit, metrics: Arc<SmallWriteMetrics>, bytes: usize) -> Self {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        metrics.reserved_bytes.fetch_add(bytes, Ordering::Relaxed);
        Self {
            _permit: permit,
            metrics,
            bytes,
        }
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.metrics
            .reserved_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

pub(crate) struct PipelineRoute {
    pub sender: mpsc::Sender<PendingObject>,
    pub queued_bytes: AtomicU64,
    pub queued_objects: AtomicU64,
    pub last_active_ms: AtomicU64,
    pub busy: AtomicBool,
    pub conversion_active: Arc<AtomicBool>,
}

impl PipelineRoute {
    pub fn new(sender: mpsc::Sender<PendingObject>, now_ms: u64) -> Self {
        Self {
            sender,
            queued_bytes: AtomicU64::new(0),
            queued_objects: AtomicU64::new(0),
            last_active_ms: AtomicU64::new(now_ms),
            busy: AtomicBool::new(false),
            conversion_active: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn accepted(&self, bytes: usize) {
        self.queued_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.queued_objects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn rejected(&self, bytes: usize) {
        self.queued_bytes.fetch_sub(bytes as u64, Ordering::Relaxed);
        self.queued_objects.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn dequeued(&self, bytes: usize, now_ms: u64) {
        self.rejected(bytes);
        self.last_active_ms.store(now_ms, Ordering::Relaxed);
    }
}

pub(crate) struct SmallPoolRuntime {
    pub policy: Arc<SmallWritePolicy>,
    pub allocator: Arc<dyn ChunkAllocator>,
    pub disk_writer: Arc<dyn DiskWriter>,
    pub routes: ArcSwap<Vec<Arc<PipelineRoute>>>,
    pub metrics: Arc<SmallWriteMetrics>,
    pub origin: Instant,
    pub closed: AtomicBool,
    pub route_nonce: AtomicU64,
    pub manager_tx: mpsc::UnboundedSender<ManagerCommand>,
    pub failed_disks: Arc<FailedDiskList>,
    pub(crate) budget: Arc<Semaphore>,
}

impl SmallPoolRuntime {
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    pub fn publish(&self, routes: &[Arc<PipelineRoute>]) {
        self.routes.store(Arc::new(routes.to_vec()));
        self.metrics.active_pipelines.set(routes.len() as u64);
    }

    pub async fn reserve(self: &Arc<Self>, bytes: usize) -> Result<ByteReservation> {
        if self.closed.load(Ordering::Acquire) {
            return Err(IoError::Finished);
        }
        let permits = u32::try_from(bytes).map_err(|_| IoError::ObjectTooLarge {
            size: bytes,
            limit: self.policy.object_limit,
        })?;
        let permit = Arc::clone(&self.budget)
            .acquire_many_owned(permits)
            .await
            .map_err(|_| IoError::Finished)?;
        if self.closed.load(Ordering::Acquire) {
            drop(permit);
            return Err(IoError::Finished);
        }
        Ok(ByteReservation::new(permit, Arc::clone(&self.metrics), bytes))
    }

    pub async fn submit(&self, mut object: PendingObject) -> Result<()> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(IoError::Finished);
            }
            let routes = self.routes.load_full();
            if routes.is_empty() {
                tokio::task::yield_now().await;
                continue;
            }
            let route = choose_route(&routes, self.route_nonce.fetch_add(1, Ordering::Relaxed));
            route.accepted(object.len);
            match route.sender.try_send(object) {
                Ok(()) => {
                    self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(error) => {
                    object = error.into_inner();
                    route.rejected(object.len);
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

fn choose_route(routes: &[Arc<PipelineRoute>], nonce: u64) -> Arc<PipelineRoute> {
    if routes.len() == 1 {
        return Arc::clone(&routes[0]);
    }
    let first = nonce as usize % routes.len();
    let mixed = nonce.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17);
    let mut second = mixed as usize % routes.len();
    if second == first {
        second = (second + 1) % routes.len();
    }
    let a = &routes[first];
    let b = &routes[second];
    if a.queued_bytes.load(Ordering::Relaxed) <= b.queued_bytes.load(Ordering::Relaxed) {
        Arc::clone(a)
    } else {
        Arc::clone(b)
    }
}

pub(crate) struct SmallWritePool {
    pub policy: Arc<SmallWritePolicy>,
    pub allocator: Arc<dyn ChunkAllocator>,
    pub disk_writer: Arc<dyn DiskWriter>,
    pub metrics: Arc<SmallWriteMetrics>,
    runtime: tokio::sync::OnceCell<Arc<SmallPoolRuntime>>,
}

impl SmallWritePool {
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        policy: SmallWritePolicy,
        metrics: Arc<SmallWriteMetrics>,
    ) -> Result<Arc<Self>> {
        policy.validate()?;
        Ok(Arc::new(Self {
            policy: Arc::new(policy),
            allocator,
            disk_writer,
            metrics,
            runtime: tokio::sync::OnceCell::new(),
        }))
    }

    pub async fn runtime(self: &Arc<Self>) -> Result<Arc<SmallPoolRuntime>> {
        self.runtime
            .get_or_try_init(|| small_manager::start(Arc::clone(self)))
            .await
            .map(Arc::clone)
    }

    pub async fn reserve(self: &Arc<Self>, bytes: usize) -> Result<(Arc<SmallPoolRuntime>, ByteReservation)> {
        if bytes > self.policy.object_limit || bytes > self.policy.memory_budget {
            return Err(IoError::ObjectTooLarge {
                size: bytes,
                limit: self.policy.object_limit.min(self.policy.memory_budget),
            });
        }
        let runtime = self.runtime().await?;
        let reservation = runtime.reserve(bytes).await?;
        Ok((runtime, reservation))
    }

    pub async fn shutdown(&self) -> Result<()> {
        let Some(runtime) = self.runtime.get() else {
            return Ok(());
        };
        if runtime.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        runtime.budget.close();
        let (done_tx, done_rx) = oneshot::channel();
        runtime
            .manager_tx
            .send(ManagerCommand::Shutdown(done_tx))
            .map_err(|_| IoError::Internal("small-write manager stopped".into()))?;
        done_rx
            .await
            .map_err(|_| IoError::Internal("small-write manager shutdown was lost".into()))?
    }
}

impl Drop for SmallWritePool {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.get() else {
            return;
        };
        if runtime.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        runtime.budget.close();
        let (done, _completion) = oneshot::channel();
        let _ = runtime.manager_tx.send(ManagerCommand::Shutdown(done));
    }
}
