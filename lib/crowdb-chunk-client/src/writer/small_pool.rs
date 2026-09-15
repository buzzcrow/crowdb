// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared admission budget and lock-free small-object routing snapshot.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::Location;
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};

use crate::config::SmallWritePolicy;
use crate::metrics::SmallWriteMetrics;
use crate::negative_list::FailedDiskList;
use crate::{ChunkAllocator, DiskWriter, IoError, Result};

use super::small_manager::{self, ManagerCommand};

pub(crate) struct PendingObject {
    pub route_hash: u64,
    pub route: Arc<PipelineRoute>,
    pub fragments: Vec<Bytes>,
    pub len: usize,
    pub enqueued_at: Instant,
    pub completion: oneshot::Sender<Result<Vec<Location>>>,
    pub charge: RouteCharge,
}

/// Retained bytes owned by one request while it moves handler -> queue -> worker.
/// The same charge moves with the fragments, so that transfer never double-counts.
pub(crate) struct RouteCharge {
    route: Arc<PipelineRoute>,
    metrics: Arc<SmallWriteMetrics>,
    bytes: u64,
}

impl RouteCharge {
    pub fn new(route: Arc<PipelineRoute>, metrics: Arc<SmallWriteMetrics>) -> Self {
        Self {
            route,
            metrics,
            bytes: 0,
        }
    }

    pub fn add(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.bytes = self.bytes.saturating_add(bytes);
        self.route.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.metrics.reserved_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn rebind(&mut self, route: Arc<PipelineRoute>) {
        if Arc::ptr_eq(&self.route, &route) {
            return;
        }
        self.route.used_bytes.fetch_sub(self.bytes, Ordering::Release);
        self.route.capacity_changed.notify_waiters();
        route.used_bytes.fetch_add(self.bytes, Ordering::Relaxed);
        self.route = route;
    }
}

impl Drop for RouteCharge {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.route.used_bytes.fetch_sub(self.bytes, Ordering::Release);
            self.metrics
                .reserved_bytes
                .fetch_sub(self.bytes, Ordering::Relaxed);
            self.route.capacity_changed.notify_waiters();
        }
    }
}

pub(crate) struct PipelineRoute {
    pub sender: mpsc::Sender<PendingObject>,
    pub used_bytes: AtomicU64,
    pub capacity_bytes: u64,
    pub capacity_changed: Notify,
    pub queued_bytes: AtomicU64,
    pub queued_objects: AtomicU64,
    pub last_active_ms: AtomicU64,
    pub busy: AtomicBool,
    pub conversion_active: Arc<AtomicBool>,
}

impl PipelineRoute {
    pub fn new(
        sender: mpsc::Sender<PendingObject>,
        now_ms: u64,
        conversion_active: Arc<AtomicBool>,
        capacity_bytes: usize,
    ) -> Self {
        Self {
            sender,
            used_bytes: AtomicU64::new(0),
            capacity_bytes: u64::try_from(capacity_bytes).unwrap_or(u64::MAX),
            capacity_changed: Notify::new(),
            queued_bytes: AtomicU64::new(0),
            queued_objects: AtomicU64::new(0),
            last_active_ms: AtomicU64::new(now_ms),
            busy: AtomicBool::new(false),
            conversion_active,
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

    pub fn has_capacity(&self) -> bool {
        self.used_bytes.load(Ordering::Acquire) < self.capacity_bytes
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
    pub(crate) conversion_budget: Arc<Semaphore>,
    pub(crate) conversion_active: Arc<AtomicBool>,
}

impl SmallPoolRuntime {
    pub fn route_for_hash(&self, hash: u64) -> Option<Arc<PipelineRoute>> {
        let routes = self.routes.load_full();
        (!routes.is_empty()).then(|| choose_route(&routes, hash))
    }
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    pub fn publish(&self, routes: &[Arc<PipelineRoute>]) {
        self.routes.store(Arc::new(routes.to_vec()));
        self.metrics.active_pipelines.set(routes.len() as u64);
        self.metrics
            .max_active_pipelines
            .fetch_max(routes.len() as u64, Ordering::Relaxed);
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
            let route = Arc::clone(&object.route);
            route.accepted(object.len);
            match route.sender.try_send(object) {
                Ok(()) => {
                    self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(error) => {
                    let closed = matches!(&error, mpsc::error::TrySendError::Closed(_));
                    object = error.into_inner();
                    route.rejected(object.len);
                    if closed {
                        let replacement = choose_route(&routes, object.route_hash);
                        object.charge.rebind(Arc::clone(&replacement));
                        object.route = replacement;
                    }
                    let notified = route.capacity_changed.notified();
                    tokio::select! {
                        () = notified => {},
                        () = tokio::time::sleep(self.policy.control_interval) => {},
                    }
                }
            }
        }
    }
}

fn choose_route(routes: &[Arc<PipelineRoute>], hash: u64) -> Arc<PipelineRoute> {
    Arc::clone(&routes[hash as usize % routes.len()])
}

pub(crate) struct SmallWritePool {
    pub policy: Arc<SmallWritePolicy>,
    pub allocator: Arc<dyn ChunkAllocator>,
    pub disk_writer: Arc<dyn DiskWriter>,
    pub metrics: Arc<SmallWriteMetrics>,
    pub failed_disks: Arc<FailedDiskList>,
    runtime: tokio::sync::OnceCell<Arc<SmallPoolRuntime>>,
}

impl SmallWritePool {
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        policy: SmallWritePolicy,
        metrics: Arc<SmallWriteMetrics>,
        failed_disks: Arc<FailedDiskList>,
    ) -> Result<Arc<Self>> {
        policy.validate()?;
        Ok(Arc::new(Self {
            policy: Arc::new(policy),
            allocator,
            disk_writer,
            metrics,
            failed_disks,
            runtime: tokio::sync::OnceCell::new(),
        }))
    }

    pub async fn runtime(self: &Arc<Self>) -> Result<Arc<SmallPoolRuntime>> {
        self.runtime
            .get_or_try_init(|| small_manager::start(Arc::clone(self)))
            .await
            .map(Arc::clone)
    }

    pub async fn prepare(self: &Arc<Self>, bytes: usize) -> Result<Arc<SmallPoolRuntime>> {
        if bytes > self.policy.object_limit || bytes > self.policy.memory_budget {
            return Err(IoError::ObjectTooLarge {
                size: bytes,
                limit: self.policy.object_limit.min(self.policy.memory_budget),
            });
        }
        self.runtime().await
    }

    pub async fn shutdown(&self) -> Result<()> {
        let Some(runtime) = self.runtime.get() else {
            return Ok(());
        };
        if runtime.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
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
        let (done, _completion) = oneshot::channel();
        let _ = runtime.manager_tx.send(ManagerCommand::Shutdown(done));
    }
}
