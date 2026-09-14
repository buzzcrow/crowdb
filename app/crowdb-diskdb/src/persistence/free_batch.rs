// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free, immediate-drain coalescing for concurrent free requests.

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_queue::SegQueue;
use tokio::sync::{oneshot, Notify};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::metrics::DiskdbMetrics;
use crate::model::alloc::{commit_prepared_batch, FreeBatchResult, FreeError, FreeRecord, PreparedFree};

pub type PersistFuture<'a> = Pin<Box<dyn Future<Output = Result<(), FreeError>> + Send + 'a>>;

/// Persistence boundary used by the coalescer and deterministic tests.
pub trait FreeBatchPersist: Send + Sync + 'static {
    fn persist<'a>(&'a self, bind: Bind, records: &'a [FreeRecord]) -> PersistFuture<'a>;
}

impl FreeBatchPersist for DdbKvClient {
    fn persist<'a>(&'a self, bind: Bind, records: &'a [FreeRecord]) -> PersistFuture<'a> {
        Box::pin(async move {
            self.persist_free_batch(bind, records)
                .await
                .map_err(FreeError::from)
        })
    }
}

struct PendingFreeRequest {
    prepared: PreparedFree,
    max_records: usize,
    complete: oneshot::Sender<Result<FreeBatchResult, FreeError>>,
}

/// One service-owned concurrent-free coordinator.
pub struct FreeBatcher<P: FreeBatchPersist = DdbKvClient> {
    persistence: Arc<P>,
    metrics: Arc<DiskdbMetrics>,
    queue: SegQueue<PendingFreeRequest>,
    draining: AtomicBool,
    closed: AtomicBool,
    admitted: AtomicUsize,
    idle: Notify,
}

impl<P: FreeBatchPersist> FreeBatcher<P> {
    #[must_use]
    pub fn new(persistence: Arc<P>, metrics: Arc<DiskdbMetrics>) -> Self {
        Self {
            persistence,
            metrics,
            queue: SegQueue::new(),
            draining: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            admitted: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }

    /// Persist one request directly while retaining shutdown admission tracking.
    pub async fn submit_direct(
        self: &Arc<Self>,
        prepared: PreparedFree,
    ) -> Result<FreeBatchResult, FreeError> {
        self.admit()?;
        let record_count = prepared.record_count().try_into().unwrap_or(u64::MAX);
        self.metrics.free_batch_input_requests.inc();
        self.metrics.free_batch_input_records.inc_by(record_count);
        self.metrics.free_batch_output_batches.inc();
        self.metrics.free_batch_output_records.inc_by(record_count);
        self.update_ratio();
        let (complete, receiver) = oneshot::channel();
        let batcher = Arc::clone(self);
        tokio::spawn(async move {
            let result = batcher
                .persistence
                .persist(prepared.bind(), &prepared.records)
                .await;
            let result = match result {
                Ok(()) => {
                    let response = prepared.result();
                    commit_prepared_batch(&[&prepared]);
                    Ok(response)
                }
                Err(error) => {
                    batcher.metrics.free_batch_failures.inc();
                    Err(error)
                }
            };
            let _ = complete.send(result);
            batcher.finish_admitted();
        });
        receiver.await.unwrap_or(Err(FreeError::Closed))
    }

    /// Queue an enabled-mode request and await its durable outcome.
    pub async fn submit(
        self: &Arc<Self>,
        prepared: PreparedFree,
        max_records: usize,
    ) -> Result<FreeBatchResult, FreeError> {
        self.admit()?;
        let record_count = prepared.record_count();
        self.metrics.free_batch_input_requests.inc();
        self.metrics
            .free_batch_input_records
            .inc_by(record_count.try_into().unwrap_or(u64::MAX));
        if record_count > max_records.max(1) {
            self.metrics.free_batch_oversize_requests.inc();
        }

        let (complete, receiver) = oneshot::channel();
        self.queue.push(PendingFreeRequest {
            prepared,
            max_records: max_records.max(1),
            complete,
        });
        self.metrics.free_batch_queue_depth.inc();
        self.start_drainer();

        receiver.await.unwrap_or(Err(FreeError::Closed))
    }

    /// Stop admission and wait until every accepted direct or queued free ends.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        loop {
            let idle = self.idle.notified();
            if self.admitted.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }

    fn admit(&self) -> Result<(), FreeError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(FreeError::Closed);
        }
        self.admitted.fetch_add(1, Ordering::AcqRel);
        if self.closed.load(Ordering::Acquire) {
            self.finish_admitted();
            return Err(FreeError::Closed);
        }
        Ok(())
    }

    fn finish_admitted(&self) {
        if self.admitted.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle.notify_waiters();
        }
    }

    fn start_drainer(self: &Arc<Self>) {
        if self
            .draining
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let batcher = Arc::clone(self);
            tokio::spawn(async move { batcher.drain().await });
        }
    }

    async fn drain(self: Arc<Self>) {
        let drain_start = Instant::now();
        let mut deferred = VecDeque::new();
        loop {
            self.move_queued(&mut deferred);
            let Some(seed) = deferred.pop_front() else {
                self.draining.store(false, Ordering::Release);
                if self.queue.is_empty()
                    || self
                        .draining
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                {
                    self.metrics
                        .free_batch_drain_latency
                        .observe(drain_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
                    return;
                }
                continue;
            };

            let batch = Self::take_batch(seed, &mut deferred);
            self.persist_batch(batch).await;
        }
    }

    fn move_queued(&self, deferred: &mut VecDeque<PendingFreeRequest>) {
        while let Some(request) = self.queue.pop() {
            self.metrics.free_batch_queue_depth.dec();
            deferred.push_back(request);
        }
    }

    fn take_batch(
        seed: PendingFreeRequest,
        deferred: &mut VecDeque<PendingFreeRequest>,
    ) -> Vec<PendingFreeRequest> {
        let bind = seed.prepared.bind();
        let mut cap = seed.max_records;
        let mut records = seed.prepared.record_count();
        let oversized = records > cap;
        let mut batch = vec![seed];
        if oversized {
            return batch;
        }

        let candidates = deferred.len();
        for _ in 0..candidates {
            let request = deferred.pop_front().expect("candidate count is exact");
            let request_records = request.prepared.record_count();
            let next_cap = cap.min(request.max_records);
            if request.prepared.bind() == bind && records.saturating_add(request_records) <= next_cap {
                records += request_records;
                cap = next_cap;
                batch.push(request);
            } else {
                deferred.push_back(request);
            }
        }
        batch
    }

    async fn persist_batch(&self, batch: Vec<PendingFreeRequest>) {
        let bind = batch[0].prepared.bind();
        let records = Self::deduplicated_records(&batch);
        self.metrics.free_batch_output_batches.inc();
        self.metrics
            .free_batch_output_records
            .inc_by(records.len().try_into().unwrap_or(u64::MAX));
        self.update_ratio();

        let result = self.persistence.persist(bind, &records).await;
        if result.is_ok() {
            let prepared: Vec<&PreparedFree> = batch.iter().map(|request| &request.prepared).collect();
            commit_prepared_batch(&prepared);
        } else {
            self.metrics.free_batch_failures.inc();
        }
        for request in batch {
            let response = match &result {
                Ok(()) => Ok(request.prepared.result()),
                Err(error) => Err(error.clone()),
            };
            let _ = request.complete.send(response);
            self.finish_admitted();
        }
    }

    fn deduplicated_records(batch: &[PendingFreeRequest]) -> Vec<FreeRecord> {
        let capacity = batch.iter().map(|request| request.prepared.records.len()).sum();
        let mut records = Vec::with_capacity(capacity);
        let mut seen = HashSet::with_capacity(capacity);
        for request in batch {
            for record in &request.prepared.records {
                let identity = (record.0, record.1, record.2, record.3.pre_allocation_ts);
                if seen.insert(identity) {
                    records.push(record.clone());
                }
            }
        }
        records
    }

    fn update_ratio(&self) {
        let requests = self.metrics.free_batch_input_requests.snapshot().total;
        let batches = self.metrics.free_batch_output_batches.snapshot().total;
        let ratio = requests.saturating_mul(1_000).checked_div(batches).unwrap_or(0);
        self.metrics.free_batch_coalescing_ratio_x1000.set(ratio);
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn state_for_tests(&self) -> (usize, bool, usize, bool) {
        (
            self.queue.len(),
            self.draining.load(Ordering::Acquire),
            self.admitted.load(Ordering::Acquire),
            self.closed.load(Ordering::Acquire),
        )
    }
}
