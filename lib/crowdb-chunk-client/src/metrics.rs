// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Aggregate chunk-client write-path metrics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crowdb_common::metrics::{Bandwidth, Counter, Gauge, LatencyHistogram, MetricsRegistry};

/// Metrics for one asynchronous operation kind.
pub struct OperationMetrics {
    latency: Arc<LatencyHistogram>,
    inflight: Arc<Gauge>,
    errors: Arc<Counter>,
}

impl OperationMetrics {
    fn register(registry: &mut MetricsRegistry, prefix: &str) -> Self {
        Self {
            latency: registry.register_histogram(format!("{prefix}.e2e.lh")),
            inflight: registry.register_gauge(format!("{prefix}.inflight.g")),
            errors: registry.register_counter(format!("{prefix}.errors.c")),
        }
    }

    /// Start timing an operation.
    #[must_use]
    pub fn start(&self) -> OperationGuard {
        self.inflight.inc();
        OperationGuard {
            latency: Arc::clone(&self.latency),
            inflight: Arc::clone(&self.inflight),
            errors: Arc::clone(&self.errors),
            started: Instant::now(),
            success: false,
        }
    }
}

/// Records latency and outcome when dropped.
pub struct OperationGuard {
    latency: Arc<LatencyHistogram>,
    inflight: Arc<Gauge>,
    errors: Arc<Counter>,
    started: Instant,
    success: bool,
}

impl OperationGuard {
    /// Mark the operation successful.
    pub fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if !self.success {
            self.errors.inc();
        }
        self.latency
            .observe(self.started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        self.inflight.dec();
    }
}

/// Hierarchical metrics for the large-write client path.
pub struct ChunkClientMetrics {
    pub object_write: OperationMetrics,
    pub chunk_allocate: OperationMetrics,
    pub chunk_append: OperationMetrics,
    pub chunk_seal: OperationMetrics,
    pub chunk_delete: OperationMetrics,
    pub diskio_write: OperationMetrics,
    pub logical_bytes: Arc<Bandwidth>,
    pub physical_bytes: Arc<Bandwidth>,
    pub diskio_write_bytes: Arc<Bandwidth>,
    pub small_write: Arc<SmallWriteMetrics>,
}

impl ChunkClientMetrics {
    /// Register all counters in the supplied process metrics registry.
    #[must_use]
    pub fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            object_write: OperationMetrics::register(registry, "chunkio.object.write"),
            chunk_allocate: OperationMetrics::register(registry, "chunkio.chunk.allocate"),
            chunk_append: OperationMetrics::register(registry, "chunkio.chunk.append"),
            chunk_seal: OperationMetrics::register(registry, "chunkio.chunk.seal"),
            chunk_delete: OperationMetrics::register(registry, "chunkio.chunk.delete"),
            diskio_write: OperationMetrics::register(registry, "chunkio.diskio.write"),
            logical_bytes: registry.register_bandwidth("chunkio.object.logical.bw"),
            physical_bytes: registry.register_bandwidth("chunkio.object.physical.bw"),
            diskio_write_bytes: registry.register_bandwidth("chunkio.diskio.write.bw"),
            small_write: Arc::new(SmallWriteMetrics::default()),
        }
    }
}

/// Lock-free counters and gauges for the shared small-write pool.
#[derive(Debug, Default)]
pub struct SmallWriteMetrics {
    pub(crate) submitted: AtomicU64,
    pub(crate) completed: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) reserved_bytes: AtomicU64,
    pub(crate) batches: AtomicU64,
    pub(crate) batch_objects: AtomicU64,
    pub(crate) batch_bytes: AtomicU64,
    pub(crate) max_batch_objects: AtomicU64,
    pub(crate) max_batch_bytes: AtomicU64,
    pub(crate) queue_delay_ns: AtomicU64,
    pub(crate) max_queue_delay_ns: AtomicU64,
    pub(crate) active_pipelines: AtomicU64,
    pub(crate) draining_pipelines: AtomicU64,
    pub(crate) scale_out: AtomicU64,
    pub(crate) scale_in: AtomicU64,
    pub(crate) tail_waste_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SmallWriteMetricsSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub reserved_bytes: u64,
    pub batches: u64,
    pub batch_objects: u64,
    pub batch_bytes: u64,
    pub max_batch_objects: u64,
    pub max_batch_bytes: u64,
    pub average_batch_fill_ppm: u64,
    pub queue_delay_ns: u64,
    pub max_queue_delay_ns: u64,
    pub active_pipelines: u64,
    pub draining_pipelines: u64,
    pub scale_out: u64,
    pub scale_in: u64,
    pub tail_waste_bytes: u64,
}

impl SmallWriteMetrics {
    #[must_use]
    pub fn snapshot(&self) -> SmallWriteMetricsSnapshot {
        let batches = self.batches.load(Ordering::Relaxed);
        let batch_bytes = self.batch_bytes.load(Ordering::Relaxed);
        SmallWriteMetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            reserved_bytes: self.reserved_bytes.load(Ordering::Relaxed),
            batches,
            batch_objects: self.batch_objects.load(Ordering::Relaxed),
            batch_bytes,
            max_batch_objects: self.max_batch_objects.load(Ordering::Relaxed),
            max_batch_bytes: self.max_batch_bytes.load(Ordering::Relaxed),
            average_batch_fill_ppm: 0,
            queue_delay_ns: self.queue_delay_ns.load(Ordering::Relaxed),
            max_queue_delay_ns: self.max_queue_delay_ns.load(Ordering::Relaxed),
            active_pipelines: self.active_pipelines.load(Ordering::Relaxed),
            draining_pipelines: self.draining_pipelines.load(Ordering::Relaxed),
            scale_out: self.scale_out.load(Ordering::Relaxed),
            scale_in: self.scale_in.load(Ordering::Relaxed),
            tail_waste_bytes: self.tail_waste_bytes.load(Ordering::Relaxed),
        }
    }
}
