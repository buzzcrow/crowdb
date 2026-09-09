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
    pub chunk_query: OperationMetrics,
    pub diskio_write: OperationMetrics,
    pub diskio_read: OperationMetrics,
    pub logical_bytes: Arc<Bandwidth>,
    pub physical_bytes: Arc<Bandwidth>,
    pub diskio_write_bytes: Arc<Bandwidth>,
    pub diskio_read_bytes: Arc<Bandwidth>,
    pub large_write_repair: Arc<LargeWriteRepairMetrics>,
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
            chunk_query: OperationMetrics::register(registry, "chunkio.chunk.query"),
            diskio_write: OperationMetrics::register(registry, "chunkio.diskio.write"),
            diskio_read: OperationMetrics::register(registry, "chunkio.diskio.read"),
            logical_bytes: registry.register_bandwidth("chunkio.object.logical.bw"),
            physical_bytes: registry.register_bandwidth("chunkio.object.physical.bw"),
            diskio_write_bytes: registry.register_bandwidth("chunkio.diskio.write.bw"),
            diskio_read_bytes: registry.register_bandwidth("chunkio.diskio.read.bw"),
            large_write_repair: Arc::new(LargeWriteRepairMetrics::register(registry)),
            small_write: Arc::new(SmallWriteMetrics::register(registry)),
        }
    }
}

/// Lock-free counters for in-line large-write segment replacement.
#[derive(Debug)]
pub struct LargeWriteRepairMetrics {
    pub(crate) attempts: Arc<Counter>,
    pub(crate) repaired_segments: Arc<Counter>,
    pub(crate) exhausted: Arc<Counter>,
    pub(crate) negative_list_hits: Arc<Counter>,
    pub(crate) discarded_segments: Arc<Counter>,
}

impl Default for LargeWriteRepairMetrics {
    fn default() -> Self {
        Self {
            attempts: Arc::new(Counter::new("chunkio.large_write.repair.attempts.c".into())),
            repaired_segments: Arc::new(Counter::new("chunkio.large_write.repair.completed.c".into())),
            exhausted: Arc::new(Counter::new("chunkio.large_write.repair.exhausted.c".into())),
            negative_list_hits: Arc::new(Counter::new(
                "chunkio.large_write.repair.negative_list_hits.c".into(),
            )),
            discarded_segments: Arc::new(Counter::new(
                "chunkio.large_write.repair.discarded_segments.c".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LargeWriteRepairMetricsSnapshot {
    pub attempts: u64,
    pub repaired_segments: u64,
    pub exhausted: u64,
    pub negative_list_hits: u64,
    pub discarded_segments: u64,
}

impl LargeWriteRepairMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            attempts: registry.register_counter("chunkio.large_write.repair.attempts.c"),
            repaired_segments: registry.register_counter("chunkio.large_write.repair.completed.c"),
            exhausted: registry.register_counter("chunkio.large_write.repair.exhausted.c"),
            negative_list_hits: registry.register_counter("chunkio.large_write.repair.negative_list_hits.c"),
            discarded_segments: registry.register_counter("chunkio.large_write.repair.discarded_segments.c"),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> LargeWriteRepairMetricsSnapshot {
        LargeWriteRepairMetricsSnapshot {
            attempts: self.attempts.snapshot().total,
            repaired_segments: self.repaired_segments.snapshot().total,
            exhausted: self.exhausted.snapshot().total,
            negative_list_hits: self.negative_list_hits.snapshot().total,
            discarded_segments: self.discarded_segments.snapshot().total,
        }
    }
}

/// Lock-free counters and gauges for the shared small-write pool.
#[derive(Debug)]
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
    pub(crate) batch_watchdog_expirations: AtomicU64,
    pub(crate) aggregate_write_requests: AtomicU64,
    pub(crate) aggregate_write_objects: AtomicU64,
    pub(crate) aggregate_write_buffers: AtomicU64,
    pub(crate) aggregate_write_logical_bytes: AtomicU64,
    pub(crate) aggregate_write_payload_bytes: AtomicU64,
    pub(crate) max_objects_per_write_request: AtomicU64,
    pub(crate) max_buffers_per_write_request: AtomicU64,
    pub(crate) queue_delay_ns: AtomicU64,
    pub(crate) max_queue_delay_ns: AtomicU64,
    pub(crate) active_pipelines: Arc<Gauge>,
    pub(crate) max_active_pipelines: AtomicU64,
    pub(crate) draining_pipelines: Arc<Gauge>,
    pub(crate) scale_out: AtomicU64,
    pub(crate) scale_in: AtomicU64,
    pub(crate) tail_waste_bytes: AtomicU64,
    pub(crate) repair_attempts: AtomicU64,
    pub(crate) repaired_replicas: AtomicU64,
    pub(crate) exhausted_repairs: AtomicU64,
    pub(crate) negative_list_hits: AtomicU64,
    pub(crate) active_repairs: AtomicU64,
    pub(crate) repair_latency_ns: AtomicU64,
    pub(crate) max_repair_latency_ns: AtomicU64,
    pub(crate) pipeline_replacements: AtomicU64,
    pub(crate) repairs_avoiding_rotation: AtomicU64,
    pub(crate) shadow_bytes: AtomicU64,
}

impl Default for SmallWriteMetrics {
    fn default() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            reserved_bytes: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            batch_objects: AtomicU64::new(0),
            batch_bytes: AtomicU64::new(0),
            max_batch_objects: AtomicU64::new(0),
            max_batch_bytes: AtomicU64::new(0),
            batch_watchdog_expirations: AtomicU64::new(0),
            aggregate_write_requests: AtomicU64::new(0),
            aggregate_write_objects: AtomicU64::new(0),
            aggregate_write_buffers: AtomicU64::new(0),
            aggregate_write_logical_bytes: AtomicU64::new(0),
            aggregate_write_payload_bytes: AtomicU64::new(0),
            max_objects_per_write_request: AtomicU64::new(0),
            max_buffers_per_write_request: AtomicU64::new(0),
            queue_delay_ns: AtomicU64::new(0),
            max_queue_delay_ns: AtomicU64::new(0),
            active_pipelines: Arc::new(Gauge::new("chunkio.small_write.active_pipelines.g".into())),
            max_active_pipelines: AtomicU64::new(0),
            draining_pipelines: Arc::new(Gauge::new("chunkio.small_write.draining_pipelines.g".into())),
            scale_out: AtomicU64::new(0),
            scale_in: AtomicU64::new(0),
            tail_waste_bytes: AtomicU64::new(0),
            repair_attempts: AtomicU64::new(0),
            repaired_replicas: AtomicU64::new(0),
            exhausted_repairs: AtomicU64::new(0),
            negative_list_hits: AtomicU64::new(0),
            active_repairs: AtomicU64::new(0),
            repair_latency_ns: AtomicU64::new(0),
            max_repair_latency_ns: AtomicU64::new(0),
            pipeline_replacements: AtomicU64::new(0),
            repairs_avoiding_rotation: AtomicU64::new(0),
            shadow_bytes: AtomicU64::new(0),
        }
    }
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
    pub batch_watchdog_expirations: u64,
    pub aggregate_write_requests: u64,
    pub aggregate_write_objects: u64,
    pub aggregate_write_buffers: u64,
    pub aggregate_write_logical_bytes: u64,
    pub aggregate_write_payload_bytes: u64,
    pub max_objects_per_write_request: u64,
    pub max_buffers_per_write_request: u64,
    pub average_batch_fill_ppm: u64,
    pub queue_delay_ns: u64,
    pub max_queue_delay_ns: u64,
    pub active_pipelines: u64,
    pub max_active_pipelines: u64,
    pub draining_pipelines: u64,
    pub scale_out: u64,
    pub scale_in: u64,
    pub tail_waste_bytes: u64,
    pub repair_attempts: u64,
    pub repaired_replicas: u64,
    pub exhausted_repairs: u64,
    pub negative_list_hits: u64,
    pub active_repairs: u64,
    pub repair_latency_ns: u64,
    pub max_repair_latency_ns: u64,
    pub pipeline_replacements: u64,
    pub repairs_avoiding_rotation: u64,
    pub shadow_bytes: u64,
}

impl SmallWriteMetrics {
    pub(crate) fn record_aggregate_write(
        &self,
        request_count: u64,
        object_count: usize,
        buffer_count: usize,
        logical_bytes: usize,
        payload_bytes: usize,
    ) {
        self.aggregate_write_requests
            .fetch_add(request_count, Ordering::Relaxed);
        self.aggregate_write_objects.fetch_add(
            (object_count as u64).saturating_mul(request_count),
            Ordering::Relaxed,
        );
        self.aggregate_write_buffers.fetch_add(
            (buffer_count as u64).saturating_mul(request_count),
            Ordering::Relaxed,
        );
        self.aggregate_write_logical_bytes.fetch_add(
            (logical_bytes as u64).saturating_mul(request_count),
            Ordering::Relaxed,
        );
        self.aggregate_write_payload_bytes.fetch_add(
            (payload_bytes as u64).saturating_mul(request_count),
            Ordering::Relaxed,
        );
        self.max_objects_per_write_request
            .fetch_max(object_count as u64, Ordering::Relaxed);
        self.max_buffers_per_write_request
            .fetch_max(buffer_count as u64, Ordering::Relaxed);
    }

    fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            active_pipelines: registry.register_gauge("chunkio.small_write.active_pipelines.g"),
            draining_pipelines: registry.register_gauge("chunkio.small_write.draining_pipelines.g"),
            ..Self::default()
        }
    }

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
            batch_watchdog_expirations: self.batch_watchdog_expirations.load(Ordering::Relaxed),
            aggregate_write_requests: self.aggregate_write_requests.load(Ordering::Relaxed),
            aggregate_write_objects: self.aggregate_write_objects.load(Ordering::Relaxed),
            aggregate_write_buffers: self.aggregate_write_buffers.load(Ordering::Relaxed),
            aggregate_write_logical_bytes: self.aggregate_write_logical_bytes.load(Ordering::Relaxed),
            aggregate_write_payload_bytes: self.aggregate_write_payload_bytes.load(Ordering::Relaxed),
            max_objects_per_write_request: self.max_objects_per_write_request.load(Ordering::Relaxed),
            max_buffers_per_write_request: self.max_buffers_per_write_request.load(Ordering::Relaxed),
            average_batch_fill_ppm: 0,
            queue_delay_ns: self.queue_delay_ns.load(Ordering::Relaxed),
            max_queue_delay_ns: self.max_queue_delay_ns.load(Ordering::Relaxed),
            active_pipelines: self.active_pipelines.snapshot(),
            max_active_pipelines: self.max_active_pipelines.load(Ordering::Relaxed),
            draining_pipelines: self.draining_pipelines.snapshot(),
            scale_out: self.scale_out.load(Ordering::Relaxed),
            scale_in: self.scale_in.load(Ordering::Relaxed),
            tail_waste_bytes: self.tail_waste_bytes.load(Ordering::Relaxed),
            repair_attempts: self.repair_attempts.load(Ordering::Relaxed),
            repaired_replicas: self.repaired_replicas.load(Ordering::Relaxed),
            exhausted_repairs: self.exhausted_repairs.load(Ordering::Relaxed),
            negative_list_hits: self.negative_list_hits.load(Ordering::Relaxed),
            active_repairs: self.active_repairs.load(Ordering::Relaxed),
            repair_latency_ns: self.repair_latency_ns.load(Ordering::Relaxed),
            max_repair_latency_ns: self.max_repair_latency_ns.load(Ordering::Relaxed),
            pipeline_replacements: self.pipeline_replacements.load(Ordering::Relaxed),
            repairs_avoiding_rotation: self.repairs_avoiding_rotation.load(Ordering::Relaxed),
            shadow_bytes: self.shadow_bytes.load(Ordering::Relaxed),
        }
    }
}
