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
    pub large_write_buffer: Arc<LargeWriteBufferMetrics>,
    pub small_write: Arc<SmallWriteMetrics>,
    pub read_recovery: Arc<ReadRecoveryMetrics>,
    pub read_flow: Arc<ReadFlowMetrics>,
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
            large_write_buffer: Arc::new(LargeWriteBufferMetrics::register(registry)),
            small_write: Arc::new(SmallWriteMetrics::register(registry)),
            read_recovery: Arc::new(ReadRecoveryMetrics::register(registry)),
            read_flow: Arc::new(ReadFlowMetrics::register(registry)),
        }
    }
}

/// Aggregate read-path work counters without object or chunk labels.
#[derive(Debug)]
pub struct ReadFlowMetrics {
    pub(crate) location_normalizations: Arc<Counter>,
    pub(crate) locations_examined: Arc<Counter>,
    pub(crate) range_locations_examined: Arc<Counter>,
    pub(crate) stream_windows: Arc<Counter>,
    pub(crate) stream_units_completed: Arc<Counter>,
    pub(crate) stream_out_of_order: Arc<Counter>,
    pub(crate) stream_credit_stalls: Arc<Counter>,
    pub(crate) stream_credit_wait_ns: Arc<Counter>,
    pub(crate) stream_bytes_reserved: Arc<Counter>,
    pub(crate) stream_bytes_released: Arc<Counter>,
    pub(crate) layout_queries: Arc<Counter>,
    pub(crate) layout_query_wait_ns: Arc<Counter>,
    pub(crate) strip_read_wait_ns: Arc<Counter>,
    pub(crate) chunk_read_wait_ns: Arc<Counter>,
    pub(crate) frame_decode_wait_ns: Arc<Counter>,
    pub(crate) frame_parse_wait_ns: Arc<Counter>,
}

/// Cumulative read-path work visible to access-server metrics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct ReadFlowMetricsSnapshot {
    pub location_normalizations: u64,
    pub locations_examined: u64,
    pub range_locations_examined: u64,
    pub stream_windows: u64,
    pub stream_units_completed: u64,
    pub stream_out_of_order: u64,
    pub stream_credit_stalls: u64,
    pub stream_credit_wait_ns: u64,
    pub stream_bytes_reserved: u64,
    pub stream_bytes_released: u64,
    pub layout_queries: u64,
    pub layout_query_wait_ns: u64,
    pub strip_read_wait_ns: u64,
    pub chunk_read_wait_ns: u64,
    pub frame_decode_wait_ns: u64,
    pub frame_parse_wait_ns: u64,
}

impl ReadFlowMetricsSnapshot {
    pub(crate) fn since(self, earlier: Self) -> Self {
        Self {
            location_normalizations: self
                .location_normalizations
                .saturating_sub(earlier.location_normalizations),
            locations_examined: self.locations_examined.saturating_sub(earlier.locations_examined),
            range_locations_examined: self
                .range_locations_examined
                .saturating_sub(earlier.range_locations_examined),
            stream_windows: self.stream_windows.saturating_sub(earlier.stream_windows),
            stream_units_completed: self
                .stream_units_completed
                .saturating_sub(earlier.stream_units_completed),
            stream_out_of_order: self
                .stream_out_of_order
                .saturating_sub(earlier.stream_out_of_order),
            stream_credit_stalls: self
                .stream_credit_stalls
                .saturating_sub(earlier.stream_credit_stalls),
            stream_credit_wait_ns: self
                .stream_credit_wait_ns
                .saturating_sub(earlier.stream_credit_wait_ns),
            stream_bytes_reserved: self
                .stream_bytes_reserved
                .saturating_sub(earlier.stream_bytes_reserved),
            stream_bytes_released: self
                .stream_bytes_released
                .saturating_sub(earlier.stream_bytes_released),
            layout_queries: self.layout_queries.saturating_sub(earlier.layout_queries),
            layout_query_wait_ns: self
                .layout_query_wait_ns
                .saturating_sub(earlier.layout_query_wait_ns),
            strip_read_wait_ns: self.strip_read_wait_ns.saturating_sub(earlier.strip_read_wait_ns),
            chunk_read_wait_ns: self.chunk_read_wait_ns.saturating_sub(earlier.chunk_read_wait_ns),
            frame_decode_wait_ns: self
                .frame_decode_wait_ns
                .saturating_sub(earlier.frame_decode_wait_ns),
            frame_parse_wait_ns: self
                .frame_parse_wait_ns
                .saturating_sub(earlier.frame_parse_wait_ns),
        }
    }
}

impl Default for ReadFlowMetrics {
    fn default() -> Self {
        Self::new(|name| Arc::new(Counter::new(name.into())))
    }
}

impl ReadFlowMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        Self::new(|name| registry.register_counter(name))
    }

    fn new(mut counter: impl FnMut(&'static str) -> Arc<Counter>) -> Self {
        Self {
            location_normalizations: counter("chunkio.read.location_normalizations.c"),
            locations_examined: counter("chunkio.read.locations_examined.c"),
            range_locations_examined: counter("chunkio.read.range_locations_examined.c"),
            stream_windows: counter("chunkio.read.stream_windows.c"),
            stream_units_completed: counter("chunkio.read.stream_units_completed.c"),
            stream_out_of_order: counter("chunkio.read.stream_out_of_order.c"),
            stream_credit_stalls: counter("chunkio.read.stream_credit_stalls.c"),
            stream_credit_wait_ns: counter("chunkio.read.stream_credit_wait_ns.c"),
            stream_bytes_reserved: counter("chunkio.read.stream_bytes_reserved.c"),
            stream_bytes_released: counter("chunkio.read.stream_bytes_released.c"),
            layout_queries: counter("chunkio.read.layout_queries.c"),
            layout_query_wait_ns: counter("chunkio.read.layout_query_wait_ns.c"),
            strip_read_wait_ns: counter("chunkio.read.strip_read_wait_ns.c"),
            chunk_read_wait_ns: counter("chunkio.read.chunk_read_wait_ns.c"),
            frame_decode_wait_ns: counter("chunkio.read.frame_decode_wait_ns.c"),
            frame_parse_wait_ns: counter("chunkio.read.frame_parse_wait_ns.c"),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ReadFlowMetricsSnapshot {
        ReadFlowMetricsSnapshot {
            location_normalizations: self.location_normalizations.snapshot().total,
            locations_examined: self.locations_examined.snapshot().total,
            range_locations_examined: self.range_locations_examined.snapshot().total,
            stream_windows: self.stream_windows.snapshot().total,
            stream_units_completed: self.stream_units_completed.snapshot().total,
            stream_out_of_order: self.stream_out_of_order.snapshot().total,
            stream_credit_stalls: self.stream_credit_stalls.snapshot().total,
            stream_credit_wait_ns: self.stream_credit_wait_ns.snapshot().total,
            stream_bytes_reserved: self.stream_bytes_reserved.snapshot().total,
            stream_bytes_released: self.stream_bytes_released.snapshot().total,
            layout_queries: self.layout_queries.snapshot().total,
            layout_query_wait_ns: self.layout_query_wait_ns.snapshot().total,
            strip_read_wait_ns: self.strip_read_wait_ns.snapshot().total,
            chunk_read_wait_ns: self.chunk_read_wait_ns.snapshot().total,
            frame_decode_wait_ns: self.frame_decode_wait_ns.snapshot().total,
            frame_parse_wait_ns: self.frame_parse_wait_ns.snapshot().total,
        }
    }
}

/// Aggregate read-recovery counters; no per-chunk or per-disk labels.
#[derive(Debug)]
pub struct ReadRecoveryMetrics {
    pub(crate) slices: Arc<Counter>,
    pub(crate) full_starts: Arc<Counter>,
    pub(crate) coalesced: Arc<Counter>,
    pub(crate) bytes_reused: Arc<Counter>,
    pub(crate) rejected: Arc<Counter>,
    pub(crate) stale: Arc<Counter>,
    pub(crate) fallback_background: Arc<Counter>,
}

impl Default for ReadRecoveryMetrics {
    fn default() -> Self {
        Self {
            slices: Arc::new(Counter::new("chunkio.read_recovery.slices.c".into())),
            full_starts: Arc::new(Counter::new("chunkio.read_recovery.full_starts.c".into())),
            coalesced: Arc::new(Counter::new("chunkio.read_recovery.coalesced.c".into())),
            bytes_reused: Arc::new(Counter::new("chunkio.read_recovery.bytes_reused.c".into())),
            rejected: Arc::new(Counter::new("chunkio.read_recovery.rejected.c".into())),
            stale: Arc::new(Counter::new("chunkio.read_recovery.stale.c".into())),
            fallback_background: Arc::new(Counter::new("chunkio.read_recovery.fallback_background.c".into())),
        }
    }
}

impl ReadRecoveryMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            slices: registry.register_counter("chunkio.read_recovery.slices.c"),
            full_starts: registry.register_counter("chunkio.read_recovery.full_starts.c"),
            coalesced: registry.register_counter("chunkio.read_recovery.coalesced.c"),
            bytes_reused: registry.register_counter("chunkio.read_recovery.bytes_reused.c"),
            rejected: registry.register_counter("chunkio.read_recovery.rejected.c"),
            stale: registry.register_counter("chunkio.read_recovery.stale.c"),
            fallback_background: registry.register_counter("chunkio.read_recovery.fallback_background.c"),
        }
    }
}

/// Lock-free counters distinguishing owner views from payload-copy fallback.
#[derive(Debug)]
pub struct LargeWriteBufferMetrics {
    pub(crate) framed_owners: Arc<Counter>,
    pub(crate) framed_views: Arc<Counter>,
    pub(crate) framed_payload_bytes: Arc<Counter>,
    pub(crate) payload_copy_operations: Arc<Counter>,
    pub(crate) payload_copy_bytes: Arc<Counter>,
}

impl Default for LargeWriteBufferMetrics {
    fn default() -> Self {
        Self::new(|name| Arc::new(Counter::new(name.into())))
    }
}

impl LargeWriteBufferMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        Self::new(|name| registry.register_counter(name))
    }

    fn new(mut counter: impl FnMut(&'static str) -> Arc<Counter>) -> Self {
        Self {
            framed_owners: counter("chunkio.large_write.buffer.framed_owners.c"),
            framed_views: counter("chunkio.large_write.buffer.framed_views.c"),
            framed_payload_bytes: counter("chunkio.large_write.buffer.framed_payload_bytes.c"),
            payload_copy_operations: counter("chunkio.large_write.buffer.payload_copy_operations.c"),
            payload_copy_bytes: counter("chunkio.large_write.buffer.payload_copy_bytes.c"),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> LargeWriteBufferMetricsSnapshot {
        LargeWriteBufferMetricsSnapshot {
            framed_owners: self.framed_owners.snapshot().total,
            framed_views: self.framed_views.snapshot().total,
            framed_payload_bytes: self.framed_payload_bytes.snapshot().total,
            payload_copy_operations: self.payload_copy_operations.snapshot().total,
            payload_copy_bytes: self.payload_copy_bytes.snapshot().total,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LargeWriteBufferMetricsSnapshot {
    pub framed_owners: u64,
    pub framed_views: u64,
    pub framed_payload_bytes: u64,
    pub payload_copy_operations: u64,
    pub payload_copy_bytes: u64,
}

/// Lock-free counters for in-line large-write segment replacement.
#[derive(Debug)]
pub struct LargeWriteRepairMetrics {
    pub(crate) attempts: Arc<Counter>,
    pub(crate) repaired_segments: Arc<Counter>,
    pub(crate) exhausted: Arc<Counter>,
    pub(crate) negative_list_hits: Arc<Counter>,
    pub(crate) discarded_segments: Arc<Counter>,
    pub(crate) chunk_rotations: Arc<Counter>,
    pub(crate) rotated_chunks: Arc<Counter>,
    pub(crate) replayed_bytes: Arc<Counter>,
    pub(crate) rotation_ns: Arc<Counter>,
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
            chunk_rotations: Arc::new(Counter::new(
                "chunkio.large_write.repair.chunk_rotations.c".into(),
            )),
            rotated_chunks: Arc::new(Counter::new("chunkio.large_write.repair.rotated_chunks.c".into())),
            replayed_bytes: Arc::new(Counter::new("chunkio.large_write.repair.replayed_bytes.c".into())),
            rotation_ns: Arc::new(Counter::new("chunkio.large_write.repair.rotation_ns.c".into())),
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
    pub chunk_rotations: u64,
    pub rotated_chunks: u64,
    pub replayed_bytes: u64,
    pub rotation_ns: u64,
}

impl LargeWriteRepairMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            attempts: registry.register_counter("chunkio.large_write.repair.attempts.c"),
            repaired_segments: registry.register_counter("chunkio.large_write.repair.completed.c"),
            exhausted: registry.register_counter("chunkio.large_write.repair.exhausted.c"),
            negative_list_hits: registry.register_counter("chunkio.large_write.repair.negative_list_hits.c"),
            discarded_segments: registry.register_counter("chunkio.large_write.repair.discarded_segments.c"),
            chunk_rotations: registry.register_counter("chunkio.large_write.repair.chunk_rotations.c"),
            rotated_chunks: registry.register_counter("chunkio.large_write.repair.rotated_chunks.c"),
            replayed_bytes: registry.register_counter("chunkio.large_write.repair.replayed_bytes.c"),
            rotation_ns: registry.register_counter("chunkio.large_write.repair.rotation_ns.c"),
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
            chunk_rotations: self.chunk_rotations.snapshot().total,
            rotated_chunks: self.rotated_chunks.snapshot().total,
            replayed_bytes: self.replayed_bytes.snapshot().total,
            rotation_ns: self.rotation_ns.snapshot().total,
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
    pub(crate) foreground_parity_bytes: AtomicU64,
    pub(crate) reservation_requests: AtomicU64,
    pub(crate) reservation_wait_ns: AtomicU64,
    pub(crate) first_reservation_ns: AtomicU64,
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
            foreground_parity_bytes: AtomicU64::new(0),
            reservation_requests: AtomicU64::new(0),
            reservation_wait_ns: AtomicU64::new(0),
            first_reservation_ns: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
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
    pub foreground_parity_bytes: u64,
    pub reservation_requests: u64,
    pub reservation_wait_ns: u64,
    pub first_reservation_ns: u64,
}

impl SmallWriteMetrics {
    pub(crate) fn record_reservation_wait(&self, elapsed: std::time::Duration) {
        let elapsed_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.reservation_requests.fetch_add(1, Ordering::Relaxed);
        self.reservation_wait_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        let _ = self.first_reservation_ns.compare_exchange(
            0,
            elapsed_ns.max(1),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_batch(&self, object_count: usize, logical_bytes: usize) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.batch_objects
            .fetch_add(object_count as u64, Ordering::Relaxed);
        self.max_batch_objects
            .fetch_max(object_count as u64, Ordering::Relaxed);
        self.batch_bytes
            .fetch_add(logical_bytes as u64, Ordering::Relaxed);
        self.max_batch_bytes
            .fetch_max(logical_bytes as u64, Ordering::Relaxed);
    }

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
            foreground_parity_bytes: self.foreground_parity_bytes.load(Ordering::Relaxed),
            reservation_requests: self.reservation_requests.load(Ordering::Relaxed),
            reservation_wait_ns: self.reservation_wait_ns.load(Ordering::Relaxed),
            first_reservation_ns: self.first_reservation_ns.load(Ordering::Relaxed),
        }
    }
}
