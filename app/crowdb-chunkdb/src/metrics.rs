// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lifecycle lock + cache metrics for chunkdb observability.
//! Hot-path counters are `AtomicU64` with `Relaxed` ordering;
//! latency histograms are `Mutex<PreciseHistogram>` (rare contention).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crowdb_common::metrics::PreciseHistogram;
use crowdb_common::metrics::{Counter, Gauge, LatencyHistogram, MetricsRegistry};
use serde::{Deserialize, Serialize};

/// Registered `ChunkDB` RPC methods, used as stable request-metric indices.
#[derive(Clone, Copy)]
pub enum RequestKind {
    AllocateChunk,
    AppendChunk,
    AdvanceChunkWrite,
    QueryChunk,
    SealChunk,
    DeleteChunk,
    DeleteChunkRange,
    UpdateChunkStrip,
    ListChunks,
}

impl RequestKind {
    const ALL: [Self; 9] = [
        Self::AllocateChunk,
        Self::AppendChunk,
        Self::AdvanceChunkWrite,
        Self::QueryChunk,
        Self::SealChunk,
        Self::DeleteChunk,
        Self::DeleteChunkRange,
        Self::UpdateChunkStrip,
        Self::ListChunks,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::AllocateChunk => "allocate_chunk",
            Self::AppendChunk => "append_chunk",
            Self::AdvanceChunkWrite => "advance_chunk_write",
            Self::QueryChunk => "query_chunk",
            Self::SealChunk => "seal_chunk",
            Self::DeleteChunk => "delete_chunk",
            Self::DeleteChunkRange => "delete_chunk_range",
            Self::UpdateChunkStrip => "update_chunk_strip",
            Self::ListChunks => "list_chunks",
        }
    }

    const fn index(self) -> usize {
        self as usize
    }
}

struct RequestMetric {
    latency: Arc<LatencyHistogram>,
    inflight: Arc<Gauge>,
    errors: Arc<Counter>,
}

/// Uniform completed-request latency/count, inflight, and error metrics.
pub struct RequestMetrics {
    methods: [RequestMetric; 9],
}

impl RequestMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        let methods = RequestKind::ALL.map(|kind| {
            let prefix = format!("request.{}", kind.name());
            RequestMetric {
                latency: registry.register_histogram(format!("{prefix}.lh")),
                inflight: registry.register_gauge(format!("{prefix}.inflight.g")),
                errors: registry.register_counter(format!("{prefix}.errors.c")),
            }
        });
        Self { methods }
    }

    /// Start accounting for a request.
    #[must_use]
    pub fn start(&self, kind: RequestKind) -> RequestGuard {
        let metric = &self.methods[kind.index()];
        metric.inflight.inc();
        RequestGuard {
            latency: Arc::clone(&metric.latency),
            inflight: Arc::clone(&metric.inflight),
            errors: Arc::clone(&metric.errors),
            started: std::time::Instant::now(),
            success: false,
        }
    }
}

/// Completes request accounting on every synchronous or asynchronous exit.
pub struct RequestGuard {
    latency: Arc<LatencyHistogram>,
    inflight: Arc<Gauge>,
    errors: Arc<Counter>,
    started: std::time::Instant,
    success: bool,
}

impl RequestGuard {
    /// Mark the request response as successful.
    pub fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !self.success {
            self.errors.inc();
        }
        self.latency
            .observe(self.started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        self.inflight.dec();
    }
}

/// Metrics for the ChunkDB RPC and allocation workflow.
#[derive(Clone)]
pub struct ChunkdbMetrics {
    pub requests: Arc<RequestMetrics>,
    pub conversion: Arc<ConversionMetrics>,
    pub repair: Arc<RepairMetrics>,
    pub allocate_inflight: Arc<Gauge>,
    pub allocate_strips: Arc<Counter>,
    pub allocate_blocks: Arc<Counter>,
    pub allocate_placement: Arc<LatencyHistogram>,
    pub allocate_diskdb_round: Arc<LatencyHistogram>,
    pub allocate_diskdb_calls: Arc<Counter>,
    pub allocate_diskdb_retries: Arc<Counter>,
    pub allocate_commit: Arc<LatencyHistogram>,
    pub allocate_commit_blocks: Arc<Counter>,
    pub allocate_commit_errors: Arc<Counter>,
    pub allocate_record_build: Arc<LatencyHistogram>,
    pub allocate_kv_persist: Arc<LatencyHistogram>,
    pub allocate_rollback: Arc<LatencyHistogram>,
    pub allocate_rollback_blocks: Arc<Counter>,
    pub allocate_errors: Arc<Counter>,
}

impl ChunkdbMetrics {
    /// Register all ChunkDB workflow metrics.
    pub fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            requests: Arc::new(RequestMetrics::register(registry)),
            conversion: Arc::new(ConversionMetrics::register(registry)),
            repair: Arc::new(RepairMetrics::register(registry)),
            allocate_inflight: registry.register_gauge("allocate.inflight.g"),
            allocate_strips: registry.register_counter("allocate.strips.c"),
            allocate_blocks: registry.register_counter("allocate.blocks.c"),
            allocate_placement: registry.register_histogram("allocate.placement.lh"),
            allocate_diskdb_round: registry.register_histogram("allocate.diskdb_round.lh"),
            allocate_diskdb_calls: registry.register_counter("allocate.diskdb_calls.c"),
            allocate_diskdb_retries: registry.register_counter("allocate.diskdb_retries.c"),
            allocate_commit: registry.register_histogram("allocate.commit.lh"),
            allocate_commit_blocks: registry.register_counter("allocate.commit_blocks.c"),
            allocate_commit_errors: registry.register_counter("allocate.commit_errors.c"),
            allocate_record_build: registry.register_histogram("allocate.record_build.lh"),
            allocate_kv_persist: registry.register_histogram("allocate.kv_persist.lh"),
            allocate_rollback: registry.register_histogram("allocate.rollback.lh"),
            allocate_rollback_blocks: registry.register_counter("allocate.rollback_blocks.c"),
            allocate_errors: registry.register_counter("allocate.errors.c"),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairMetricsSnapshot {
    pub tasks_admitted: u64,
    pub attempts_started: u64,
    pub attempts_completed: u64,
    pub attempts_failed: u64,
    pub segments_repaired: u64,
    pub bytes_written: u64,
    pub active: u64,
    pub memory_bytes: u64,
    pub memory_limit_bytes: u64,
}

pub struct RepairMetrics {
    tasks_admitted: Arc<Counter>,
    attempts_started: Arc<Counter>,
    attempts_completed: Arc<Counter>,
    attempts_failed: Arc<Counter>,
    segments_repaired: Arc<Counter>,
    bytes_written: Arc<Counter>,
    active: Arc<Gauge>,
    memory_bytes: Arc<Gauge>,
    memory_limit_bytes: Arc<Gauge>,
}

impl RepairMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            tasks_admitted: registry.register_counter("repair.tasks_admitted.c"),
            attempts_started: registry.register_counter("repair.attempts_started.c"),
            attempts_completed: registry.register_counter("repair.attempts_completed.c"),
            attempts_failed: registry.register_counter("repair.attempts_failed.c"),
            segments_repaired: registry.register_counter("repair.segments_repaired.c"),
            bytes_written: registry.register_counter("repair.bytes_written.c"),
            active: registry.register_gauge("repair.active.g"),
            memory_bytes: registry.register_gauge("repair.memory_bytes.g"),
            memory_limit_bytes: registry.register_gauge("repair.memory_limit_bytes.g"),
        }
    }

    pub(crate) fn set_memory_limit(&self, bytes: usize) {
        self.memory_limit_bytes
            .set(u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    pub(crate) fn admit(&self, count: u64) {
        self.tasks_admitted.inc_by(count);
    }

    pub(crate) fn start_attempt(&self) {
        self.attempts_started.inc();
        self.active.inc();
    }

    pub(crate) fn reserve_memory(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.memory_bytes.inc_by(bytes);
    }

    pub(crate) fn release_memory(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.memory_bytes.dec_by(bytes);
    }

    pub(crate) fn finish_attempt(&self, success: bool, segments: u64, bytes: u64) {
        if success {
            self.attempts_completed.inc();
            self.segments_repaired.inc_by(segments);
            self.bytes_written.inc_by(bytes);
        } else {
            self.attempts_failed.inc();
        }
        self.active.dec();
    }

    #[must_use]
    pub fn snapshot(&self) -> RepairMetricsSnapshot {
        RepairMetricsSnapshot {
            tasks_admitted: self.tasks_admitted.snapshot().total,
            attempts_started: self.attempts_started.snapshot().total,
            attempts_completed: self.attempts_completed.snapshot().total,
            attempts_failed: self.attempts_failed.snapshot().total,
            segments_repaired: self.segments_repaired.snapshot().total,
            bytes_written: self.bytes_written.snapshot().total,
            active: self.active.snapshot(),
            memory_bytes: self.memory_bytes.snapshot(),
            memory_limit_bytes: self.memory_limit_bytes.snapshot(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversionMetricsSnapshot {
    pub attempts_started: u64,
    pub attempts_completed: u64,
    pub attempts_failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub mirror_segments_retired: u64,
    pub active: u64,
    pub peak_active: u64,
}

pub struct ConversionMetrics {
    attempts_started: Arc<Counter>,
    attempts_completed: Arc<Counter>,
    attempts_failed: Arc<Counter>,
    bytes_read: Arc<Counter>,
    bytes_written: Arc<Counter>,
    mirror_segments_retired: Arc<Counter>,
    active: Arc<Gauge>,
    peak_active: Arc<Gauge>,
    peak_value: AtomicU64,
}

impl ConversionMetrics {
    fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            attempts_started: registry.register_counter("conversion.attempts_started.c"),
            attempts_completed: registry.register_counter("conversion.attempts_completed.c"),
            attempts_failed: registry.register_counter("conversion.attempts_failed.c"),
            bytes_read: registry.register_counter("conversion.bytes_read.c"),
            bytes_written: registry.register_counter("conversion.bytes_written.c"),
            mirror_segments_retired: registry.register_counter("conversion.mirror_segments_retired.c"),
            active: registry.register_gauge("conversion.active.g"),
            peak_active: registry.register_gauge("conversion.peak_active.g"),
            peak_value: AtomicU64::new(0),
        }
    }

    pub(crate) fn start_attempt(&self) {
        self.attempts_started.inc();
        self.active.inc();
        let active = self.active.snapshot();
        if self.peak_value.fetch_max(active, Ordering::Relaxed) < active {
            self.peak_active.set(active);
        }
    }

    pub(crate) fn finish_attempt(
        &self,
        success: bool,
        bytes_read: u64,
        bytes_written: u64,
        mirror_segments_retired: u64,
    ) {
        if success {
            self.attempts_completed.inc();
            self.bytes_read.inc_by(bytes_read);
            self.bytes_written.inc_by(bytes_written);
            self.mirror_segments_retired.inc_by(mirror_segments_retired);
        } else {
            self.attempts_failed.inc();
        }
        self.active.dec();
    }

    #[must_use]
    pub fn snapshot(&self) -> ConversionMetricsSnapshot {
        ConversionMetricsSnapshot {
            attempts_started: self.attempts_started.snapshot().total,
            attempts_completed: self.attempts_completed.snapshot().total,
            attempts_failed: self.attempts_failed.snapshot().total,
            bytes_read: self.bytes_read.snapshot().total,
            bytes_written: self.bytes_written.snapshot().total,
            mirror_segments_retired: self.mirror_segments_retired.snapshot().total,
            active: self.active.snapshot(),
            peak_active: self.peak_active.snapshot(),
        }
    }
}

/// Snapshot of [`LifecycleMetrics`] at a point in time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleMetricsSnapshot {
    pub lock_timeout_count: u64,
    pub lock_busy_count: u64,
    pub cache_hit_count: u64,
    pub cache_miss_count: u64,
    pub cache_size: u64,
    pub reap_idle_count: u64,
    pub reap_idle_entries_removed: u64,
    /// Number of cache entries invalidated (one increment per chunk
    /// removed, by both `invalidate_chunk` and `invalidate_range`).
    pub invalidate_count: u64,
    pub lock_wait_count: u64,
    pub lock_wait_p50_us: u64,
    pub lock_wait_p99_us: u64,
    pub lock_wait_max_us: u64,
    pub lock_hold_count: u64,
    pub lock_hold_p50_us: u64,
    pub lock_hold_p99_us: u64,
    pub lock_hold_max_us: u64,
}

/// Latency histograms behind a `Mutex` — `PreciseHistogram` requires
/// `&mut self` for `record()`.
#[derive(Debug)]
struct LatencyHistograms {
    lock_wait: PreciseHistogram,
    lock_hold: PreciseHistogram,
}

impl Default for LatencyHistograms {
    fn default() -> Self {
        Self {
            lock_wait: PreciseHistogram::new(3),
            lock_hold: PreciseHistogram::new(3),
        }
    }
}

/// Metrics for the per-chunk lifecycle lock + payload cache.
#[derive(Debug, Default)]
pub struct LifecycleMetrics {
    lock_timeout_count: AtomicU64,
    lock_busy_count: AtomicU64,
    cache_hit_count: AtomicU64,
    cache_miss_count: AtomicU64,
    reap_idle_count: AtomicU64,
    reap_idle_entries_removed: AtomicU64,
    invalidate_count: AtomicU64,
    lat: Mutex<LatencyHistograms>,
}

impl LifecycleMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record_lock_timeout(&self) {
        self.lock_timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_lock_busy(&self) {
        self.lock_busy_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cache_hit(&self) {
        self.cache_hit_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cache_miss(&self) {
        self.cache_miss_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reap_idle(&self, entries_removed: u64) {
        self.reap_idle_count.fetch_add(1, Ordering::Relaxed);
        self.reap_idle_entries_removed
            .fetch_add(entries_removed, Ordering::Relaxed);
    }

    pub(crate) fn record_invalidate(&self) {
        self.invalidate_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_lock_wait(&self, dur_us: u64) {
        if let Ok(mut g) = self.lat.lock() {
            g.lock_wait.record(dur_us.max(1));
        }
    }

    pub(crate) fn record_lock_hold(&self, dur_us: u64) {
        if let Ok(mut g) = self.lat.lock() {
            g.lock_hold.record(dur_us.max(1));
        }
    }

    /// Snapshot all counters + histogram percentiles. `cache_size` is
    /// passed in by the caller (read from `quick_cache::Cache::entry_count()`).
    #[must_use]
    pub fn snapshot(&self, cache_size: u64) -> LifecycleMetricsSnapshot {
        let lat = self.lat.lock().map_or_else(
            |_| LatencyHistograms::default(),
            |g| LatencyHistograms {
                lock_wait: g.lock_wait.clone(),
                lock_hold: g.lock_hold.clone(),
            },
        );
        LifecycleMetricsSnapshot {
            lock_timeout_count: self.lock_timeout_count.load(Ordering::Relaxed),
            lock_busy_count: self.lock_busy_count.load(Ordering::Relaxed),
            cache_hit_count: self.cache_hit_count.load(Ordering::Relaxed),
            cache_miss_count: self.cache_miss_count.load(Ordering::Relaxed),
            cache_size,
            reap_idle_count: self.reap_idle_count.load(Ordering::Relaxed),
            reap_idle_entries_removed: self.reap_idle_entries_removed.load(Ordering::Relaxed),
            invalidate_count: self.invalidate_count.load(Ordering::Relaxed),
            lock_wait_count: lat.lock_wait.len(),
            lock_wait_p50_us: lat.lock_wait.value_at_quantile(0.50),
            lock_wait_p99_us: lat.lock_wait.value_at_quantile(0.99),
            lock_wait_max_us: lat.lock_wait.max(),
            lock_hold_count: lat.lock_hold.len(),
            lock_hold_p50_us: lat.lock_hold.value_at_quantile(0.50),
            lock_hold_p99_us: lat.lock_hold.value_at_quantile(0.99),
            lock_hold_max_us: lat.lock_hold.max(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversion_metrics_track_attempts_io_and_peak_concurrency() {
        let mut registry = MetricsRegistry::new();
        let metrics = ConversionMetrics::register(&mut registry);
        metrics.start_attempt();
        metrics.start_attempt();
        metrics.finish_attempt(true, 8, 12, 24);
        metrics.finish_attempt(false, 100, 100, 100);

        assert_eq!(
            metrics.snapshot(),
            ConversionMetricsSnapshot {
                attempts_started: 2,
                attempts_completed: 1,
                attempts_failed: 1,
                bytes_read: 8,
                bytes_written: 12,
                mirror_segments_retired: 24,
                active: 0,
                peak_active: 2,
            }
        );
    }

    #[test]
    fn repair_metrics_track_queue_attempts_io_and_memory() {
        let mut registry = MetricsRegistry::new();
        let metrics = RepairMetrics::register(&mut registry);
        metrics.set_memory_limit(64);
        metrics.admit(2);
        metrics.start_attempt();
        metrics.reserve_memory(16);
        metrics.release_memory(16);
        metrics.finish_attempt(true, 1, 32);

        assert_eq!(
            metrics.snapshot(),
            RepairMetricsSnapshot {
                tasks_admitted: 2,
                attempts_started: 1,
                attempts_completed: 1,
                attempts_failed: 0,
                segments_repaired: 1,
                bytes_written: 32,
                active: 0,
                memory_bytes: 0,
                memory_limit_bytes: 64,
            }
        );
    }
}
