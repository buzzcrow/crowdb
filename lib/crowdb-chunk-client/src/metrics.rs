// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Aggregate chunk-client write-path metrics.

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
        }
    }
}
