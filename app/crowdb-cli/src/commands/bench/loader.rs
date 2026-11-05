// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metrics recorder retained by the chunk and disk workload adapters.

use std::sync::Arc;

use crowdb_common::metrics::{Bandwidth, Counter, LatencyHistogram, MetricsRegistry};

/// Shared error counters + latency histogram + bandwidth + running flag
/// for one bench run. All fields are lock-free atomics — safe to update
/// from many tokio tasks or C++ I/O worker threads concurrently.
pub struct BenchRecorder {
    errors: Arc<Counter>,
    correctness_errors: Arc<Counter>,
    /// Latency histogram (registered → window flushed each interval,
    /// cumulative preserved for final report via `snapshot_total()`).
    /// Its `count` is the success-ops count (every `record_ok` observes).
    latency: Arc<LatencyHistogram>,
    /// Payload bandwidth (bytes/s, registered → flushed each interval).
    bytes: Arc<Bandwidth>,
}

impl BenchRecorder {
    /// Build a recorder: all metrics registered on `reg` so the
    /// `MetricsRunner` periodic flush writes them to the metrics log.
    #[must_use]
    pub fn from_registry(reg: &mut MetricsRegistry) -> Self {
        Self {
            errors: reg.register_counter("bench.ops.error.c"),
            correctness_errors: reg.register_counter("bench.ops.correctness_error.c"),
            latency: reg.register_histogram("bench.latency.lh"),
            bytes: reg.register_bandwidth("bench.bytes.bw"),
        }
    }

    /// Record one successful op with its latency (µs) and payload bytes.
    pub fn record_ok(&self, us: u64, bytes: u64) {
        self.record_ok_ns(us.saturating_mul(1000), bytes);
    }

    /// Preserve nanosecond precision for RPC completion callbacks.
    pub fn record_ok_ns(&self, ns: u64, bytes: u64) {
        self.latency.observe(ns);
        self.bytes.observe(bytes);
    }

    /// Record one failed op (transport / consensus error).
    pub fn record_err(&self) {
        self.errors.inc();
    }

    /// Record one correctness mismatch (value bytes differ from expected).
    #[allow(dead_code)]
    pub fn record_correctness_err(&self) {
        self.correctness_errors.inc();
    }
}
