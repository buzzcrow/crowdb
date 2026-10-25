// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Process-wide phase histograms, registered once and observed without locks.

use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crowdb_common::metrics::{HistogramSnapshot, LatencyHistogram, MetricsRegistry};

pub(crate) struct StreamPerfMetrics {
    pub(crate) append: Arc<LatencyHistogram>,
    pub(crate) queue: Arc<LatencyHistogram>,
    pub(crate) capacity: Arc<LatencyHistogram>,
    pub(crate) capacity_wait: Arc<LatencyHistogram>,
    pub(crate) preparation: Arc<LatencyHistogram>,
    pub(crate) successor_prepare: Arc<LatencyHistogram>,
    pub(crate) successor_wait: Arc<LatencyHistogram>,
    pub(crate) growth: Arc<LatencyHistogram>,
    pub(crate) initialize: Arc<LatencyHistogram>,
    pub(crate) seal: Arc<LatencyHistogram>,
    pub(crate) seal_drain: Arc<LatencyHistogram>,
    pub(crate) batch: Arc<LatencyHistogram>,
    pub(crate) encode: Arc<LatencyHistogram>,
    pub(crate) mirror: Arc<LatencyHistogram>,
    pub(crate) cursor: Arc<LatencyHistogram>,
    pub(crate) local_view: Arc<LatencyHistogram>,
    pub(crate) checkpoint: Arc<LatencyHistogram>,
    pub(crate) publish: Arc<LatencyHistogram>,
    pub(crate) record_build: Arc<LatencyHistogram>,
    pub(crate) record_get: Arc<LatencyHistogram>,
    pub(crate) record_scan: Arc<LatencyHistogram>,
    pub(crate) record_cas: Arc<LatencyHistogram>,
    pub(crate) read: Arc<LatencyHistogram>,
    pub(crate) read_plan: Arc<LatencyHistogram>,
    pub(crate) read_io: Arc<LatencyHistogram>,
}

fn histogram(name: &'static str) -> Arc<LatencyHistogram> {
    Arc::new(LatencyHistogram::new(name.into()))
}

pub(crate) fn perf() -> &'static StreamPerfMetrics {
    static METRICS: OnceLock<StreamPerfMetrics> = OnceLock::new();
    METRICS.get_or_init(|| StreamPerfMetrics {
        append: histogram("stream.append.e2e.lh"),
        queue: histogram("stream.append.queue.lh"),
        capacity: histogram("stream.append.capacity.lh"),
        capacity_wait: histogram("stream.capacity.wait.lh"),
        preparation: histogram("stream.capacity.prepare.lh"),
        successor_prepare: histogram("stream.successor.prepare.lh"),
        successor_wait: histogram("stream.successor.wait.lh"),
        growth: histogram("stream.capacity.grow.lh"),
        initialize: histogram("stream.capacity.initialize.lh"),
        seal: histogram("stream.chunk.seal.lh"),
        seal_drain: histogram("stream.chunk.seal_drain.lh"),
        batch: histogram("stream.append.batch.lh"),
        encode: histogram("stream.append.encode.lh"),
        mirror: histogram("stream.append.mirror.lh"),
        cursor: histogram("stream.checkpoint.cursor.lh"),
        local_view: histogram("stream.append.local_view.lh"),
        checkpoint: histogram("stream.checkpoint.lh"),
        publish: histogram("stream.metadata.publish.lh"),
        record_build: histogram("stream.metadata.record_build.lh"),
        record_get: histogram("stream.metadata.record_get.lh"),
        record_scan: histogram("stream.metadata.record_scan.lh"),
        record_cas: histogram("stream.metadata.record_cas.lh"),
        read: histogram("stream.read.e2e.lh"),
        read_plan: histogram("stream.read.plan.lh"),
        read_io: histogram("stream.read.io.lh"),
    })
}

/// Cumulative process-wide latency distributions, including failed operations.
/// Values are nanoseconds; periodic perf flushes do not reset these snapshots.
/// Queue samples end when the worker takes each request. A later request can
/// wait behind another request's capacity preparation; the first request's own
/// preparation is recorded separately. Preparation overlaps ordinary appends.
/// Batch and metadata stages are nested, so their percentiles are not additive.
#[must_use]
pub fn stream_perf_metrics() -> Vec<(&'static str, HistogramSnapshot)> {
    histograms()
        .into_iter()
        .map(|(name, histogram)| (name, histogram.snapshot_total()))
        .collect()
}

/// Include the process stream histograms in the service's periodic perf log.
/// Registration is a startup operation; observations only use atomic counters.
///
/// # Panics
/// Panics if a stream metric name was registered with another histogram.
pub fn register_stream_perf_metrics(registry: &mut MetricsRegistry) {
    for (_, histogram) in histograms() {
        registry.register_shared_histogram(Arc::clone(histogram));
    }
}

fn histograms() -> [(&'static str, &'static Arc<LatencyHistogram>); 25] {
    let metrics = perf();
    [
        ("stream.append.e2e.lh", &metrics.append),
        ("stream.append.queue.lh", &metrics.queue),
        ("stream.append.capacity.lh", &metrics.capacity),
        ("stream.capacity.wait.lh", &metrics.capacity_wait),
        ("stream.capacity.prepare.lh", &metrics.preparation),
        ("stream.successor.prepare.lh", &metrics.successor_prepare),
        ("stream.successor.wait.lh", &metrics.successor_wait),
        ("stream.capacity.grow.lh", &metrics.growth),
        ("stream.capacity.initialize.lh", &metrics.initialize),
        ("stream.chunk.seal.lh", &metrics.seal),
        ("stream.chunk.seal_drain.lh", &metrics.seal_drain),
        ("stream.append.batch.lh", &metrics.batch),
        ("stream.append.encode.lh", &metrics.encode),
        ("stream.append.mirror.lh", &metrics.mirror),
        ("stream.checkpoint.cursor.lh", &metrics.cursor),
        ("stream.append.local_view.lh", &metrics.local_view),
        ("stream.checkpoint.lh", &metrics.checkpoint),
        ("stream.metadata.publish.lh", &metrics.publish),
        ("stream.metadata.record_build.lh", &metrics.record_build),
        ("stream.metadata.record_get.lh", &metrics.record_get),
        ("stream.metadata.record_scan.lh", &metrics.record_scan),
        ("stream.metadata.record_cas.lh", &metrics.record_cas),
        ("stream.read.e2e.lh", &metrics.read),
        ("stream.read.plan.lh", &metrics.read_plan),
        ("stream.read.io.lh", &metrics.read_io),
    ]
}

pub(crate) struct PhaseTimer<'a> {
    histogram: &'a LatencyHistogram,
    started: Instant,
}

impl<'a> PhaseTimer<'a> {
    pub(crate) fn start(histogram: &'a LatencyHistogram) -> Self {
        Self {
            histogram,
            started: Instant::now(),
        }
    }
}

impl Drop for PhaseTimer<'_> {
    fn drop(&mut self) {
        observe(self.histogram, self.started);
    }
}

pub(crate) fn observe(histogram: &LatencyHistogram, started: Instant) {
    histogram.observe(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
}
