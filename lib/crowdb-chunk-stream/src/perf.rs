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
    pub(crate) batch: Arc<LatencyHistogram>,
    pub(crate) encode: Arc<LatencyHistogram>,
    pub(crate) mirror: Arc<LatencyHistogram>,
    pub(crate) cursor: Arc<LatencyHistogram>,
    pub(crate) local_view: Arc<LatencyHistogram>,
    pub(crate) checkpoint: Arc<LatencyHistogram>,
    pub(crate) publish: Arc<LatencyHistogram>,
    pub(crate) directory_build: Arc<LatencyHistogram>,
    pub(crate) page_cas: Arc<LatencyHistogram>,
    pub(crate) head_get: Arc<LatencyHistogram>,
    pub(crate) head_cas: Arc<LatencyHistogram>,
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
        batch: histogram("stream.append.batch.lh"),
        encode: histogram("stream.append.encode.lh"),
        mirror: histogram("stream.append.mirror.lh"),
        cursor: histogram("stream.append.cursor.lh"),
        local_view: histogram("stream.append.local_view.lh"),
        checkpoint: histogram("stream.checkpoint.lh"),
        publish: histogram("stream.metadata.publish.lh"),
        directory_build: histogram("stream.metadata.build.lh"),
        page_cas: histogram("stream.metadata.page_cas.lh"),
        head_get: histogram("stream.metadata.head_get.lh"),
        head_cas: histogram("stream.metadata.head_cas.lh"),
        read: histogram("stream.read.e2e.lh"),
        read_plan: histogram("stream.read.plan.lh"),
        read_io: histogram("stream.read.io.lh"),
    })
}

/// Cumulative process-wide latency distributions, including failed operations.
/// Values are nanoseconds; periodic perf flushes do not reset these snapshots.
/// Queue samples end at batch dispatch and include capacity preparation.
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

fn histograms() -> [(&'static str, &'static Arc<LatencyHistogram>); 17] {
    let metrics = perf();
    [
        ("stream.append.e2e.lh", &metrics.append),
        ("stream.append.queue.lh", &metrics.queue),
        ("stream.append.capacity.lh", &metrics.capacity),
        ("stream.append.batch.lh", &metrics.batch),
        ("stream.append.encode.lh", &metrics.encode),
        ("stream.append.mirror.lh", &metrics.mirror),
        ("stream.append.cursor.lh", &metrics.cursor),
        ("stream.append.local_view.lh", &metrics.local_view),
        ("stream.checkpoint.lh", &metrics.checkpoint),
        ("stream.metadata.publish.lh", &metrics.publish),
        ("stream.metadata.build.lh", &metrics.directory_build),
        ("stream.metadata.page_cas.lh", &metrics.page_cas),
        ("stream.metadata.head_get.lh", &metrics.head_get),
        ("stream.metadata.head_cas.lh", &metrics.head_cas),
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
