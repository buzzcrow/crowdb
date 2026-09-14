// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};
use std::time::Duration;

use crowdb_chunk_stream::StreamMetricsSnapshot;

use super::config::Workload;

#[derive(Default)]
pub struct TaskResult {
    pub operations: u64,
    pub bytes: u64,
    pub errors: u64,
    pub latencies_us: Vec<u64>,
}

impl TaskResult {
    pub fn merge(&mut self, mut other: Self) {
        self.operations += other.operations;
        self.bytes += other.bytes;
        self.errors += other.errors;
        self.latencies_us.append(&mut other.latencies_us);
    }
}

pub struct BenchResult {
    pub workload: Workload,
    pub operations: u64,
    pub bytes: u64,
    pub errors: u64,
    pub elapsed: Duration,
    pub latency_avg_us: u64,
    pub latency_p50_us: u64,
    pub latency_p99_us: u64,
    pub rss_start_kib: u64,
    pub rss_end_kib: u64,
    pub rss_peak_kib: u64,
    pub metrics: StreamMetricsSnapshot,
}

impl BenchResult {
    pub fn from_task(
        workload: Workload,
        elapsed: Duration,
        rss_start_kib: u64,
        rss_end_kib: u64,
        rss_peak_kib: u64,
        metrics: StreamMetricsSnapshot,
        mut task: TaskResult,
    ) -> Self {
        task.latencies_us.sort_unstable();
        let latency_avg_us = if task.latencies_us.is_empty() {
            0
        } else {
            task.latencies_us.iter().sum::<u64>() / task.latencies_us.len() as u64
        };
        Self {
            workload,
            operations: task.operations,
            bytes: task.bytes,
            errors: task.errors,
            elapsed,
            latency_avg_us,
            latency_p50_us: percentile(&task.latencies_us, 50),
            latency_p99_us: percentile(&task.latencies_us, 99),
            rss_start_kib,
            rss_end_kib,
            rss_peak_kib,
            metrics,
        }
    }
}

impl Display for BenchResult {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let seconds = self.elapsed.as_secs_f64();
        write!(
            formatter,
            "chunk-stream: workload={} operations={} bytes={} errors={} seconds={:.3} ops_s={:.2} mib_s={:.2} avg_us={} p50_us={} p99_us={} rss_start_kib={} rss_end_kib={} rss_peak_kib={} max_queue_requests={} max_queue_bytes={} batches={} batch_requests={} rollovers={} metadata_publications={} cache_hits={} cache_misses={} physical_reads={} reclaimed_bytes={} watchdogs={}",
            self.workload,
            self.operations,
            self.bytes,
            self.errors,
            seconds,
            as_f64(self.operations) / seconds,
            as_f64(self.bytes) / seconds / (1024.0 * 1024.0),
            self.latency_avg_us,
            self.latency_p50_us,
            self.latency_p99_us,
            self.rss_start_kib,
            self.rss_end_kib,
            self.rss_peak_kib,
            self.metrics.max_queued_requests,
            self.metrics.max_queued_bytes,
            self.metrics.batches,
            self.metrics.batch_requests,
            self.metrics.rollovers,
            self.metrics.metadata_publications,
            self.metrics.extent_page_cache_hits,
            self.metrics.extent_page_cache_misses,
            self.metrics.physical_read_requests,
            self.metrics.reclaimed_bytes,
            self.metrics.watchdog_observations,
        )
    }
}

fn as_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}
