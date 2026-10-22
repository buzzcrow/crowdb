// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free window and cumulative nanosecond histograms.
//!
//! Live snapshots/flushes are approximate while writers are running. Stop or
//! join producers before taking the exact final snapshot. Owned `HistogramData`
//! shards avoid shared atomics and merge after their polling workers stop.

use std::sync::atomic::{AtomicU64, Ordering};

use super::MetricName;

mod data;
use data::{bucket_index, NUM_BUCKETS};
pub use data::{HistogramData, HistogramSnapshot};

#[derive(Debug)]
struct AtomicHistogram {
    buckets: [AtomicU64; NUM_BUCKETS],
    sum_low: AtomicU64,
    sum_high: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}

impl AtomicHistogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_low: AtomicU64::new(0),
            sum_high: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }

    fn observe(&self, ns: u64) {
        self.buckets[bucket_index(ns)].fetch_add(1, Ordering::Relaxed);
        let previous = self.sum_low.fetch_add(ns, Ordering::Relaxed);
        if previous > u64::MAX - ns {
            self.sum_high.fetch_add(1, Ordering::Relaxed);
        }
        if ns < self.min.load(Ordering::Relaxed) {
            self.min.fetch_min(ns, Ordering::Relaxed);
        }
        if ns > self.max.load(Ordering::Relaxed) {
            self.max.fetch_max(ns, Ordering::Relaxed);
        }
    }

    fn capture(&self, reset: bool) -> HistogramData {
        let get = |atomic: &AtomicU64, empty| {
            if reset {
                atomic.swap(empty, Ordering::Relaxed)
            } else {
                atomic.load(Ordering::Relaxed)
            }
        };
        HistogramData {
            buckets: self.buckets.iter().map(|b| get(b, 0)).collect(),
            sum: (u128::from(get(&self.sum_high, 0)) << 64) | u128::from(get(&self.sum_low, 0)),
            min: get(&self.min, u64::MAX),
            max: get(&self.max, 0),
        }
    }
}

#[derive(Debug)]
pub struct LatencyHistogram {
    name: MetricName,
    window: AtomicHistogram,
    total: AtomicHistogram,
}

impl LatencyHistogram {
    #[must_use]
    pub fn new(name: MetricName) -> Self {
        Self {
            name,
            window: AtomicHistogram::new(),
            total: AtomicHistogram::new(),
        }
    }

    pub fn observe(&self, ns: u64) {
        self.window.observe(ns);
        self.total.observe(ns);
    }

    pub fn flush(&self) -> HistogramSnapshot {
        self.window
            .capture(true)
            .snapshot(self.total.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum())
    }

    #[must_use]
    pub fn snapshot(&self) -> HistogramSnapshot {
        self.window
            .capture(false)
            .snapshot(self.total.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum())
    }

    #[must_use]
    pub fn snapshot_total(&self) -> HistogramSnapshot {
        self.total.capture(false).snapshot(0)
    }

    /// Capture a mergeable cumulative shard. Join writers for an exact result.
    #[must_use]
    pub fn capture_total(&self) -> HistogramData {
        self.total.capture(false)
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}
