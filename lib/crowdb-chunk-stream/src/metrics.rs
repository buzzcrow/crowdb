// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free counters for one logical stream handle.
#[derive(Debug, Default)]
pub struct StreamMetrics {
    pub(crate) submitted: AtomicU64,
    pub(crate) completed: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) logical_append_bytes: AtomicU64,
    pub(crate) physical_append_bytes: AtomicU64,
    pub(crate) batches: AtomicU64,
    pub(crate) batch_requests: AtomicU64,
    pub(crate) watchdog_observations: AtomicU64,
    pub(crate) rollovers: AtomicU64,
    pub(crate) read_bytes: AtomicU64,
    pub(crate) reclaimed_bytes: AtomicU64,
    pub(crate) max_queued_requests: AtomicU64,
    pub(crate) max_queued_bytes: AtomicU64,
    pub(crate) metadata_publications: AtomicU64,
    pub(crate) extent_page_cache_hits: AtomicU64,
    pub(crate) extent_page_cache_misses: AtomicU64,
    pub(crate) physical_read_requests: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamMetricsSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub logical_append_bytes: u64,
    pub physical_append_bytes: u64,
    pub batches: u64,
    pub batch_requests: u64,
    pub watchdog_observations: u64,
    pub rollovers: u64,
    pub read_bytes: u64,
    pub reclaimed_bytes: u64,
    pub max_queued_requests: u64,
    pub max_queued_bytes: u64,
    pub metadata_publications: u64,
    pub extent_page_cache_hits: u64,
    pub extent_page_cache_misses: u64,
    pub physical_read_requests: u64,
}

impl StreamMetrics {
    #[must_use]
    pub fn snapshot(&self) -> StreamMetricsSnapshot {
        StreamMetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            logical_append_bytes: self.logical_append_bytes.load(Ordering::Relaxed),
            physical_append_bytes: self.physical_append_bytes.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            batch_requests: self.batch_requests.load(Ordering::Relaxed),
            watchdog_observations: self.watchdog_observations.load(Ordering::Relaxed),
            rollovers: self.rollovers.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            reclaimed_bytes: self.reclaimed_bytes.load(Ordering::Relaxed),
            max_queued_requests: self.max_queued_requests.load(Ordering::Relaxed),
            max_queued_bytes: self.max_queued_bytes.load(Ordering::Relaxed),
            metadata_publications: self.metadata_publications.load(Ordering::Relaxed),
            extent_page_cache_hits: self.extent_page_cache_hits.load(Ordering::Relaxed),
            extent_page_cache_misses: self.extent_page_cache_misses.load(Ordering::Relaxed),
            physical_read_requests: self.physical_read_requests.load(Ordering::Relaxed),
        }
    }
}
