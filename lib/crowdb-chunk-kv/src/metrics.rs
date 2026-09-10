// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct PartitionMetrics {
    mutation_requests: AtomicU64,
    mutation_applied: AtomicU64,
    condition_failed: AtomicU64,
    range_rejects: AtomicU64,
    stale_epochs: AtomicU64,
    admission_backpressure: AtomicU64,
    write_stalls: AtomicU64,
    apply_unknown: AtomicU64,
    recoveries: AtomicU64,
    checkpoints: AtomicU64,
    split_begins: AtomicU64,
    split_fences: AtomicU64,
    split_commits: AtomicU64,
    split_aborts: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartitionMetricsSnapshot {
    pub mutation_requests: u64,
    pub mutation_applied: u64,
    pub condition_failed: u64,
    pub range_rejects: u64,
    pub stale_epochs: u64,
    pub admission_backpressure: u64,
    pub write_stalls: u64,
    pub apply_unknown: u64,
    pub recoveries: u64,
    pub checkpoints: u64,
    pub split_begins: u64,
    pub split_fences: u64,
    pub split_commits: u64,
    pub split_aborts: u64,
}

impl PartitionMetrics {
    #[must_use]
    pub fn snapshot(&self) -> PartitionMetricsSnapshot {
        PartitionMetricsSnapshot {
            mutation_requests: self.mutation_requests.load(Ordering::Relaxed),
            mutation_applied: self.mutation_applied.load(Ordering::Relaxed),
            condition_failed: self.condition_failed.load(Ordering::Relaxed),
            range_rejects: self.range_rejects.load(Ordering::Relaxed),
            stale_epochs: self.stale_epochs.load(Ordering::Relaxed),
            admission_backpressure: self.admission_backpressure.load(Ordering::Relaxed),
            write_stalls: self.write_stalls.load(Ordering::Relaxed),
            apply_unknown: self.apply_unknown.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
            split_begins: self.split_begins.load(Ordering::Relaxed),
            split_fences: self.split_fences.load(Ordering::Relaxed),
            split_commits: self.split_commits.load(Ordering::Relaxed),
            split_aborts: self.split_aborts.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn mutation_request(&self) {
        self.mutation_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn mutation_result(&self, applied: bool) {
        let counter = if applied {
            &self.mutation_applied
        } else {
            &self.condition_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn range_reject(&self) {
        self.range_rejects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn stale_epoch(&self) {
        self.stale_epochs.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn admission_backpressure(&self) {
        self.admission_backpressure.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn write_stall(&self) {
        self.write_stalls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn apply_unknown(&self) {
        self.apply_unknown.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn recovery(&self) {
        self.recoveries.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn checkpoint(&self) {
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn split_begin(&self) {
        self.split_begins.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn split_fence(&self) {
        self.split_fences.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn split_commit(&self) {
        self.split_commits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn split_abort(&self) {
        self.split_aborts.fetch_add(1, Ordering::Relaxed);
    }
}
