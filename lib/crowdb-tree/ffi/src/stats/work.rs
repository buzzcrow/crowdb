// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod waits;
pub use waits::{TreeWaitSnapshot, TreeWaitStats, TREE_WAIT_CLASS_NAMES};
mod counters;
mod histograms;
use counters::raw_counters;
pub use counters::{TreeWorkCounters, TREE_WORK_CAUSE_NAMES, TREE_WORK_COUNTER_NAMES};
use histograms::raw_histograms;
pub use histograms::{TreeHistogramStats, TreeWorkHistograms, TreeWorkStage};

use std::ptr::NonNull;

use crate::error::{check, CtError};
use crate::{sys, Crowdbtree};

/// Fixed v1 stage order; stages are shared across every window and totals sample.
pub const TREE_WORK_STAGE_NAMES: [&str; 21] = [
    "flush.batch",
    "flush.admission_wait",
    "flush.capture",
    "flush.writer_drain",
    "flush.handoff",
    "flush.range",
    "publish.queue_wait",
    "publish.commit",
    "maintenance.plan",
    "overlay.coalesce",
    "page.split",
    "page.merge",
    "page.layout",
    "page.encode",
    "budget.wait",
    "page.load_pipeline",
    "page.writeback_pipeline",
    "snapshot.capture",
    "snapshot.commit",
    "memory.retire_wait",
    "flush.repair",
];

/// Relaxed totals and one approximate collector window for a pipeline stage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeWorkStageStats {
    pub attempts: u64,
    pub inflight: u64,
    pub total_count: u64,
    pub total_sum_ns: u64,
    pub total_max_ns: u64,
    /// Success, error, cancelled, retry, stale, in that order.
    pub outcomes: [u64; 5],
    pub window_count: u64,
    pub window_sum_ns: u64,
    pub window_max_ns: u64,
    /// Independent non-draining age sample; see `age_sampled_at_ns`.
    pub age: TreeWorkStageAge,
}

/// Approximate fixed-capacity observation of active work. Saturation never
/// rejects work. Only a complete sample covers every active operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeWorkStageAge {
    pub inflight: u64,
    pub tracked: u64,
    pub untracked: u64,
    /// Oldest age in the observed subset, zero when none was observed.
    pub observed_oldest_age_ns: u64,
    pub complete: bool,
}

/// Immutable value shared by all consumers of one collector tick.
/// A totals-only sample has sequence zero and empty window fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeWorkSnapshot {
    pub window_sequence: u64,
    pub sampled_at_ns: u64,
    /// The age sample and cumulative/window counters are not transactional.
    pub age_sampled_at_ns: u64,
    pub stages: [TreeWorkStageStats; 21],
    pub counters: TreeWorkCounters,
    pub histograms: TreeWorkHistograms,
    pub waits: TreeWaitSnapshot,
}

/// Cumulative flush dispatch counters, independent of stage collector windows.
/// Fields are relaxed samples. `active_steps` is current concurrency and
/// `peak_steps` is its lifetime high-water mark; neither counts waiting jobs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeFlushDispatchStats {
    pub sampled_at_ns: u64,
    pub inline_probes: u64,
    pub inline_ranges: u64,
    pub queued_steps: u64,
    pub prepared_runs: u64,
    pub prepared_entries: u64,
    pub prepared_bytes: u64,
    pub cow_bytes: u64,
    pub active_steps: u64,
    pub peak_steps: u64,
}

impl From<sys::ct_tree_work_snapshot_v1> for TreeWorkSnapshot {
    fn from(raw: sys::ct_tree_work_snapshot_v1) -> Self {
        Self {
            window_sequence: raw.window_sequence,
            sampled_at_ns: raw.sampled_at_ns,
            age_sampled_at_ns: 0,
            counters: TreeWorkCounters::default(),
            histograms: TreeWorkHistograms::default(),
            waits: TreeWaitSnapshot::default(),
            stages: raw.stages.map(|stage| TreeWorkStageStats {
                attempts: stage.attempts,
                inflight: stage.inflight,
                total_count: stage.total_count,
                total_sum_ns: stage.total_sum_ns,
                total_max_ns: stage.total_max_ns,
                outcomes: stage.outcomes,
                window_count: stage.window_count,
                window_sum_ns: stage.window_sum_ns,
                window_max_ns: stage.window_max_ns,
                age: TreeWorkStageAge::default(),
            }),
        }
    }
}

impl TreeWorkSnapshot {
    fn with_ages(mut self, raw: sys::ct_tree_work_ages_v1) -> Self {
        self.age_sampled_at_ns = raw.sampled_at_ns;
        for (stage, age) in self.stages.iter_mut().zip(raw.stages) {
            stage.age = TreeWorkStageAge {
                inflight: age.inflight,
                tracked: age.tracked,
                untracked: age.untracked,
                observed_oldest_age_ns: age.observed_oldest_age_ns,
                complete: age.complete == 1,
            };
        }
        self
    }
}

fn raw_ages() -> Result<sys::ct_tree_work_ages_v1, CtError> {
    Ok(sys::ct_tree_work_ages_v1 {
        struct_size: u32::try_from(std::mem::size_of::<sys::ct_tree_work_ages_v1>())
            .map_err(|_| CtError::Internal)?,
        version: 1,
        ..Default::default()
    })
}

fn raw_snapshot() -> Result<sys::ct_tree_work_snapshot_v1, CtError> {
    Ok(sys::ct_tree_work_snapshot_v1 {
        struct_size: u32::try_from(std::mem::size_of::<sys::ct_tree_work_snapshot_v1>())
            .map_err(|_| CtError::Internal)?,
        version: 1,
        ..Default::default()
    })
}

/// Exclusive draining owner. The ledger remains valid after the Tree closes;
/// observations retained by accepted work are released with their actual owners.
#[derive(Debug)]
pub struct TreeWorkCollector {
    handle: NonNull<sys::ct_work_metrics_collector>,
}

// SAFETY: native capture uses a nonblocking atomic claim; ledger counters are
// atomic and neither capture nor destruction accesses a raw Tree pointer.
unsafe impl Send for TreeWorkCollector {}
unsafe impl Sync for TreeWorkCollector {}

impl TreeWorkCollector {
    /// Samples backing counters after Tree close without retaining storage owners.
    pub fn backing_stats(&self) -> Result<super::BackingStatsSnapshot, CtError> {
        super::backing::sample(|out| unsafe {
            sys::ct_work_metrics_collector_backing_stats(self.handle.as_ptr(), out)
        })
    }

    /// Drains one window. Concurrent captures return `Unavailable` without
    /// resetting any samples; callers distribute this immutable result.
    pub fn capture(&self) -> Result<TreeWorkSnapshot, CtError> {
        let mut raw = raw_snapshot()?;
        let mut ages = raw_ages()?;
        let mut counters = raw_counters()?;
        let mut histograms = raw_histograms()?;
        check(unsafe {
            sys::ct_work_metrics_capture_v3(
                self.handle.as_ptr(),
                &mut raw,
                &mut ages,
                &mut counters,
                &mut histograms,
            )
        })?;
        let mut snapshot = TreeWorkSnapshot::from(raw)
            .with_ages(ages)
            .with_counters(counters);
        snapshot.histograms = histograms.into();
        snapshot.waits = self.wait_metrics_totals()?;
        Ok(snapshot)
    }
}

impl Drop for TreeWorkCollector {
    fn drop(&mut self) {
        unsafe { sys::ct_work_metrics_collector_free(self.handle.as_ptr()) };
    }
}

impl Crowdbtree {
    /// Samples dispatch counters without taking a collector claim or draining it.
    pub fn flush_dispatch_stats(&self) -> Result<TreeFlushDispatchStats, CtError> {
        let mut raw = sys::ct_tree_flush_dispatch_v1 {
            struct_size: u32::try_from(std::mem::size_of::<sys::ct_tree_flush_dispatch_v1>())
                .map_err(|_| CtError::Internal)?,
            version: 1,
            ..Default::default()
        };
        check(unsafe { sys::ct_flush_dispatch_totals(self.as_ptr(), &mut raw) })?;
        Ok(TreeFlushDispatchStats {
            sampled_at_ns: raw.sampled_at_ns,
            inline_probes: raw.inline_probes,
            inline_ranges: raw.inline_ranges,
            queued_steps: raw.queued_steps,
            prepared_runs: raw.prepared_runs,
            prepared_entries: raw.prepared_entries,
            prepared_bytes: raw.prepared_bytes,
            cow_bytes: raw.cow_bytes,
            active_steps: raw.active_steps,
            peak_steps: raw.peak_steps,
        })
    }

    /// Claims the only window collector; another claim returns `Unavailable`.
    pub fn work_metrics_collector(&self) -> Result<TreeWorkCollector, CtError> {
        let mut handle = std::ptr::null_mut();
        check(unsafe { sys::ct_work_metrics_collector_create(self.as_ptr(), &mut handle) })?;
        Ok(TreeWorkCollector {
            handle: NonNull::new(handle).ok_or(CtError::Internal)?,
        })
    }

    /// Samples cumulative counters without resetting the collector window.
    pub fn work_metrics_totals(&self) -> Result<TreeWorkSnapshot, CtError> {
        let mut raw = raw_snapshot()?;
        let mut ages = raw_ages()?;
        check(unsafe { sys::ct_work_metrics_ages(self.as_ptr(), &mut ages) })?;
        check(unsafe { sys::ct_work_metrics_totals(self.as_ptr(), &mut raw) })?;
        let mut counters = raw_counters()?;
        check(unsafe { sys::ct_work_counters_totals(self.as_ptr(), &mut counters) })?;
        let mut snapshot = TreeWorkSnapshot::from(raw)
            .with_ages(ages)
            .with_counters(counters);
        snapshot.histograms = self.work_histogram_totals()?;
        snapshot.waits = self.wait_metrics_totals()?;
        Ok(snapshot)
    }
}
