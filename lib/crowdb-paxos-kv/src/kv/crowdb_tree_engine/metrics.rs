// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fmt::Write;
use std::sync::Arc;

use crowdb_tree_ffi::{
    CtError, TreeWorkSnapshot, TREE_WORK_CAUSE_NAMES, TREE_WORK_COUNTER_NAMES, TREE_WORK_STAGE_NAMES,
};

use super::CrowdbTreeEngine;

impl CrowdbTreeEngine {
    /// Called once by the host collector per tick. Consumers read the shared
    /// immutable result through `work_metrics_window` without draining again.
    ///
    /// # Errors
    /// Returns `Unavailable` if another capture is already in progress.
    pub fn capture_work_metrics(&self) -> Result<Option<Arc<TreeWorkSnapshot>>, CtError> {
        let Some(collector) = &self.work_collector else {
            return Ok(None);
        };
        let snapshot = Arc::new(collector.capture()?);
        self.work_window.rcu(|current| {
            if matches!(current.as_ref(), Some(old) if old.window_sequence >= snapshot.window_sequence) {
                current.clone()
            } else {
                Some(Arc::clone(&snapshot))
            }
        });
        Ok(Some(snapshot))
    }

    /// Returns the latest collector window; sampling this never resets counts.
    #[must_use]
    pub fn work_metrics_window(&self) -> Option<Arc<TreeWorkSnapshot>> {
        self.work_window.load_full()
    }

    /// Formats the same immutable window used by diagnostics and other readers.
    #[must_use]
    pub fn format_work_metrics_window(&self, prefix: &str, timestamp: &str) -> String {
        let Some(window) = self.work_metrics_window() else {
            return String::new();
        };
        let mut output = String::new();
        for (name, stage) in TREE_WORK_STAGE_NAMES.iter().zip(window.stages) {
            if stage.attempts == 0 {
                continue;
            }
            let _ = writeln!(
                output,
                "{timestamp} {prefix}.tree.work.{name} window={} count={} total={} inflight={} sum_ns={} max_ns={} success={} error={} cancelled={} retry={} stale={} age_sampled_at_ns={} age_inflight={} age_tracked={} age_untracked={} observed_oldest_age_ns={} age_complete={}",
                window.window_sequence,
                stage.window_count,
                stage.total_count,
                stage.inflight,
                stage.window_sum_ns,
                stage.window_max_ns,
                stage.outcomes[0],
                stage.outcomes[1],
                stage.outcomes[2],
                stage.outcomes[3],
                stage.outcomes[4],
                window.age_sampled_at_ns,
                stage.age.inflight,
                stage.age.tracked,
                stage.age.untracked,
                stage.age.observed_oldest_age_ns,
                stage.age.complete
            );
        }
        for (index, name) in TREE_WORK_STAGE_NAMES.iter().enumerate() {
            if window.histograms.enabled_stages & (1 << index) == 0 {
                continue;
            }
            let histogram = window.histograms.stages[index];
            let _ = writeln!(
                output,
                "{timestamp} {prefix}.tree.histogram.{name} window={} count={} total={} sum_ns={} min_ns={} max_ns={} p50_ns={} p90_ns={} p95_ns={} p99_ns={} p999_ns={} zero={} overflow={}",
                window.histograms.window_sequence,
                histogram.count,
                histogram.total_count,
                histogram.sum_ns,
                histogram.min_ns,
                histogram.max_ns,
                histogram.p50_ns,
                histogram.p90_ns,
                histogram.p95_ns,
                histogram.p99_ns,
                histogram.p999_ns,
                histogram.zero_count,
                histogram.overflow_count
            );
        }
        for (cause, values) in TREE_WORK_CAUSE_NAMES.iter().zip(window.counters.values) {
            for (counter, value) in TREE_WORK_COUNTER_NAMES.iter().zip(values) {
                if value != 0 {
                    let _ = writeln!(
                        output,
                        "{timestamp} {prefix}.tree.work.{cause}.{counter} total={value} window={} sampled_at_ns={}",
                        window.window_sequence, window.counters.sampled_at_ns
                    );
                }
            }
        }
        for (name, wait) in crowdb_tree_ffi::TREE_WAIT_CLASS_NAMES
            .iter()
            .zip(window.waits.classes)
        {
            if wait.attempts == 0 {
                continue;
            }
            let _ = writeln!(output,
                "{timestamp} {prefix}.tree.wait.{name} count={} sum_ns={} max_ns={} inflight={} granted={} error={} cancelled={} age_ns={} age_complete={} sampled_at_ns={}",
                wait.total_count, wait.total_sum_ns, wait.total_max_ns, wait.inflight,
                wait.outcomes[0], wait.outcomes[1], wait.outcomes[2],
                wait.age.observed_oldest_age_ns, wait.age.complete, window.waits.sampled_at_ns);
        }
        output
    }
}
