// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{TreeWorkCollector, TreeWorkStageAge};
use crate::{error::check, sys, Crowdbtree, CtError};

pub const TREE_WAIT_CLASS_NAMES: [&str; 7] = [
    "handoff",
    "routing",
    "reclaim",
    "admitted_writer",
    "gap_repair",
    "io",
    "shared_cache",
];

/// Cumulative per-Tree request summaries, independent of host reservation totals.
/// Shared-cache samples count joined waiters, excluding first loaders and hits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeWaitStats {
    pub attempts: u64,
    pub inflight: u64,
    pub total_count: u64,
    pub total_sum_ns: u64,
    pub total_max_ns: u64,
    /// Granted/ready, error, cancelled.
    pub outcomes: [u64; 3],
    pub age: TreeWorkStageAge,
}

/// Approximate non-draining cumulative sample. The age sample uses fixed paid
/// storage; saturation reports incomplete coverage without rejecting work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeWaitSnapshot {
    pub sampled_at_ns: u64,
    pub classes: [TreeWaitStats; 7],
}

fn sample(read: impl FnOnce(&mut sys::ct_tree_wait_snapshot_v1) -> i32) -> Result<TreeWaitSnapshot, CtError> {
    let mut raw = sys::ct_tree_wait_snapshot_v1 {
        struct_size: u32::try_from(std::mem::size_of::<sys::ct_tree_wait_snapshot_v1>())
            .map_err(|_| CtError::Internal)?,
        version: 1,
        ..Default::default()
    };
    check(read(&mut raw))?;
    Ok(TreeWaitSnapshot {
        sampled_at_ns: raw.sampled_at_ns,
        classes: raw.classes.map(|value| TreeWaitStats {
            attempts: value.attempts,
            inflight: value.inflight,
            total_count: value.total_count,
            total_sum_ns: value.total_sum_ns,
            total_max_ns: value.total_max_ns,
            outcomes: value.outcomes,
            age: TreeWorkStageAge {
                inflight: value.inflight,
                tracked: value.tracked,
                untracked: value.untracked,
                observed_oldest_age_ns: value.observed_oldest_age_ns,
                complete: value.complete == 1,
            },
        }),
    })
}

impl TreeWorkCollector {
    /// Valid after Tree close and never resets the stage collector window.
    pub fn wait_metrics_totals(&self) -> Result<TreeWaitSnapshot, CtError> {
        sample(|out| unsafe { sys::ct_work_metrics_collector_wait_totals(self.handle.as_ptr(), out) })
    }
}

impl Crowdbtree {
    pub fn wait_metrics_totals(&self) -> Result<TreeWaitSnapshot, CtError> {
        sample(|out| unsafe { sys::ct_tree_wait_totals(self.as_ptr(), out) })
    }
}
