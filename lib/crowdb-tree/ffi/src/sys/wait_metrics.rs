// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_tree, ct_work_metrics_collector};
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ct_tree_wait_summary_v1 {
    pub attempts: u64,
    pub inflight: u64,
    pub total_count: u64,
    pub total_sum_ns: u64,
    pub total_max_ns: u64,
    pub outcomes: [u64; 3],
    pub tracked: u64,
    pub untracked: u64,
    pub observed_oldest_age_ns: u64,
    pub complete: u64,
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ct_tree_wait_snapshot_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub sampled_at_ns: u64,
    pub classes: [ct_tree_wait_summary_v1; 7],
}
extern "C" {
    pub fn ct_tree_wait_totals(tree: *mut ct_tree, out: *mut ct_tree_wait_snapshot_v1) -> i32;
    pub fn ct_work_metrics_collector_wait_totals(
        collector: *mut ct_work_metrics_collector,
        out: *mut ct_tree_wait_snapshot_v1,
    ) -> i32;
}
