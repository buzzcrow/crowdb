// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::ct_tree;

#[repr(C)]
pub struct ct_work_metrics_collector {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct ct_tree_work_stage_v1 {
    pub attempts: u64,
    pub inflight: u64,
    pub total_count: u64,
    pub total_sum_ns: u64,
    pub total_max_ns: u64,
    pub outcomes: [u64; 5],
    pub window_count: u64,
    pub window_sum_ns: u64,
    pub window_max_ns: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_work_snapshot_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub window_sequence: u64,
    pub sampled_at_ns: u64,
    pub stages: [ct_tree_work_stage_v1; 21],
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct ct_tree_work_stage_age_v1 {
    pub inflight: u64,
    pub tracked: u64,
    pub untracked: u64,
    pub observed_oldest_age_ns: u64,
    pub complete: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_work_ages_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub sampled_at_ns: u64,
    pub stages: [ct_tree_work_stage_age_v1; 21],
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_flush_dispatch_v1 {
    pub struct_size: u32,
    pub version: u32,
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

extern "C" {
    pub fn ct_work_metrics_collector_create(
        tree: *mut ct_tree,
        out: *mut *mut ct_work_metrics_collector,
    ) -> i32;
    pub fn ct_work_metrics_collector_free(collector: *mut ct_work_metrics_collector);
    pub fn ct_work_metrics_totals(tree: *mut ct_tree, out: *mut ct_tree_work_snapshot_v1) -> i32;
    pub fn ct_work_metrics_ages(tree: *mut ct_tree, out: *mut ct_tree_work_ages_v1) -> i32;
    pub fn ct_flush_dispatch_totals(tree: *mut ct_tree, out: *mut ct_tree_flush_dispatch_v1) -> i32;
}

#[repr(C)]
pub struct ct_tree_work_counters_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub sampled_at_ns: u64,
    pub values: [[u64; 55]; 8],
}
impl Default for ct_tree_work_counters_v1 {
    fn default() -> Self {
        Self {
            struct_size: 0,
            version: 0,
            sampled_at_ns: 0,
            values: [[0; 55]; 8],
        }
    }
}
extern "C" {
    pub fn ct_work_counters_totals(tree: *mut ct_tree, out: *mut ct_tree_work_counters_v1) -> i32;
}
