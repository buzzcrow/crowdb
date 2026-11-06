// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{
    ct_tree, ct_tree_work_ages_v1, ct_tree_work_counters_v1, ct_tree_work_snapshot_v1,
    ct_work_metrics_collector,
};

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct ct_tree_histogram_v1 {
    pub count: u64,
    pub sum_low: u64,
    pub sum_high: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub zero_count: u64,
    pub overflow_count: u64,
    pub total_count: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_work_histograms_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub window_sequence: u64,
    pub sampled_at_ns: u64,
    pub enabled_stages: u64,
    pub stages: [ct_tree_histogram_v1; 21],
}

extern "C" {
    pub fn ct_work_histogram_enable_v1(tree: *mut ct_tree, stage: u32) -> i32;
    pub fn ct_work_histograms_totals_v1(tree: *mut ct_tree, out: *mut ct_tree_work_histograms_v1) -> i32;
    pub fn ct_work_metrics_capture_v3(
        collector: *mut ct_work_metrics_collector,
        out: *mut ct_tree_work_snapshot_v1,
        ages: *mut ct_tree_work_ages_v1,
        counters: *mut ct_tree_work_counters_v1,
        histograms: *mut ct_tree_work_histograms_v1,
    ) -> i32;
}
