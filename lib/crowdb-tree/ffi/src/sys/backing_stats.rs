// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_tree, ct_work_metrics_collector};
#[repr(C)]
#[derive(Default)]
pub struct ct_backing_stats_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub scope_id: u64,
    pub allocated_bytes: u64,
}
unsafe extern "C" {
    pub fn ct_work_metrics_collector_backing_stats(
        collector: *mut ct_work_metrics_collector,
        out: *mut ct_backing_stats_v1,
    ) -> i32;
    pub fn ct_get_backing_stats(tree: *mut ct_tree, out: *mut ct_backing_stats_v1) -> i32;
}
