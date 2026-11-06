// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ct_options, ct_range_rebuild_stats, ct_tree};

#[repr(C)]
pub struct ct_runtime {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct ct_resource_limits {
    pub handoff_bytes: usize,
    pub routing_bytes: usize,
    pub reclaim_bytes: usize,
    pub writer_bytes: usize,
    pub gap_repair_bytes: usize,
    pub io_operations: usize,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_runtime_options {
    pub struct_size: usize,
    pub workers: usize,
    pub cpu_queue_limit: usize,
    pub max_waiters_per_class: usize,
    pub lifecycle_bytes: usize,
    pub limits: ct_resource_limits,
    pub backend_workers: usize,
    pub backend_queue_limit: usize,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_runtime_options {
    pub struct_size: usize,
    pub cpu_queue_limit: usize,
    pub lifecycle_bytes: usize,
    pub limits: ct_resource_limits,
    pub backend_queue_limit: usize,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_executor_snapshot_v1 {
    pub queued: u64,
    pub inflight: u64,
    pub submitted: u64,
    pub completed: u64,
    pub rejected: u64,
    pub queue_ns: u64,
    pub execute_ns: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_runtime_snapshot_v1 {
    pub struct_size: usize,
    pub version: u32,
    pub stopped: u32,
    pub reserved: ct_resource_limits,
    pub allocated: ct_resource_limits,
    pub bindings: u64,
    pub cpu: ct_executor_snapshot_v1,
    pub backend: ct_executor_snapshot_v1,
    pub budget_waiters: u64,
    pub budget_grants: u64,
    pub budget_cancelled: u64,
    pub budget_wait_ns: u64,
}

extern "C" {
    pub fn ct_runtime_snapshot(runtime: *const ct_runtime, out: *mut ct_runtime_snapshot_v1) -> i32;
    pub fn ct_runtime_options_default(out: *mut ct_runtime_options);
    pub fn ct_tree_runtime_options_default(out: *mut ct_tree_runtime_options);
    pub fn ct_runtime_create(options: *const ct_runtime_options, out: *mut *mut ct_runtime) -> i32;
    pub fn ct_runtime_stop_admission(runtime: *mut ct_runtime);
    pub fn ct_runtime_drain_cpu(runtime: *mut ct_runtime);
    pub fn ct_runtime_free(runtime: *mut ct_runtime);
    pub fn ct_open_with_runtime(
        options: *const ct_options,
        runtime: *mut ct_runtime,
        binding: *const ct_tree_runtime_options,
        out: *mut *mut ct_tree,
    ) -> i32;
    pub fn ct_rebuild_range_with_runtime(
        source: *mut ct_tree,
        options: *const ct_options,
        runtime: *mut ct_runtime,
        binding: *const ct_tree_runtime_options,
        out: *mut *mut ct_tree,
        stats: *mut ct_range_rebuild_stats,
    ) -> i32;
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct ct_resource_usage_v1 {
    pub limit: u64,
    pub used: u64,
    pub reserved: u64,
    pub allocated: u64,
    pub denied_tree: u64,
    pub denied_host: u64,
    pub waits: u64,
    pub wait_ns: u64,
    pub waiters: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_resource_usage_snapshot_v1 {
    pub struct_size: usize,
    pub version: u32,
    pub resource_mask: u32,
    pub scope_id: u64,
    pub resources: [ct_resource_usage_v1; 20],
}

extern "C" {
    pub fn ct_runtime_resource_usage(
        runtime: *const ct_runtime,
        out: *mut ct_resource_usage_snapshot_v1,
    ) -> i32;
    pub fn ct_tree_resource_usage(tree: *mut ct_tree, out: *mut ct_resource_usage_snapshot_v1) -> i32;
}
