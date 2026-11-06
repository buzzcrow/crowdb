// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ct_tree;

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_logical_stats_v1 {
    pub leaf_pages: u64,
    pub inner_pages: u64,
    pub height: u64,
    pub dirty_pages: u64,
    pub overlay_pages: u64,
    pub overlay_runs: u64,
    pub overlay_entries: u64,
    pub page_size_known_bytes: u64,
    pub page_size_unknown_pages: u64,
    pub page_size_estimate_bytes: u64,
    pub page_size_max_observed_bytes: u64,
    pub kv_known_count: u64,
    pub kv_known_bytes: u64,
    pub kv_unknown_pages: u64,
    pub consolidate_candidate_pages: u64,
    pub split_candidate_pages: u64,
    pub split_ready_pages: u64,
    pub merge_candidate_pages: u64,
    pub merge_candidate_pairs: u64,
    pub merge_ready_pairs: u64,
    pub writeback_candidate_pages: u64,
    pub evict_candidate_pages: u64,
    pub checkpoint_pending_pages: u64,
    pub checkpoint_mapping_pending: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_resource_stats_v1 {
    pub allocated_bytes: u64,
    pub reserved_bytes: u64,
    pub payload_bytes: u64,
    pub frame_bytes: u64,
    pub metadata_bytes: u64,
    pub io_bytes: u64,
    pub scratch_bytes: u64,
    pub retired_bytes: u64,
    pub resident_frames: u64,
    pub cache_bytes: u64,
    pub cache_pinned_bytes: u64,
    pub proxy_pages: u64,
    pub read_inflight_bytes: u64,
    pub write_inflight_bytes: u64,
    pub materialize_inflight_bytes: u64,
    pub allocated_backing_bytes: u64,
    pub maintenance_queued: u64,
    pub maintenance_inflight: u64,
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_stats_snapshot_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub instance_generation: u64,
    pub commit_seq: u64,
    pub sampled_at_ns: u64,
    pub logical: ct_tree_logical_stats_v1,
    pub resources: ct_tree_resource_stats_v1,
}

extern "C" {
    pub fn ct_get_tree_state_stats(tree: *mut ct_tree, out: *mut ct_tree_stats_snapshot_v1) -> i32;
}

#[repr(C)]
#[derive(Default)]
pub struct ct_tree_progress_snapshot_v1 {
    pub struct_size: u32,
    pub version: u32,
    pub instance_generation: u64,
    pub commit_seq: u64,
    pub sampled_at_ns: u64,
    pub candidate_oldest_age_ns: u64,
    pub checkpoint_oldest_pending_age_ns: u64,
    pub maintenance_oldest_age_ns: u64,
}

extern "C" {
    pub fn ct_get_tree_progress_stats(tree: *mut ct_tree, out: *mut ct_tree_progress_snapshot_v1) -> i32;
}
