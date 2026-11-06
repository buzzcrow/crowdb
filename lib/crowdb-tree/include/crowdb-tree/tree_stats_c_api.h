// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <cstddef>
#include <cstdint>

extern "C" {
struct ct_tree;

// Every logical field belongs to the same immutable publication. Unknown
// page summaries are explicit; estimates must not replace exact subtotals.
struct ct_tree_logical_stats_v1
{
    uint64_t leaf_pages;
    uint64_t inner_pages;
    uint64_t height;
    uint64_t dirty_pages;
    uint64_t overlay_pages;
    uint64_t overlay_runs;
    uint64_t overlay_entries;
    uint64_t page_size_known_bytes;
    uint64_t page_size_unknown_pages;
    uint64_t page_size_estimate_bytes;
    uint64_t page_size_max_observed_bytes;
    uint64_t kv_known_count;
    uint64_t kv_known_bytes;
    uint64_t kv_unknown_pages;
    uint64_t consolidate_candidate_pages;
    uint64_t split_candidate_pages;
    uint64_t split_ready_pages;
    uint64_t merge_candidate_pages;
    uint64_t merge_candidate_pairs;
    uint64_t merge_ready_pairs;
    uint64_t writeback_candidate_pages;
    uint64_t evict_candidate_pages;
    uint64_t checkpoint_pending_pages;
    uint64_t checkpoint_mapping_pending;
};

// Independent approximate samples. Referenced/pinned subsets overlap.
struct ct_tree_resource_stats_v1
{
    uint64_t allocated_bytes;
    uint64_t reserved_bytes;
    uint64_t payload_bytes;
    uint64_t frame_bytes;
    uint64_t metadata_bytes;
    uint64_t io_bytes;
    uint64_t scratch_bytes;
    uint64_t retired_bytes;
    uint64_t resident_frames;
    uint64_t cache_bytes;
    uint64_t cache_pinned_bytes;
    uint64_t proxy_pages;
    uint64_t read_inflight_bytes;
    uint64_t write_inflight_bytes;
    uint64_t materialize_inflight_bytes;
    uint64_t allocated_backing_bytes;
    uint64_t maintenance_queued;
    uint64_t maintenance_inflight;
};

struct ct_tree_stats_snapshot_v1
{
    uint32_t                  struct_size;
    uint32_t                  version;
    uint64_t                  instance_generation;
    uint64_t                  commit_seq;
    uint64_t                  sampled_at_ns;
    ct_tree_logical_stats_v1  logical;
    ct_tree_resource_stats_v1 resources;
};

// Set struct_size=sizeof(*out), version=1. Unsupported versions/sizes leave
// output unchanged. Legacy Trees return Unavailable; no mapping scan occurs.
int32_t ct_get_tree_state_stats(ct_tree *tree, ct_tree_stats_snapshot_v1 *out);

struct ct_tree_progress_snapshot_v1
{
    uint32_t struct_size;
    uint32_t version;
    uint64_t instance_generation;
    uint64_t commit_seq;
    uint64_t sampled_at_ns;
    uint64_t candidate_oldest_age_ns;
    uint64_t checkpoint_oldest_pending_age_ns;
    uint64_t maintenance_oldest_age_ns;
};

// Independent versioned extension; the existing state snapshot ABI is stable.
int32_t ct_get_tree_progress_stats(ct_tree *tree, ct_tree_progress_snapshot_v1 *out);
}
