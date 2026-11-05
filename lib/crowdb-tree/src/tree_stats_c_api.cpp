// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/tree_stats_c_api.h"

#include "c_api_internal.h"
#include "crowdb-tree/btree/tree.h"

using namespace crowdb::tree;

int32_t ct_get_tree_state_stats(ct_tree *tree, ct_tree_stats_snapshot_v1 *out)
try {
    if (tree == nullptr || out == nullptr || out->struct_size != sizeof(*out) || out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    const auto snapshot = c_api_tree(tree)->tree_state_stats();
    if (snapshot.instance_generation == 0) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    ct_tree_stats_snapshot_v1 result{};
    result.struct_size                          = sizeof(result);
    result.version                              = 1;
    result.instance_generation                  = snapshot.instance_generation;
    result.commit_seq                           = snapshot.commit_seq;
    result.sampled_at_ns                        = snapshot.sampled_at_ns;
    result.logical.leaf_pages                   = snapshot.logical.leaf_pages;
    result.logical.inner_pages                  = snapshot.logical.inner_pages;
    result.logical.height                       = snapshot.logical.height;
    result.logical.dirty_pages                  = snapshot.logical.dirty_pages;
    result.logical.overlay_pages                = snapshot.logical.overlay_pages;
    result.logical.overlay_runs                 = snapshot.logical.overlay_runs;
    result.logical.overlay_entries              = snapshot.logical.overlay_entries;
    result.logical.page_size_known_bytes        = snapshot.logical.page_size_known_bytes;
    result.logical.page_size_unknown_pages      = snapshot.logical.page_size_unknown_pages;
    result.logical.page_size_estimate_bytes     = snapshot.logical.page_size_estimate_bytes;
    result.logical.page_size_max_observed_bytes = snapshot.logical.page_size_max_observed_bytes;
    result.logical.kv_known_count               = snapshot.logical.kv_known_count;
    result.logical.kv_known_bytes               = snapshot.logical.kv_known_bytes;
    result.logical.kv_unknown_pages             = snapshot.logical.kv_unknown_pages;
    result.logical.consolidate_candidate_pages  = snapshot.logical.consolidate_candidate_pages;
    result.logical.split_candidate_pages        = snapshot.logical.split_candidate_pages;
    result.logical.split_ready_pages            = snapshot.logical.split_ready_pages;
    result.logical.merge_candidate_pages        = snapshot.logical.merge_candidate_pages;
    result.logical.merge_candidate_pairs        = snapshot.logical.merge_candidate_pairs;
    result.logical.merge_ready_pairs            = snapshot.logical.merge_ready_pairs;
    result.logical.writeback_candidate_pages    = snapshot.logical.writeback_candidate_pages;
    result.logical.evict_candidate_pages        = snapshot.logical.evict_candidate_pages;
    result.logical.checkpoint_pending_pages     = snapshot.logical.checkpoint_pending_pages;
    result.logical.checkpoint_mapping_pending   = snapshot.logical.checkpoint_mapping_pending ? 1 : 0;
    result.resources.allocated_bytes            = snapshot.resources.allocated_bytes;
    result.resources.reserved_bytes             = snapshot.resources.reserved_bytes;
    result.resources.payload_bytes              = snapshot.resources.payload_bytes;
    result.resources.frame_bytes                = snapshot.resources.frame_bytes;
    result.resources.metadata_bytes             = snapshot.resources.metadata_bytes;
    result.resources.io_bytes                   = snapshot.resources.io_bytes;
    result.resources.scratch_bytes              = snapshot.resources.scratch_bytes;
    result.resources.retired_bytes              = snapshot.resources.retired_bytes;
    result.resources.resident_frames            = snapshot.resources.resident_frames;
    result.resources.cache_bytes                = snapshot.resources.cache_bytes;
    result.resources.cache_pinned_bytes         = snapshot.resources.cache_pinned_bytes;
    result.resources.proxy_pages                = snapshot.resources.proxy_pages;
    result.resources.read_inflight_bytes        = snapshot.resources.read_inflight_bytes;
    result.resources.write_inflight_bytes       = snapshot.resources.write_inflight_bytes;
    result.resources.materialize_inflight_bytes = snapshot.resources.materialize_inflight_bytes;
    result.resources.allocated_backing_bytes    = snapshot.resources.allocated_backing_bytes;
    result.resources.maintenance_queued         = snapshot.resources.maintenance_queued;
    result.resources.maintenance_inflight       = snapshot.resources.maintenance_inflight;
    *out                                        = result;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}
