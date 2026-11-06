// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>

namespace crowdb::tree
{
// Published with the mapping that produced these counts. Unknown summaries
// contribute to their unknown-page count, never to the exact subtotal.
struct TreeLogicalStats
{
    uint64_t leaf_pages                   = 0;
    uint64_t inner_pages                  = 0;
    uint64_t height                       = 0;
    uint64_t dirty_pages                  = 0;
    uint64_t overlay_pages                = 0;
    uint64_t overlay_runs                 = 0;
    uint64_t overlay_entries              = 0;
    uint64_t page_size_known_bytes        = 0;
    uint64_t page_size_unknown_pages      = 0;
    uint64_t page_size_estimate_bytes     = 0;
    uint64_t page_size_max_observed_bytes = 0;
    uint64_t kv_known_count               = 0;
    uint64_t kv_known_bytes               = 0;
    uint64_t kv_unknown_pages             = 0;
    uint64_t consolidate_candidate_pages  = 0;
    uint64_t split_candidate_pages        = 0;
    uint64_t split_ready_pages            = 0;
    uint64_t merge_candidate_pages        = 0;
    uint64_t merge_candidate_pairs        = 0;
    uint64_t merge_ready_pairs            = 0;
    uint64_t writeback_candidate_pages    = 0;
    uint64_t evict_candidate_pages        = 0;
    uint64_t checkpoint_pending_pages     = 0;
    bool     checkpoint_mapping_pending   = false;

    [[nodiscard]] uint64_t page_count() const
    {
        return leaf_pages + inner_pages;
    }
};

// Allocation and reference gauges are sampled independently with relaxed
// atomics. Referenced/pinned subsets overlap; only allocated + reserved is
// meaningful as physical occupancy, and admission uses its own exact ledger.
struct TreeResourceStats
{
    uint64_t allocated_bytes            = 0;
    uint64_t reserved_bytes             = 0;
    uint64_t payload_bytes              = 0;
    uint64_t frame_bytes                = 0;
    uint64_t metadata_bytes             = 0;
    uint64_t io_bytes                   = 0;
    uint64_t scratch_bytes              = 0;
    uint64_t retired_bytes              = 0;
    uint64_t resident_frames            = 0;
    uint64_t cache_bytes                = 0;
    uint64_t cache_pinned_bytes         = 0;
    uint64_t proxy_pages                = 0;
    uint64_t read_inflight_bytes        = 0;
    uint64_t write_inflight_bytes       = 0;
    uint64_t materialize_inflight_bytes = 0;
    // Backing gauges may project a shared backend ledger. Use backing_stats()
    // scope_id to deduplicate those views; RAM and admission remain per Tree.
    uint64_t allocated_backing_bytes = 0;
    uint64_t maintenance_queued      = 0;
    uint64_t maintenance_inflight    = 0;
};

// Backend-ledger projection. Equal nonzero scope IDs name the same physical
// accounting domain; observers must deduplicate scopes rather than sum views.
struct BackingStatsSnapshot
{
    uint64_t scope_id        = 0;
    uint64_t allocated_bytes = 0;
};

struct TreeProgressStats
{
    uint64_t maintenance_oldest_age_ns        = 0;
    uint64_t candidate_oldest_age_ns          = 0;
    uint64_t checkpoint_oldest_pending_age_ns = 0;
};

struct TreeStatsSnapshot
{
    uint64_t          instance_generation = 0;
    uint64_t          commit_seq          = 0;
    uint64_t          sampled_at_ns       = 0;
    TreeLogicalStats  logical;
    TreeResourceStats resources;
    TreeProgressStats progress;
};

struct TreeSummary
{
    uint64_t root_version                  = 0;
    uint64_t covered_slot                  = 0;
    uint64_t live_kv                       = 0;
    uint64_t live_key_bytes                = 0;
    uint64_t live_value_bytes              = 0;
    uint64_t reachable_leaf_pages          = 0;
    uint64_t reachable_inner_pages         = 0;
    uint64_t reachable_overflow_pages      = 0;
    uint64_t reachable_page_capacity_bytes = 0;
    uint64_t live_logical_bytes            = 0;
    bool     exact                         = true;
};

// Result of a cadence-driven compact_sparse_blocks() pass (R129). Snapshot
// folding drops eligible tombstones during every snapshot; this struct
// reports the block-level relocation and deletion outcome of one compaction
// pass.
struct MergeGcStats
{
    uint64_t blocks_selected = 0; // sparse source blocks chosen for relocation
    uint64_t pages_relocated = 0; // resident + unloaded pages moved
    uint64_t bytes_relocated = 0; // bytes written for relocated extents
    uint64_t blocks_deleted  = 0; // source blocks unlinked after commit
};

// Compatibility diagnostics. Runtime cache values are sampled from maintained
// counters; no page, keyspace or frame walk occurs. Deprecated fields below
// return zero. TreeStatsSnapshot and the work collector expose current schemas.
struct EngineStats
{
    uint64_t last_applied_slot         = 0;     // in-memory L1 coverage (see last_applied_slot())
    uint64_t contiguous_slot           = 0;     // gap-free-applied watermark (see contiguous_slot())
    uint64_t gc_watermark              = 0;     // min(snapshot_slot, safe_slot) (see gc_watermark())
    bool     io_failed                 = false; // latched media fault (see io_failed())
    uint64_t snapshot_pages_written    = 0;     // frames physically written by the last successful durable checkpoint
    uint64_t snapshot_pages_total      = 0;     // cumulative pages written across all snapshots
    uint64_t snapshot_segments_written = 0;     // deprecated dirty-segment counter; always zero
    // Runtime decoded-page cache hits/misses/evictions and resident pages.
    // Dirty counts use the published logical pages; used aliases resident.
    // writebacks and num_frames describe the removed arena and return zero.
    uint64_t buffer_pool_hits       = 0;
    uint64_t buffer_pool_misses     = 0;
    uint64_t buffer_pool_evictions  = 0;
    uint64_t buffer_pool_writebacks = 0;
    uint32_t buffer_pool_resident   = 0;
    uint32_t buffer_pool_dirty      = 0;
    uint32_t buffer_pool_used       = 0;
    uint32_t buffer_pool_num_frames = 0;
    // Deprecated legacy MemTable/flush/lookup events; these fields return zero.
    // The work collector reports the new fixed quantity/cause schema, whose
    // units deliberately do not alias the historical counters below.
    uint64_t mt_upsert_total        = 0; // apply() writes into L0
    uint64_t mt_overwrite_total     = 0;
    uint64_t mt_history_keep_total  = 0;
    uint64_t mt_history_merge_total = 0;
    uint64_t mt_version_cas_retries = 0;
    uint64_t mt_resident_bytes      = 0;
    uint64_t mt_get_total           = 0; // get() lookups in L0
    uint64_t mt_get_hit_total       = 0; // L0 lookups that found a cell
    uint64_t flush_drain_total      = 0; // drain_all_frozen_locked calls
    uint64_t flush_entries_total    = 0; // entries drained from L0 to L1
    uint64_t snapshot_total         = 0; // snapshot() calls (durable checkpoints)
    uint64_t l1_get_total           = 0; // get() lookups that descended to L1
    uint64_t l1_get_hit_total       = 0; // L1 lookups that found a cell
    uint64_t map_lookup_total       = 0; // mapping table lookups
    uint64_t demand_load_total      = 0; // demand-load page faults
    uint64_t leaf_count             = 0; // live leaf pages (O(1) atomic)
    uint64_t inner_count            = 0; // live inner pages (O(1) atomic)
};

// Deprecated empty compatibility profile. Runtime stage measurements are
// available through the explicit work metrics collector.
struct ScanProfile
{
    uint64_t count   = 0; // scans in the window
    uint64_t entries = 0; // total entries returned

    struct Step
    {
        uint64_t sum_ns = 0;
        uint64_t max_ns = 0;
        uint64_t avg_ns = 0; // sum_ns / count (filled by scan_profile)
    };

    Step l0;
    Step l1;
    Step merge;
    Step total;
};

} // namespace crowdb::tree
