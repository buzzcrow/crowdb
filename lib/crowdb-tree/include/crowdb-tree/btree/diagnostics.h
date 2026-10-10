// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>

namespace crowdb::tree
{
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

// Point-in-time diagnostics snapshot: batches every
// cheap (O(1)) internal counter worth exposing to an operator into one
// struct, so a caller/FFI/console poll costs one call instead of many
// small ones. Deliberately excludes anything that requires walking the
// tree (height()/leaf_count()) or the full keyspace -- every field
// here is already an atomic counter or BufferPool::stats(), also O(1).
struct EngineStats
{
    uint64_t last_applied_slot         = 0;     // in-memory L1 coverage (see last_applied_slot())
    uint64_t contiguous_slot           = 0;     // gap-free-applied watermark (see contiguous_slot())
    uint64_t gc_watermark              = 0;     // min(snapshot_slot, safe_slot) (see gc_watermark())
    bool     io_failed                 = false; // latched media fault (see io_failed())
    uint64_t snapshot_pages_written    = 0;     // last snapshot()'s dirty base pages written
    uint64_t snapshot_pages_total      = 0;     // cumulative pages written across all snapshots
    uint64_t snapshot_segments_written = 0;     // last snapshot()'s dirty mapping segments written
    // BufferPool::Stats as of this call -- see buffer_pool.h.
    uint64_t buffer_pool_hits       = 0;
    uint64_t buffer_pool_misses     = 0;
    uint64_t buffer_pool_evictions  = 0;
    uint64_t buffer_pool_writebacks = 0;
    uint32_t buffer_pool_resident   = 0;
    uint32_t buffer_pool_dirty      = 0;
    uint32_t buffer_pool_used       = 0;
    uint32_t buffer_pool_num_frames = 0;
    // MemTable (L0) / flush / L1 cumulative counters (monotonic since open).
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

// Per-step scan profile: each step's aggregate over the window since the last
// scan_profile() call (the underlying LatencySummary handles are flushed, so
// this is a destructive read -- the window resets on each call). `count` is the
// number of scans in the window; `entries` is the total entries returned. Each
// step's `sum_ns` / `max_ns` cover only that step; `avg_ns` is sum_ns / count.
// Steps: l1 (B-tree descent + per-leaf resolve), merge (L0+L1 advance +
// winner + decode), total (whole scan).
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
