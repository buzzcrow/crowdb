// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-common/log.h"

namespace crowdb::tree
{
int Crowdbtree::height() const
{
    return static_cast<int>(core_->capture()->stats.height);
}

size_t Crowdbtree::leaf_count() const
{
    return core_->capture()->stats.leaf_pages;
}

EngineStats Crowdbtree::stats() const
{
    const auto  state  = core_->state_stats();
    const auto  cached = core_->cache()->snapshot();
    EngineStats result;
    result.snapshot_pages_written = core_->last_snapshot_pages_written();
    result.snapshot_pages_total   = core_->snapshot_pages_total();
    result.last_applied_slot      = last_applied_slot();
    result.contiguous_slot        = contiguous_slot();
    result.gc_watermark           = gc_watermark();
    result.io_failed              = io_failed();
    result.buffer_pool_hits       = cached.hits;
    result.buffer_pool_misses     = cached.misses;
    result.buffer_pool_evictions  = cached.evictions;
    result.buffer_pool_resident   = static_cast<uint32_t>(cached.resident_pages);
    result.buffer_pool_used       = result.buffer_pool_resident;
    result.buffer_pool_dirty      = static_cast<uint32_t>(state.logical.dirty_pages);
    result.leaf_count             = state.logical.leaf_pages;
    result.inner_count            = state.logical.inner_pages;
    return result;
}

TreeSummary Crowdbtree::tree_summary() const
{
    const auto  state = core_->capture();
    TreeSummary result;
    if (state->summary) {
        result              = *state->summary;
        result.root_version = state->commit_seq;
        const auto sources  = state->sources ? state->sources->source_stats() : SourceMemoryStats{};
        result.exact        = sources.known && sources.records == 0 && !state->pending_handoff;
        return result;
    }
    result.root_version                  = state->commit_seq;
    result.covered_slot                  = state->coverage;
    result.live_kv                       = state->stats.kv_known_count;
    result.live_logical_bytes            = state->stats.kv_known_bytes;
    result.reachable_leaf_pages          = state->stats.leaf_pages;
    result.reachable_inner_pages         = state->stats.inner_pages;
    result.reachable_page_capacity_bytes = state->stats.page_size_known_bytes;
    result.exact = state->stats.leaf_pages == 0 && state->stats.inner_pages == 0 && state->sources &&
                   state->sources->source_stats().known && state->sources->source_stats().records == 0 &&
                   !state->pending_handoff;
    return result;
}

ScanProfile Crowdbtree::scan_profile() const
{
    return {};
}

void Crowdbtree::init_metrics(const std::string &prefix, const std::string &backend_label)
{
    (void)backend_label;
    init_core_metrics(prefix);
}

std::string Crowdbtree::flush_metrics_str(double window_secs, const char *timestamp, size_t width, size_t count_w,
                                          size_t tps_w)
{
    if (metrics_registry_ == nullptr) {
        return {};
    }
    char  *buf = nullptr;
    size_t len = 0;
    FILE  *fp  = open_memstream(&buf, &len);
    if (fp == nullptr) {
        return {};
    }
    try {
        metrics_registry_->flush_to(fp, window_secs, timestamp, "cpp-tree", width, count_w, tps_w);
    }
    catch (...) {
        std::fclose(fp);
        std::free(buf);
        throw;
    }
    std::fclose(fp);
    std::unique_ptr<char, decltype(&std::free)> output(buf, &std::free);
    return {buf, len};
}

size_t Crowdbtree::max_name_len() const
{
    if (metrics_registry_ == nullptr) {
        return 0;
    }
    return metrics_registry_->max_name_len();
}

void Crowdbtree::record_compaction_metrics(const MergeGcStats &stats, uint64_t elapsed_ns)
{
    CRB_LOG_INFO("[{}] compact_sparse_blocks: selected={} relocated={} bytes={} deleted={} elapsed_ns={}", name_,
                 stats.blocks_selected, stats.pages_relocated, stats.bytes_relocated, stats.blocks_deleted, elapsed_ns);
}
} // namespace crowdb::tree
