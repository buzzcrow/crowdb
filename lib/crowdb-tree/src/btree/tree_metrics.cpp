// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/tree_metrics.h"

#include "crowdb-tree/memory_budget.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <utility>

namespace crowdb::tree
{
namespace
{
bool checked_delta(uint64_t &value, uint64_t removed, uint64_t added) noexcept
{
    if (value < removed || std::numeric_limits<uint64_t>::max() - (value - removed) < added) {
        return false;
    }
    value = value - removed + added;
    return true;
}

bool valid(const PageMetricsContribution *page) noexcept
{
    return page == nullptr ||
           ((!page->split_ready || page->split_candidate) && (page->overlay_entries == 0 || page->overlay_runs != 0));
}

TreeLogicalStats contribution(const PageMetricsContribution *page) noexcept
{
    if (page == nullptr) {
        return {};
    }
    TreeLogicalStats out;
    out.leaf_pages                  = static_cast<uint64_t>(page->leaf);
    out.inner_pages                 = static_cast<uint64_t>(!page->leaf);
    out.dirty_pages                 = static_cast<uint64_t>(page->dirty);
    out.overlay_pages               = static_cast<uint64_t>(page->overlay_runs != 0);
    out.overlay_runs                = page->overlay_runs;
    out.overlay_entries             = page->overlay_entries;
    out.page_size_known_bytes       = page->page_size_known ? page->page_size_bytes : 0;
    out.page_size_unknown_pages     = static_cast<uint64_t>(!page->page_size_known);
    out.page_size_estimate_bytes    = page->page_size_estimate_bytes;
    out.kv_known_count              = page->leaf && page->kv_known ? page->kv_count : 0;
    out.kv_known_bytes              = page->leaf && page->kv_known ? page->kv_bytes : 0;
    out.kv_unknown_pages            = static_cast<uint64_t>(page->leaf && !page->kv_known);
    out.consolidate_candidate_pages = static_cast<uint64_t>(page->consolidate_candidate);
    out.split_candidate_pages       = static_cast<uint64_t>(page->split_candidate);
    out.split_ready_pages           = static_cast<uint64_t>(page->split_ready);
    out.merge_candidate_pages       = static_cast<uint64_t>(page->merge_candidate);
    out.writeback_candidate_pages   = static_cast<uint64_t>(page->writeback_candidate);
    out.evict_candidate_pages       = static_cast<uint64_t>(page->evict_candidate);
    return out;
}

constexpr std::array kPageFields{&TreeLogicalStats::leaf_pages,
                                 &TreeLogicalStats::inner_pages,
                                 &TreeLogicalStats::dirty_pages,
                                 &TreeLogicalStats::overlay_pages,
                                 &TreeLogicalStats::overlay_runs,
                                 &TreeLogicalStats::overlay_entries,
                                 &TreeLogicalStats::page_size_known_bytes,
                                 &TreeLogicalStats::page_size_unknown_pages,
                                 &TreeLogicalStats::page_size_estimate_bytes,
                                 &TreeLogicalStats::kv_known_count,
                                 &TreeLogicalStats::kv_known_bytes,
                                 &TreeLogicalStats::kv_unknown_pages,
                                 &TreeLogicalStats::consolidate_candidate_pages,
                                 &TreeLogicalStats::split_candidate_pages,
                                 &TreeLogicalStats::split_ready_pages,
                                 &TreeLogicalStats::merge_candidate_pages,
                                 &TreeLogicalStats::writeback_candidate_pages,
                                 &TreeLogicalStats::evict_candidate_pages};

constexpr std::array kResourceFields{&TreeResourceStats::allocated_bytes,
                                     &TreeResourceStats::reserved_bytes,
                                     &TreeResourceStats::payload_bytes,
                                     &TreeResourceStats::frame_bytes,
                                     &TreeResourceStats::metadata_bytes,
                                     &TreeResourceStats::io_bytes,
                                     &TreeResourceStats::scratch_bytes,
                                     &TreeResourceStats::retired_bytes,
                                     &TreeResourceStats::resident_frames,
                                     &TreeResourceStats::cache_bytes,
                                     &TreeResourceStats::cache_pinned_bytes,
                                     &TreeResourceStats::proxy_pages,
                                     &TreeResourceStats::read_inflight_bytes,
                                     &TreeResourceStats::write_inflight_bytes,
                                     &TreeResourceStats::materialize_inflight_bytes,
                                     &TreeResourceStats::allocated_backing_bytes,
                                     &TreeResourceStats::maintenance_queued,
                                     &TreeResourceStats::maintenance_inflight};
static_assert(kResourceFields.size() == static_cast<size_t>(TreeResourceMetric::Count));

void observe_max(std::atomic<uint64_t> &target, uint64_t value) noexcept
{
    auto prior = target.load(std::memory_order_relaxed);
    while (prior < value && !target.compare_exchange_weak(prior, value, std::memory_order_relaxed)) {
    }
}
} // namespace

bool apply_page_metrics(TreeLogicalStats &stats, const PageMetricsContribution *old_page,
                        const PageMetricsContribution *new_page) noexcept
{
    if (!valid(old_page) || !valid(new_page)) {
        return false;
    }
    auto       next    = stats;
    const auto removed = contribution(old_page);
    const auto added   = contribution(new_page);
    for (auto field : kPageFields) {
        if (!checked_delta(next.*field, removed.*field, added.*field)) {
            return false;
        }
    }
    if (std::numeric_limits<uint64_t>::max() - next.leaf_pages < next.inner_pages) {
        return false;
    }
    if (new_page != nullptr) {
        next.page_size_max_observed_bytes =
            std::max({next.page_size_max_observed_bytes, new_page->page_size_estimate_bytes,
                      new_page->page_size_known ? new_page->page_size_bytes : 0});
    }
    stats = next;
    return true;
}

bool equal_page_metrics(const TreeLogicalStats &left, const TreeLogicalStats &right) noexcept
{
    return std::all_of(kPageFields.begin(), kPageFields.end(), [&](auto field) { return left.*field == right.*field; });
}

bool apply_merge_pair_metrics(TreeLogicalStats &stats, uint64_t removed_pairs, uint64_t removed_ready,
                              uint64_t added_pairs, uint64_t added_ready) noexcept
{
    auto next = stats;
    if (removed_ready > removed_pairs || added_ready > added_pairs ||
        !checked_delta(next.merge_candidate_pairs, removed_pairs, added_pairs) ||
        !checked_delta(next.merge_ready_pairs, removed_ready, added_ready) ||
        next.merge_ready_pairs > next.merge_candidate_pairs) {
        return false;
    }
    stats = next;
    return true;
}

bool TreeResourceMetrics::add(TreeResourceMetric metric, uint64_t amount) noexcept
{
    const auto index = static_cast<size_t>(metric);
    if (index >= values_.size()) {
        return false;
    }
    auto &value = values_[index];
    auto  prior = value.load(std::memory_order_relaxed);
    do {
        if (std::numeric_limits<uint64_t>::max() - prior < amount) {
            return false;
        }
    } while (!value.compare_exchange_weak(prior, prior + amount, std::memory_order_relaxed));
    return true;
}

bool TreeResourceMetrics::remove(TreeResourceMetric metric, uint64_t amount) noexcept
{
    const auto index = static_cast<size_t>(metric);
    if (index >= values_.size()) {
        return false;
    }
    auto &value = values_[index];
    auto  prior = value.load(std::memory_order_relaxed);
    do {
        if (prior < amount) {
            return false;
        }
    } while (!value.compare_exchange_weak(prior, prior - amount, std::memory_order_relaxed));
    return true;
}

TreeResourceStats TreeResourceMetrics::snapshot() const noexcept
{
    TreeResourceStats result;
    for (size_t i = 0; i < values_.size(); ++i) {
        result.*kResourceFields[i] = values_[i].load(std::memory_order_relaxed);
    }
    return result;
}

size_t TreeWorkMetrics::begin(TreeStage stage, uint64_t started_at_ns) noexcept
{
    auto &entry = stages_[static_cast<size_t>(stage)];
    entry.attempts.fetch_add(1, std::memory_order_relaxed);
    entry.inflight.fetch_add(1, std::memory_order_relaxed);
    return track_age(stage, started_at_ns);
}

void TreeWorkMetrics::complete(TreeStage stage, TreeOutcome outcome, uint64_t ns, size_t age_slot) noexcept
{
    observe_histogram(stage, ns);
    auto &entry = stages_[static_cast<size_t>(stage)];
    entry.count.fetch_add(1, std::memory_order_relaxed);
    entry.sum_ns.fetch_add(ns, std::memory_order_relaxed);
    entry.outcomes[static_cast<size_t>(outcome)].fetch_add(1, std::memory_order_relaxed);
    observe_max(entry.max_ns, ns);
    entry.window_count.fetch_add(1, std::memory_order_relaxed);
    entry.window_sum_ns.fetch_add(ns, std::memory_order_relaxed);
    observe_max(entry.window_max_ns, ns);
    finish_age(stage, age_slot);
    entry.inflight.fetch_sub(1, std::memory_order_relaxed);
}

TreeWorkTotals TreeWorkMetrics::totals() const noexcept
{
    TreeWorkTotals result{};
    for (size_t i = 0; i < stages_.size(); ++i) {
        const auto &entry = stages_[i];
        auto       &out   = result[i];
        out.attempts      = entry.attempts.load(std::memory_order_relaxed);
        out.inflight      = entry.inflight.load(std::memory_order_relaxed);
        out.count         = entry.count.load(std::memory_order_relaxed);
        out.sum_ns        = entry.sum_ns.load(std::memory_order_relaxed);
        out.max_ns        = entry.max_ns.load(std::memory_order_relaxed);
        for (size_t j = 0; j < out.outcomes.size(); ++j) {
            out.outcomes[j] = entry.outcomes[j].load(std::memory_order_relaxed);
        }
    }
    return result;
}

TreeWorkWindow TreeWorkMetrics::drain_window() noexcept
{
    TreeWorkWindow result{};
    for (size_t i = 0; i < stages_.size(); ++i) {
        auto &entry = stages_[i];
        result[i]   = {.count       = entry.window_count.exchange(0, std::memory_order_relaxed),
                       .sum_ns      = entry.window_sum_ns.exchange(0, std::memory_order_relaxed),
                       .max_ns      = entry.window_max_ns.exchange(0, std::memory_order_relaxed),
                       .total_count = entry.count.load(std::memory_order_relaxed)};
    }
    return result;
}

std::shared_ptr<TreeMetricsCollector> TreeWorkMetrics::claim_collector(const std::shared_ptr<AllocationBudget> &budget)
{
    auto owner    = shared_from_this();
    bool expected = false;
    if (!collector_claimed_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
        return nullptr;
    }
    MetadataAllocator<TreeMetricsCollector> allocator(budget);
    TreeMetricsCollector                   *value = nullptr;
    try {
        value = allocator.allocate(1);
        ::new (static_cast<void *>(value)) TreeMetricsCollector(std::move(owner));
    }
    catch (...) {
        if (value != nullptr) {
            allocator.deallocate(value, 1);
        }
        collector_claimed_.store(false, std::memory_order_relaxed);
        throw;
    }
    // shared_ptr construction calls this deleter itself if its control-block
    // allocation fails. Do not reset the claim twice across that boundary.
    return {value,
            [allocator](TreeMetricsCollector *entry) mutable {
                std::destroy_at(entry);
                allocator.deallocate(entry, 1);
            },
            allocator};
}

TreeMetricsOperation::TreeMetricsOperation(std::shared_ptr<TreeWorkMetrics> metrics, TreeStage stage,
                                           uint64_t started_at_ns) noexcept
    : metrics_(std::move(metrics)),
      stage_(stage),
      started_at_ns_(started_at_ns)
{
    assert(static_cast<size_t>(stage) < kTreeStageCount);
    if (metrics_) {
        age_slot_ = metrics_->begin(stage_, started_at_ns_);
    }
}

TreeMetricsOperation::~TreeMetricsOperation()
{
    if (!finished_.load(std::memory_order_relaxed)) {
        (void)finish(TreeOutcome::Cancelled, tree_metrics_now_ns());
    }
}

bool TreeMetricsOperation::finish(TreeOutcome outcome, uint64_t ended_at_ns) noexcept
{
    bool expected = false;
    if (static_cast<size_t>(outcome) >= kTreeOutcomeCount ||
        !finished_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
        return false;
    }
    if (metrics_) {
        metrics_->complete(stage_, outcome, ended_at_ns >= started_at_ns_ ? ended_at_ns - started_at_ns_ : 0,
                           age_slot_);
    }
    return true;
}

TreeMetricsCollector::TreeMetricsCollector(std::shared_ptr<TreeWorkMetrics> metrics) : metrics_(std::move(metrics))
{
}

TreeMetricsCollector::~TreeMetricsCollector()
{
    metrics_->collector_claimed_.store(false, std::memory_order_relaxed);
}

TreeWorkWindow TreeMetricsCollector::capture_window() noexcept
{
    return metrics_->drain_window();
}

uint64_t tree_metrics_now_ns() noexcept
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

} // namespace crowdb::tree
