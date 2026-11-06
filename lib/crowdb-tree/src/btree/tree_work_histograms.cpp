// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_work_histograms.h"

#include "crowdb-common/metrics/latency_histogram.h"
#include "crowdb-tree/memory_budget.h"

namespace crowdb::tree
{
struct TreeStageHistogram
{
    explicit TreeStageHistogram(std::shared_ptr<AllocationBudget> budget) : budget(std::move(budget)), histogram("")
    {
    }

    std::shared_ptr<AllocationBudget> budget;
    common::metrics::LatencyHistogram histogram;
};

namespace
{
TreeHistogramSnapshot convert(const common::metrics::LatencyHistogram::Snapshot &value) noexcept
{
    return {.count          = value.count,
            .sum_low        = value.sum_low,
            .sum_high       = value.sum_high,
            .min_ns         = value.min,
            .max_ns         = value.max,
            .p50_ns         = value.p50,
            .p90_ns         = value.p90,
            .p95_ns         = value.p95,
            .p99_ns         = value.p99,
            .p999_ns        = value.p999,
            .zero_count     = value.zero_count,
            .overflow_count = value.overflow_count,
            .total_count    = value.total_count};
}

void dispose(TreeStageHistogram *histogram) noexcept
{
    if (histogram != nullptr) {
        MetadataAllocator<TreeStageHistogram> allocator(histogram->budget);
        std::destroy_at(histogram);
        allocator.deallocate(histogram, 1);
    }
}
} // namespace

TreeWorkMetrics::~TreeWorkMetrics()
{
    for (auto &slot : histograms_) {
        dispose(slot.load(std::memory_order_relaxed));
    }
}

size_t TreeWorkMetrics::histogram_allocation_bytes() noexcept
{
    return sizeof(TreeStageHistogram);
}

bool TreeWorkMetrics::histogram_enabled(TreeStage stage) const noexcept
{
    const auto index = static_cast<size_t>(stage);
    return index < histograms_.size() && histograms_[index].load(std::memory_order_acquire) != nullptr;
}

Status TreeWorkMetrics::enable_histogram(TreeStage stage, const std::shared_ptr<AllocationBudget> &budget)
{
    const auto index = static_cast<size_t>(stage);
    if (index >= histograms_.size() || !budget) {
        return Status::invalid_argument();
    }
    auto &slot = histograms_[index];
    if (slot.load(std::memory_order_acquire) != nullptr) {
        return Status::Ok();
    }
    MetadataAllocator<TreeStageHistogram> allocator(budget);
    TreeStageHistogram                   *candidate = nullptr;
    try {
        candidate = allocator.allocate(1);
        std::construct_at(candidate, budget);
    }
    catch (const std::bad_alloc &) {
        if (candidate != nullptr) {
            allocator.deallocate(candidate, 1);
        }
        return slot.load(std::memory_order_acquire) != nullptr ? Status::Ok() : Status::resource_exhausted();
    }
    TreeStageHistogram *expected = nullptr;
    if (!slot.compare_exchange_strong(expected, candidate, std::memory_order_release, std::memory_order_relaxed)) {
        dispose(candidate);
    }
    return Status::Ok();
}

void TreeWorkMetrics::observe_histogram(TreeStage stage, uint64_t ns) noexcept
{
    if (auto *histogram = histograms_[static_cast<size_t>(stage)].load(std::memory_order_acquire)) {
        histogram->histogram.observe(ns);
    }
}

TreeWorkHistograms TreeWorkMetrics::histogram_totals() const noexcept
{
    TreeWorkHistograms result;
    for (size_t i = 0; i < histograms_.size(); ++i) {
        if (const auto *histogram = histograms_[i].load(std::memory_order_acquire)) {
            result.enabled_stages |= uint64_t{1} << i;
            result.stages[i] = convert(histogram->histogram.snapshot_total());
        }
    }
    return result;
}

TreeWorkHistograms TreeWorkMetrics::drain_histograms() noexcept
{
    TreeWorkHistograms result;
    for (size_t i = 0; i < histograms_.size(); ++i) {
        if (auto *histogram = histograms_[i].load(std::memory_order_acquire)) {
            result.enabled_stages |= uint64_t{1} << i;
            result.stages[i] = convert(histogram->histogram.flush());
        }
    }
    return result;
}

TreeWorkHistograms TreeMetricsCollector::capture_histograms() noexcept
{
    return metrics_->drain_histograms();
}
} // namespace crowdb::tree
