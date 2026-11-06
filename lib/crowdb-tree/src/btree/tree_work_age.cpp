// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_metrics.h"

#include <algorithm>
#include <cassert>
#include <limits>

namespace crowdb::tree
{
namespace
{
constexpr uint64_t kStageBits = 5;
constexpr uint64_t kStageMask = (1ULL << kStageBits) - 1;
static_assert(kTreeStageCount <= kStageMask);
} // namespace

size_t TreeWorkMetrics::track_age(TreeStage stage, uint64_t started_at_ns) noexcept
{
    const auto index = static_cast<size_t>(stage);
    if (started_at_ns <= (std::numeric_limits<uint64_t>::max() >> kStageBits)) {
        const auto value = (started_at_ns << kStageBits) | (index + 1);
        const auto first = next_age_slot_.fetch_add(1, std::memory_order_relaxed);
        for (size_t i = 0; i < active_ages_.size(); ++i) {
            const auto slot  = (first + i) % active_ages_.size();
            uint64_t   empty = 0;
            if (active_ages_[slot].compare_exchange_strong(empty, value, std::memory_order_release,
                                                           std::memory_order_relaxed)) {
                return slot;
            }
        }
    }
    stages_[index].untracked_ages.fetch_add(1, std::memory_order_relaxed);
    return kTreeWorkAgeCapacity;
}

void TreeWorkMetrics::finish_age(TreeStage stage, size_t slot) noexcept
{
    if (slot < active_ages_.size()) {
        active_ages_[slot].store(0, std::memory_order_release);
    }
    else {
        const auto before = stages_[static_cast<size_t>(stage)].untracked_ages.fetch_sub(1, std::memory_order_relaxed);
        assert(before != 0);
        (void)before;
    }
}

TreeWorkAges TreeWorkMetrics::ages(uint64_t now_ns) const noexcept
{
    TreeWorkAges                          result{};
    std::array<uint64_t, kTreeStageCount> earliest;
    earliest.fill(std::numeric_limits<uint64_t>::max());
    for (size_t i = 0; i < stages_.size(); ++i) {
        result[i].inflight  = stages_[i].inflight.load(std::memory_order_relaxed);
        result[i].untracked = stages_[i].untracked_ages.load(std::memory_order_relaxed);
    }
    for (const auto &slot : active_ages_) {
        const auto value = slot.load(std::memory_order_acquire);
        if (value == 0) {
            continue;
        }
        const auto index = static_cast<size_t>((value & kStageMask) - 1);
        ++result[index].tracked;
        earliest[index] = std::min(earliest[index], value >> kStageBits);
    }
    for (size_t i = 0; i < result.size(); ++i) {
        auto &out    = result[i];
        out.complete = out.untracked == 0 && out.tracked == out.inflight;
        if (out.tracked != 0 && now_ns >= earliest[i]) {
            out.oldest_age_ns = now_ns - earliest[i];
        }
    }
    return result;
}
} // namespace crowdb::tree
