// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_wait_metrics.h"

#include "tree_metrics.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <utility>

namespace crowdb::tree
{
namespace
{
constexpr uint64_t kBits = 3;
constexpr uint64_t kMask = (1ULL << kBits) - 1;
static_assert(kTreeWaitClasses <= kMask);
} // namespace

size_t TreeWaitMetrics::begin(TreeWaitClass kind, uint64_t started) noexcept
{
    const auto index   = static_cast<size_t>(kind);
    auto      &summary = summaries_[index];
    summary.attempts.fetch_add(1, std::memory_order_relaxed);
    summary.inflight.fetch_add(1, std::memory_order_relaxed);
    if (started <= (std::numeric_limits<uint64_t>::max() >> kBits)) {
        const auto value = (started << kBits) | (index + 1);
        const auto first = next_age_.fetch_add(1, std::memory_order_relaxed);
        for (size_t i = 0; i < ages_.size(); ++i) {
            const auto slot  = (first + i) % ages_.size();
            uint64_t   empty = 0;
            if (ages_[slot].compare_exchange_strong(empty, value, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                return slot;
            }
        }
    }
    summary.untracked.fetch_add(1, std::memory_order_relaxed);
    return kTreeWaitAgeCapacity;
}

void TreeWaitMetrics::complete(TreeWaitClass kind, TreeWaitOutcome outcome, uint64_t ns, size_t slot) noexcept
{
    auto &summary = summaries_[static_cast<size_t>(kind)];
    if (slot < ages_.size()) {
        ages_[slot].store(0, std::memory_order_release);
    }
    else {
        summary.untracked.fetch_sub(1, std::memory_order_relaxed);
    }
    summary.inflight.fetch_sub(1, std::memory_order_relaxed);
    summary.count.fetch_add(1, std::memory_order_relaxed);
    summary.sum_ns.fetch_add(ns, std::memory_order_relaxed);
    auto old = summary.max_ns.load(std::memory_order_relaxed);
    while (old < ns && !summary.max_ns.compare_exchange_weak(old, ns, std::memory_order_relaxed)) {
    }
    summary.outcomes[static_cast<size_t>(outcome)].fetch_add(1, std::memory_order_relaxed);
}

std::array<TreeWaitTotals, kTreeWaitClasses> TreeWaitMetrics::totals(uint64_t now_ns) const noexcept
{
    std::array<TreeWaitTotals, kTreeWaitClasses> result{};
    std::array<uint64_t, kTreeWaitClasses>       earliest;
    earliest.fill(std::numeric_limits<uint64_t>::max());
    for (size_t i = 0; i < result.size(); ++i) {
        const auto &in  = summaries_[i];
        auto       &out = result[i];
        out.attempts    = in.attempts.load(std::memory_order_relaxed);
        out.inflight    = in.inflight.load(std::memory_order_relaxed);
        out.count       = in.count.load(std::memory_order_relaxed);
        out.sum_ns      = in.sum_ns.load(std::memory_order_relaxed);
        out.max_ns      = in.max_ns.load(std::memory_order_relaxed);
        out.untracked   = in.untracked.load(std::memory_order_relaxed);
        for (size_t o = 0; o < out.outcomes.size(); ++o) {
            out.outcomes[o] = in.outcomes[o].load(std::memory_order_relaxed);
        }
    }
    for (const auto &slot : ages_) {
        const auto value = slot.load(std::memory_order_acquire);
        if (value != 0) {
            const auto index = static_cast<size_t>((value & kMask) - 1);
            ++result[index].tracked;
            earliest[index] = std::min(earliest[index], value >> kBits);
        }
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

TreeWaitSpan::~TreeWaitSpan()
{
    if (metrics_) {
        finish(TreeWaitOutcome::Cancelled, tree_metrics_now_ns());
    }
}

void TreeWaitSpan::begin(std::shared_ptr<TreeWorkMetrics> metrics, TreeWaitClass kind, uint64_t started) noexcept
{
    assert(!metrics_);
    metrics_ = std::move(metrics);
    kind_    = kind;
    started_ = started;
    if (metrics_) {
        slot_ = metrics_->waits.begin(kind_, started_);
    }
}

void TreeWaitSpan::finish(TreeWaitOutcome outcome, uint64_t ended) noexcept
{
    if (auto metrics = std::exchange(metrics_, {})) {
        metrics->waits.complete(kind_, outcome, ended >= started_ ? ended - started_ : 0, slot_);
    }
}
} // namespace crowdb::tree
