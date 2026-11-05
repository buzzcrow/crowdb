// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace crowdb::tree
{
class TreeWorkMetrics;

// Budget classes retain their ResourceBudget order; cache joins are separate
// from both the first loader's pipeline and ordinary ready-cache hits.
enum class TreeWaitClass : uint8_t { Handoff, Routing, Reclaim, AdmittedWriter, GapRepair, Io, SharedCache, Count };
enum class TreeWaitOutcome : uint8_t { Granted, Error, Cancelled, Count };
inline constexpr size_t kTreeWaitClasses     = static_cast<size_t>(TreeWaitClass::Count);
inline constexpr size_t kTreeWaitOutcomes    = static_cast<size_t>(TreeWaitOutcome::Count);
inline constexpr size_t kTreeWaitAgeCapacity = 256;

struct TreeWaitTotals
{
    uint64_t                                attempts = 0;
    uint64_t                                inflight = 0;
    uint64_t                                count    = 0;
    uint64_t                                sum_ns   = 0;
    uint64_t                                max_ns   = 0;
    std::array<uint64_t, kTreeWaitOutcomes> outcomes{};
    uint64_t                                tracked       = 0;
    uint64_t                                untracked     = 0;
    uint64_t                                oldest_age_ns = 0;
    bool                                    complete      = false;
};

class TreeWaitMetrics
{
  public:
    [[nodiscard]] std::array<TreeWaitTotals, kTreeWaitClasses> totals(uint64_t now_ns) const noexcept;

  private:
    friend class TreeWaitSpan;
    size_t begin(TreeWaitClass kind, uint64_t started) noexcept;
    void   complete(TreeWaitClass kind, TreeWaitOutcome outcome, uint64_t ns, size_t slot) noexcept;

    struct Summary
    {
        std::atomic<uint64_t> attempts{0}, inflight{0}, count{0}, sum_ns{0}, max_ns{0}, untracked{0};
        std::array<std::atomic<uint64_t>, kTreeWaitOutcomes> outcomes{};
    };

    std::array<Summary, kTreeWaitClasses>                   summaries_{};
    std::array<std::atomic<uint64_t>, kTreeWaitAgeCapacity> ages_{};
    std::atomic<size_t>                                     next_age_{0};
};

// One request owner serializes begin/terminal. Repeated terminal calls do
// nothing; an unfinished owner records cancellation before releasing metrics.
class TreeWaitSpan
{
  public:
    TreeWaitSpan() = default;
    ~TreeWaitSpan();
    TreeWaitSpan(const TreeWaitSpan &)            = delete;
    TreeWaitSpan &operator=(const TreeWaitSpan &) = delete;
    void          begin(std::shared_ptr<TreeWorkMetrics> metrics, TreeWaitClass kind, uint64_t started) noexcept;
    void          finish(TreeWaitOutcome outcome, uint64_t ended) noexcept;

  private:
    std::shared_ptr<TreeWorkMetrics> metrics_;
    TreeWaitClass                    kind_    = TreeWaitClass::Handoff;
    uint64_t                         started_ = 0;
    size_t                           slot_    = kTreeWaitAgeCapacity;
};
} // namespace crowdb::tree
