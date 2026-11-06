// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "apply_progress.h"
#include "source_write_budget.h"

namespace crowdb::tree
{
struct SourceCatalog;

// Paid immutable-plan ownership. Destruction reopens admission only if the
// captured table has not permanently closed. No executor is retained.
class SourcePause : public std::enable_shared_from_this<SourcePause>
{
  public:
    SourcePause(std::shared_ptr<MemTable> table, std::shared_ptr<SourceWriteBudget> budget,
                std::shared_ptr<ApplyProgress> progress)
        : table_(std::move(table)),
          budget_(std::move(budget)),
          progress_(std::move(progress))
    {
    }

    ~SourcePause();
    static Status create(const SourceCatalog &catalog, const std::shared_ptr<detail::AllocationReservation> &metadata,
                         std::shared_ptr<ApplyProgress> progress, std::shared_ptr<SourcePause> *out);

    [[nodiscard]] bool ready() const noexcept
    {
        return ready_.load(std::memory_order_acquire);
    }

    void wake_on_release() noexcept
    {
        notify_ = true;
    }

    [[nodiscard]] bool matches(const SourceCatalog &catalog) const noexcept;

    [[nodiscard]] WriteFootprint bound() const noexcept
    {
        return budget_->paused_bound();
    }

  private:
    static void                        complete(void *context) noexcept;
    std::shared_ptr<MemTable>          table_;
    std::shared_ptr<SourceWriteBudget> budget_;
    std::shared_ptr<ApplyProgress>     progress_;
    std::shared_ptr<SourcePause>       self_;
    std::atomic<bool>                  ready_{false};
    bool                               owns_   = false;
    bool                               notify_ = false;
};
} // namespace crowdb::tree
