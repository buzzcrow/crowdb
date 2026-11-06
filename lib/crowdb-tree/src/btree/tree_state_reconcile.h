// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "published_state.h"

namespace crowdb::tree
{
// Explicit, resumable diagnostics over one fixed publication. Each call is
// bounded by radix slots; collecting ordinary metrics never invokes this.
class TreeStateReconciliation
{
  public:
    explicit TreeStateReconciliation(std::shared_ptr<const PublishedTreeState> view);
    Status step(size_t max_slots);

    [[nodiscard]] bool complete() const noexcept
    {
        return complete_;
    }

    [[nodiscard]] uint64_t visited_pages() const noexcept
    {
        return visited_;
    }

    [[nodiscard]] const TreeLogicalStats &recomputed() const noexcept
    {
        return recomputed_;
    }

  private:
    friend class PublicationCoordinator;
    Status step_ledger(size_t max_slots, const PublishedTreeState &current, const CheckpointPendingMetrics &pending,
                       const MergePairMetrics &merge);
    void   inspect_membership(uint64_t pid, const PublishedPage &page);
    void   finish_ledger();
    const CheckpointPendingMetrics *pending_       = nullptr;
    const MergePairMetrics         *merge_         = nullptr;
    bool                            ledger_mode_   = false;
    bool                            started_       = false;
    uint64_t                        pending_pages_ = 0;
    uint64_t                        merge_pairs_   = 0;
    uint64_t                        merge_ready_   = 0;
    static void visit(void *context, uint64_t pid, const std::shared_ptr<const PublishedPage> &page);
    void        inspect_page();
    std::shared_ptr<const PublishedTreeState> view_;
    std::shared_ptr<const PublishedPage>      page_;
    size_t                                    next_run_        = 0;
    uint64_t                                  overlay_entries_ = 0;
    uint64_t                                  next_pid_        = 0;
    uint64_t                                  visited_         = 0;
    TreeLogicalStats                          recomputed_;
    Status                                    status_;
    bool                                      complete_ = false;
};
} // namespace crowdb::tree
