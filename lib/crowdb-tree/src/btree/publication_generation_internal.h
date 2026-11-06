// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "page_cache_residency.h"
#include "published_state.h"

namespace crowdb::tree
{
struct PublicationCoordinator::CheckpointLedger
{
    bool retire_step() noexcept
    {
        return pending.retire_step() && merge.retire_step();
    }

    explicit CheckpointLedger(std::pmr::memory_resource *upstream)
        : resource(upstream),
          pending(&resource),
          merge(&resource)
    {
    }

    detail::BudgetMemoryResource resource;
    CheckpointPendingMetrics     pending;
    MergePairMetrics             merge;
};

class PublicationCoordinator::Replacement
{
  public:
    ~Replacement()
    {
        if (root != nullptr) {
            Root::dispose(root);
        }
    }

  private:
    friend class PublicationCoordinator;
    std::shared_ptr<PublishedTreeState>         state;
    std::shared_ptr<CheckpointLedger>           checkpoint;
    std::shared_ptr<PageCacheResidency::Update> residency;
    Root                                       *root = nullptr;
};
} // namespace crowdb::tree
