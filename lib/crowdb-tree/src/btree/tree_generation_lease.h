// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "crowdb-tree/status.h"

namespace crowdb::tree
{
class TreeCore;
struct PublishedTreeState;
struct SplitGeneration;
struct TreeGenerationPreparation;
struct SourceMemoryStats;

// The caller retains TreeCore throughout this host operation. Acquiring the
// lease drains accepted work in the background; commit only installs storage
// which preparation already owns. Cancellation reopens the unchanged child.
class TreeGenerationLease
{
  public:
    static Status prepare(TreeCore &owner, std::shared_ptr<const PublishedTreeState> candidate,
                          const SplitGeneration *split, std::shared_ptr<TreeGenerationLease> *out);
    ~TreeGenerationLease();

    Status acquire();
    Status commit() noexcept;
    // Before publication only: the independent tail has stopped growing.
    Status set_split_frontier(uint64_t frontier, SourceMemoryStats memory) noexcept;
    void   cancel() noexcept;

  private:
    explicit TreeGenerationLease(detail::ReservedUnique<TreeGenerationPreparation> state);
    detail::ReservedUnique<TreeGenerationPreparation> state_;
};
} // namespace crowdb::tree
