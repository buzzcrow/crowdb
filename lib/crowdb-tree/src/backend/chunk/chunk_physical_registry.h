// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/backing_metrics.h"
#include "chunk_fixed_pack.h"

namespace crowdb::tree
{
struct ChunkPhysicalPack;
struct BackingStatsSnapshot;
class TreeResourceMetrics;

class ChunkPhysicalAdmission
{
  public:
    ChunkPhysicalAdmission() = default;

    ~ChunkPhysicalAdmission()
    {
        if (rollback_) {
            abort_(rollback_);
        }
    }

    ChunkPhysicalAdmission(const ChunkPhysicalAdmission &)            = delete;
    ChunkPhysicalAdmission &operator=(const ChunkPhysicalAdmission &) = delete;

    void accept() noexcept
    {
        rollback_.reset();
    }

  private:
    friend class ChunkPhysicalRegistry;
    std::shared_ptr<void> rollback_;
    void (*abort_)(const std::shared_ptr<void> &) noexcept = nullptr;
};

// The stable host transport owns a canonical ledger per logical Tree across
// PageStore facades and writer reopen.
// Commands use the host retirement lane; only blocking backend/host callers may
// wait for them. No command performs I/O or walks the full allocation catalog.
class ChunkPhysicalRegistry
{
    struct State;

  public:
    static Status                                             create(const std::shared_ptr<AllocationBudget> &metadata,
                                                                     std::shared_ptr<ChunkPhysicalRegistry>  *out);
    [[nodiscard]] BackingStatsSnapshot                        backing_stats() const noexcept;
    [[nodiscard]] const std::shared_ptr<TreeResourceMetrics> &backing_metrics() const noexcept;
    [[nodiscard]] bool          uses_queue(const detail::RetirementQueue *queue) const noexcept;
    [[nodiscard]] static size_t allocation_bound() noexcept;
    [[nodiscard]] static size_t pack_allocation_bound() noexcept;
    // Admission rolls back an unallocated pack if durable staging fails.
    Status prepare(detail::ChunkStageId stage, detail::ChunkId chunk, const std::shared_ptr<AllocationBudget> &metadata,
                   std::shared_ptr<ChunkPhysicalPack> *out, ChunkPhysicalAdmission *admission = nullptr);
    Status observe(const std::shared_ptr<ChunkPhysicalPack> &pack, detail::ChunkAllocationIdentity receipt,
                   const std::shared_ptr<AllocationBudget> &metadata);
    // receipt is absent only after a durable Deleting record and an idempotent
    // physical Delete acknowledgement. The stage identity must still match.
    Status deleted(detail::ChunkStageId stage, detail::ChunkId chunk,
                   const std::optional<detail::ChunkAllocationIdentity> &receipt,
                   const std::shared_ptr<AllocationBudget>              &metadata);
    [[nodiscard]] static std::shared_ptr<BackingMetricExtent>
    extent(const std::shared_ptr<ChunkPhysicalPack> &pack) noexcept;

  private:
    explicit ChunkPhysicalRegistry(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
