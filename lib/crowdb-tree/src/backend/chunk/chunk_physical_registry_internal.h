// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/budget_memory_resource.h"
#include "chunk_physical_registry.h"

#include <map>

namespace crowdb::tree
{
struct ChunkPhysicalPack
{
    ChunkPhysicalPack(detail::ChunkStageId stage, detail::ChunkId chunk) : stage(stage), chunk(chunk)
    {
    }

    const detail::ChunkStageId                     stage;
    const detail::ChunkId                          chunk;
    BackingMetricExtent                            physical;
    std::optional<detail::ChunkAllocationIdentity> receipt;
    bool                                           deleted = false;
};

struct ChunkPhysicalRegistry::State : std::enable_shared_from_this<State>
{
    using Key = std::array<uint64_t, 3>;
    enum class Action : uint8_t { Prepare, Observe, Delete, Forget };

    struct Request : detail::RetirementNode
    {
        std::shared_ptr<State>                         state;
        std::shared_ptr<Request>                       queued_owner;
        Action                                         action = Action::Prepare;
        detail::ChunkStageId                           stage;
        detail::ChunkId                                chunk;
        std::shared_ptr<AllocationBudget>              metadata;
        std::shared_ptr<ChunkPhysicalPack>             pack;
        std::optional<detail::ChunkAllocationIdentity> receipt;
        Status                                         status;
        std::atomic<bool>                              done{false};
    };

    explicit State(std::shared_ptr<detail::RetirementQueue> queue) : queue(std::move(queue))
    {
    }

    bool        retire_step() noexcept;
    Status      execute(const std::shared_ptr<Request> &request);
    void        apply(Request &request);
    static bool dispatch(detail::RetirementNode *node) noexcept;
    static void abort(const std::shared_ptr<void> &owner) noexcept;

    static Key key(detail::ChunkStageId stage, detail::ChunkId chunk) noexcept
    {
        return {chunk.high, chunk.low, stage.owner_epoch};
    }

    std::shared_ptr<TreeResourceMetrics>                   backing_metrics;
    uint64_t                                               backing_scope = 0;
    std::shared_ptr<detail::RetirementQueue>               queue;
    detail::BudgetMemoryResource                           memory;
    std::pmr::map<Key, std::shared_ptr<ChunkPhysicalPack>> packs{&memory};
};
} // namespace crowdb::tree
