// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_physical_registry.h"
#include "btree/retirement_queue.h"
#include "btree/tree_metrics.h"
#include "runtime/bootstrap_budget.h"

#include <gtest/gtest.h>

namespace crowdb::tree::test_chunk
{
struct TestPhysicalBudget : AllocationBudget
{
    std::atomic<int>                       allowances{-1};
    size_t                                 limit = SIZE_MAX;
    std::atomic<size_t>                    claimed{0};
    std::atomic<bool>                      reject{false};
    std::weak_ptr<detail::RetirementQueue> queue;
    std::shared_ptr<TreeResourceMetrics>   metrics = std::make_shared<TreeResourceMetrics>();

    bool claim(size_t bytes) noexcept override
    {
        auto permitted = allowances.load();
        while (permitted >= 0) {
            if (permitted == 0) {
                return false;
            }
            if (allowances.compare_exchange_weak(permitted, permitted - 1)) {
                break;
            }
        }
        if (reject.load()) {
            return false;
        }
        auto previous = claimed.load();
        do {
            if (bytes > limit - previous) {
                return false;
            }
        } while (!claimed.compare_exchange_weak(previous, previous + bytes));
        return true;
    }

    void release(size_t bytes) noexcept override
    {
        claimed.fetch_sub(bytes);
    }

    [[nodiscard]] std::shared_ptr<detail::RetirementQueue> retirement_queue() const noexcept override
    {
        return queue.lock();
    }

    [[nodiscard]] std::shared_ptr<TreeResourceMetrics> resource_metrics() const noexcept override
    {
        return metrics;
    }
};

struct TestPhysicalHost
{
    std::shared_ptr<TestPhysicalBudget>         budget = std::make_shared<TestPhysicalBudget>();
    detail::FlushRuntime                        cpu{1, 4, budget};
    std::shared_ptr<detail::FlushRuntime::Lane> lane = std::make_shared<detail::FlushRuntime::Lane>(4);
    std::shared_ptr<detail::RetirementQueue>    queue;
    std::shared_ptr<ChunkPhysicalRegistry>      registry;

    TestPhysicalHost()
    {
        // Keep the queue allocator independent of budget's weak queue reference.
        auto lifecycle = detail::BootstrapBudget::create(65536);
        EXPECT_TRUE(detail::RetirementQueue::create(cpu, lane, lifecycle, &queue).ok());
        budget->queue = queue;
        EXPECT_TRUE(ChunkPhysicalRegistry::create(budget, &registry).ok());
    }

    ~TestPhysicalHost()
    {
        registry.reset();
        lane->drain();
        queue->close_and_drain();
    }

    static constexpr detail::ChunkStageId            stage{.owner_epoch = 1, .operation_high = 10, .operation_low = 20};
    static constexpr detail::ChunkId                 chunk{uint64_t{2} << 56, 1};
    static constexpr detail::ChunkAllocationIdentity receipt{
        .chunk = chunk, .owner_epoch = 1, .created_ms = 37, .physical_bytes = 8192};
    static constexpr std::array<uint64_t, 8> identity{1, 10, 20, chunk.high, 1, 0, 32, 2};

    std::shared_ptr<ChunkPhysicalPack> prepare()
    {
        std::shared_ptr<ChunkPhysicalPack> result;
        EXPECT_TRUE(registry->prepare(stage, chunk, budget, &result).ok());
        return result;
    }
};
} // namespace crowdb::tree::test_chunk
