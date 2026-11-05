// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/backend_release_service.h"
#include "runtime/bootstrap_budget.h"

#include <gtest/gtest.h>

#include <atomic>
#include <latch>
#include <thread>
#include <vector>

namespace crowdb::tree
{
namespace
{
struct TestRelease : BackendReleaseService::Node
{
    std::shared_ptr<AllocationBudget> budget;
    std::atomic<size_t>              *completed;
    size_t                            remaining = 0;
    std::latch                       *entered   = nullptr;
    std::latch                       *proceed   = nullptr;

    TestRelease(std::shared_ptr<AllocationBudget> credit, std::atomic<size_t> *done, size_t steps)
        : budget(std::move(credit)),
          completed(done),
          remaining(steps)
    {
        dispose = [](BackendReleaseService::Node *base) noexcept {
            auto *node = static_cast<TestRelease *>(base);
            if (node->remaining != 0) {
                --node->remaining;
                return false;
            }
            if (node->entered != nullptr) {
                node->entered->count_down();
                node->proceed->wait();
            }
            auto allocator = MetadataAllocator<TestRelease>(node->budget);
            node->completed->fetch_add(1);
            std::destroy_at(node);
            allocator.deallocate(node, 1);
            return true;
        };
    }
};

struct TestReleaseHost
{
    std::shared_ptr<detail::BootstrapBudget> budget = detail::BootstrapBudget::create(1024 * 1024);
    std::shared_ptr<BackendReleaseService>   service;
    std::atomic<size_t>                      completed{0};

    explicit TestReleaseHost(size_t capacity = 64)
    {
        EXPECT_TRUE(BackendReleaseService::create(budget, capacity, {}, nullptr, nullptr, &service).ok());
    }

    TestRelease *prepare(size_t steps = 0)
    {
        MetadataAllocator<TestRelease> allocator(budget);
        auto                          *node = allocator.allocate(1);
        std::construct_at(node, budget, &completed, steps);
        auto status = service->register_node(*node);
        if (!status.ok()) {
            std::destroy_at(node);
            allocator.deallocate(node, 1);
            return nullptr;
        }
        return node;
    }
};

TEST(BackendReleaseService, FinalOwnerOnlyQueuesAndHostPumpOwnsActualDisposal)
{
    TestReleaseHost host;
    const auto      baseline = host.budget->snapshot().allocated;
    auto           *node     = host.prepare();
    ASSERT_NE(node, nullptr);
    const auto held = host.budget->snapshot().allocated;
    EXPECT_GT(held, baseline);
    std::thread reader([node] { BackendReleaseService::enqueue(*node); });
    reader.join();
    EXPECT_EQ(host.completed.load(), 0U);
    EXPECT_EQ(host.budget->snapshot().allocated, held);
    EXPECT_EQ(host.service->snapshot().registered, 1U);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.completed.load(), 1U);
    EXPECT_EQ(host.budget->snapshot().allocated, baseline);
}

TEST(BackendReleaseService, StopRejectsNewNodesButRetainedReaderCanReleaseLater)
{
    TestReleaseHost host(1);
    auto           *node = host.prepare();
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(host.prepare(), nullptr);
    host.service->stop_admission();
    EXPECT_FALSE(host.service->drained());
    EXPECT_EQ(host.prepare(), nullptr);
    std::weak_ptr<BackendReleaseService> weak = host.service;
    host.service.reset();
    ASSERT_FALSE(weak.expired());
    BackendReleaseService::enqueue(*node);
    auto pump = weak.lock();
    ASSERT_NE(pump, nullptr);
    EXPECT_EQ(pump->pump(1).completed, 1U);
    EXPECT_TRUE(pump->drained());
    pump.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(BackendReleaseService, FiniteBatchesPreventDeferredAndIncomingStarvation)
{
    TestReleaseHost host;
    auto           *slow = host.prepare(2);
    auto           *fast = host.prepare();
    ASSERT_NE(slow, nullptr);
    ASSERT_NE(fast, nullptr);
    BackendReleaseService::enqueue(*fast);
    BackendReleaseService::enqueue(*slow);
    EXPECT_EQ(host.service->pump(1).completed, 0U);
    auto *newer = host.prepare();
    ASSERT_NE(newer, nullptr);
    BackendReleaseService::enqueue(*newer);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.service->pump(1).completed, 0U);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.completed.load(), 3U);
}

TEST(BackendReleaseService, DrainedIsPublishedOnlyAfterActualNodeFree)
{
    TestReleaseHost host;
    const auto      baseline = host.budget->snapshot().allocated;
    auto           *node     = host.prepare();
    ASSERT_NE(node, nullptr);
    std::latch entered(1);
    std::latch proceed(1);
    node->entered = &entered;
    node->proceed = &proceed;
    BackendReleaseService::enqueue(*node);
    host.service->stop_admission();
    std::thread pump([&] { (void)host.service->pump(1); });
    entered.wait();
    EXPECT_FALSE(host.service->drained());
    EXPECT_GT(host.budget->snapshot().allocated, baseline);
    EXPECT_TRUE(host.service->pump(1).more);
    proceed.count_down();
    pump.join();
    EXPECT_TRUE(host.service->drained());
    EXPECT_EQ(host.budget->snapshot().allocated, baseline);
}

TEST(BackendReleaseService, ConcurrentFinalOwnersUseTheirPreallocatedNodes)
{
    TestReleaseHost            host;
    std::vector<TestRelease *> nodes;
    nodes.reserve(64);
    for (size_t index = 0; index < 64; ++index) {
        nodes.push_back(host.prepare());
    }
    const auto               held = host.budget->snapshot().allocated;
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (size_t thread = 0; thread < 4; ++thread) {
        readers.emplace_back([&, thread] {
            for (size_t index = thread; index < nodes.size(); index += 4) {
                BackendReleaseService::enqueue(*nodes[index]);
            }
        });
    }
    for (auto &reader : readers) {
        reader.join();
    }
    EXPECT_EQ(host.budget->snapshot().allocated, held);
    host.service->stop_admission();
    while (!host.service->drained()) {
        const auto step = host.service->pump(3);
        EXPECT_LE(step.visited, 3U);
    }
    EXPECT_EQ(host.completed.load(), 64U);
}
} // namespace
} // namespace crowdb::tree
