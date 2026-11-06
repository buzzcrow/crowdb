// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "btree/retired_shared.h"
#include "btree/tree_metrics.h"
#include "crowdb-tree/memtable/memtable.h"

#include <gtest/gtest.h>

#include <chrono>
#include <latch>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
struct TestBudget : AllocationBudget
{
    std::atomic<size_t> claimed{0}, physical{0};
    std::atomic<bool>   reject{false};

    bool claim(size_t bytes) noexcept override
    {
        if (reject.load()) {
            return false;
        }
        claimed.fetch_add(bytes);
        return true;
    }

    void release(size_t bytes) noexcept override
    {
        claimed.fetch_sub(bytes);
    }

    void record_allocation(AllocationKind /*kind*/, size_t bytes, bool acquired) noexcept override
    {
        if (acquired) {
            physical.fetch_add(bytes);
        }
        else {
            physical.fetch_sub(bytes);
        }
    }
};

struct Chain
{
    std::shared_ptr<Chain> next;
    std::atomic<size_t>   *destroyed;

    ~Chain()
    {
        destroyed->fetch_add(1);
    }
};

TEST(RetirementQueue, LastOwnerSchedulesBoundedChainDisposalAndKeepsExactStorageCharged)
{
    auto budget = std::make_shared<TestBudget>();
    {
        FlushRuntime                     runtime(1, 2, budget);
        auto                             lane = std::make_shared<FlushRuntime::Lane>(2);
        std::shared_ptr<RetirementQueue> queue;
        ASSERT_TRUE(RetirementQueue::create(runtime, lane, budget, &queue).ok());
        std::atomic<size_t>    destroyed{0};
        std::shared_ptr<Chain> chain;
        for (size_t i = 0; i < 10000; ++i) {
            chain = retired_shared<Chain>(budget, queue, std::move(chain), &destroyed);
        }

        struct Gate
        {
            std::latch entered{1}, release{1};
        } gate;

        ASSERT_TRUE(runtime
                        .try_submit(
                            lane,
                            [](void *context) noexcept {
                                auto &gate = *static_cast<Gate *>(context);
                                gate.entered.count_down();
                                gate.release.wait();
                            },
                            &gate)
                        .ok());
        gate.entered.wait();
        const auto before = budget->physical.load();
        chain.reset();
        EXPECT_EQ(destroyed.load(), 0U);
        EXPECT_LT(budget->physical.load(), before); // independent shared control was freed
        EXPECT_GT(budget->physical.load(), 10000 * sizeof(RetiredObject<Chain>));
        gate.release.count_down();
        lane->drain();
        EXPECT_EQ(destroyed.load(), 10000U);
        const auto work = queue->snapshot();
        EXPECT_EQ(work.enqueued, 10000U);
        EXPECT_EQ(work.freed, 10000U);
        EXPECT_GT(work.cpu_steps, 1U);
        EXPECT_LE(work.max_step_nodes, RetirementQueue::kNodesPerStep);
        queue->close_and_drain();
    }
    EXPECT_EQ(budget->physical.load(), 0U);
    EXPECT_EQ(budget->claimed.load(), 0U);
}

TEST(RetirementQueue, LateExternalOwnerFreesAfterExecutorDestructionWithoutRecursion)
{
    auto                             budget = std::make_shared<TestBudget>();
    std::shared_ptr<RetirementQueue> queue;
    std::shared_ptr<Chain>           chain;
    std::atomic<size_t>              destroyed{0};
    {
        FlushRuntime runtime(1, 1, budget);
        auto         lane = std::make_shared<FlushRuntime::Lane>(1);
        ASSERT_TRUE(RetirementQueue::create(runtime, lane, budget, &queue).ok());
        for (size_t i = 0; i < 50000; ++i) {
            chain = retired_shared<Chain>(budget, queue, std::move(chain), &destroyed);
        }
        queue->close_and_drain();
    }
    EXPECT_EQ(destroyed.load(), 0U);
    queue.reset();
    EXPECT_GT(budget->physical.load(), 50000 * sizeof(RetiredObject<Chain>));
    chain.reset();
    EXPECT_EQ(destroyed.load(), 50000U);
    EXPECT_EQ(budget->physical.load(), 0U);
    EXPECT_EQ(budget->claimed.load(), 0U);
}

TEST(RetirementQueue, ClosedQueuesUseOneThreadTrampolineForCrossDomainChains)
{
    auto                   budget = std::make_shared<TestBudget>();
    std::shared_ptr<Chain> chain;
    std::atomic<size_t>    destroyed{0};
    {
        FlushRuntime runtime(1, 1, budget);
        auto         lane = std::make_shared<FlushRuntime::Lane>(1);
        for (size_t i = 0; i < 3000; ++i) {
            std::shared_ptr<RetirementQueue> queue;
            ASSERT_TRUE(RetirementQueue::create(runtime, lane, budget, &queue).ok());
            chain = retired_shared<Chain>(budget, queue, std::move(chain), &destroyed);
            queue->close_and_drain();
        }
    }
    chain.reset();
    EXPECT_EQ(destroyed.load(), 3000U);
    EXPECT_EQ(budget->physical.load(), 0U);
    EXPECT_EQ(budget->claimed.load(), 0U);
}

TEST(RetirementQueue, RealMemTableLastOwnerSlicesNodesAndPreservesDetachedGuard)
{
    auto                             queue_budget = std::make_shared<TestBudget>();
    FlushRuntime                     runtime(1, 2, queue_budget);
    auto                             lane = std::make_shared<FlushRuntime::Lane>(2);
    std::shared_ptr<RetirementQueue> queue;
    ASSERT_TRUE(RetirementQueue::create(runtime, lane, queue_budget, &queue).ok());
    ResourceBudget::Limits limits{};
    constexpr auto         category = static_cast<size_t>(BudgetClass::kRouting);
    limits[category]                = 16 * 1024 * 1024;
    ResourceBudget host(limits, 1);
    host.set_retirement_queue(queue);
    auto account  = host.make_account(limits);
    auto admitted = ResourceBudget::try_reserve(account, BudgetClass::kRouting, limits[category]);
    ASSERT_TRUE(admitted);
    if (!admitted) {
        return;
    }
    auto       credit   = AllocationReservation::create(std::move(*admitted));
    const auto baseline = account->snapshot().allocated[category];
    auto       table    = reserved_shared<MemTable>(credit, 1, nullptr, credit);
    for (size_t i = 0; i < 3000; ++i) {
        std::string key  = std::to_string(i);
        buffer      cell = buffer::alloc(64);
        std::fill_n(cell.data(), cell.size(), uint8_t{17});
        ASSERT_TRUE(table->upsert(key, i + 1, std::move(cell)));
    }
    auto        guard = table->read_guard(credit);
    const auto *cell  = table->find("1024");
    ASSERT_NE(cell, nullptr);
    table.reset();
    lane->drain();
    EXPECT_GT(account->snapshot().allocated[category], baseline);
    EXPECT_EQ(cell->cell.size(), 64U);
    guard = {};
    lane->drain();
    EXPECT_EQ(account->snapshot().allocated[category], baseline);
    EXPECT_LE(queue->snapshot().max_step_nodes, RetirementQueue::kNodesPerStep);
    credit->seal();
    credit.reset();
    EXPECT_EQ(account->snapshot().allocated[category], 0U);
    queue->close_and_drain();
}

TEST(RetirementQueue, ConcurrentLateOwnersAndNestedReleasesDrainExactlyOnce)
{
    auto                             budget = std::make_shared<TestBudget>();
    std::shared_ptr<RetirementQueue> queue;
    {
        FlushRuntime runtime(1, 1, budget);
        auto         lane = std::make_shared<FlushRuntime::Lane>(1);
        ASSERT_TRUE(RetirementQueue::create(runtime, lane, budget, &queue).ok());
        queue->close_and_drain();
    }
    std::atomic<size_t>                   destroyed{0};
    std::array<std::shared_ptr<Chain>, 8> chains;
    for (auto &chain : chains) {
        for (size_t i = 0; i < 2000; ++i) {
            chain = retired_shared<Chain>(budget, queue, std::move(chain), &destroyed);
        }
    }
    std::array<std::thread, 8> threads;
    std::latch                 start{8};
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([&, i] {
            start.count_down();
            start.wait();
            chains[i].reset();
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(destroyed.load(), 16000U);
    EXPECT_EQ(queue->snapshot().freed, 16000U);
    queue.reset();
    EXPECT_EQ(budget->physical.load(), 0U);
    EXPECT_EQ(budget->claimed.load(), 0U);
}

class EpochRetirement : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(RetirementQueue::create(runtime, lane, queue_budget, &queue).ok());
        host.set_retirement_queue(queue);
        account       = host.make_account(limits());
        auto admitted = ResourceBudget::try_reserve(account, BudgetClass::kRouting, limits()[1]);
        ASSERT_TRUE(admitted);
        if (!admitted) {
            return;
        }
        credit = AllocationReservation::create(std::move(*admitted));
    }

    void TearDown() override
    {
        account->drain_retirements();
        credit->seal();
        credit.reset();
        EXPECT_EQ(account->snapshot().allocated[1], 0U);
        queue->close_and_drain();
    }

    static ResourceBudget::Limits limits()
    {
        ResourceBudget::Limits result{};
        result[1] = 16 * 1024 * 1024;
        return result;
    }

    struct Entry : EpochManager::Deferred
    {
        AllocationLease      credit;
        std::atomic<size_t> *freed;
    };

    void enqueue(EpochManager &epoch, std::atomic<size_t> &freed)
    {
        AllocationLease lease(credit, sizeof(Entry), AllocationKind::kMetadata);
        auto           *entry = new Entry;
        entry->credit         = std::move(lease);
        entry->freed          = &freed;
        entry->destroy        = [](EpochManager::Deferred *node) noexcept {
            auto *entry = static_cast<Entry *>(node);
            auto  lease = std::move(entry->credit);
            entry->freed->fetch_add(1);
            delete entry;
        };
        epoch.defer(entry);
    }

    std::shared_ptr<TestBudget>              queue_budget = std::make_shared<TestBudget>();
    FlushRuntime                             runtime{1, 2, queue_budget};
    std::shared_ptr<FlushRuntime::Lane>      lane = std::make_shared<FlushRuntime::Lane>(2);
    std::shared_ptr<RetirementQueue>         queue;
    ResourceBudget                           host{limits(), 1};
    std::shared_ptr<ResourceBudget::Account> account;
    std::shared_ptr<AllocationReservation>   credit;
};

TEST_F(EpochRetirement, PeriodicCollectionStopsAtReaderAndResumesOnItsExit)
{
    EpochManager        epoch(credit);
    auto                guard = epoch.enter();
    std::atomic<size_t> freed{0};
    for (size_t i = 0; i < 3000; ++i) {
        enqueue(epoch, freed);
    }
    lane->drain();
    account->drain_retirements();
    EXPECT_EQ(freed.load(), 0U);
    EXPECT_EQ(epoch.pending_retired(), 3000U);
    const auto stopped = queue->snapshot().cpu_steps;
    lane->drain();
    EXPECT_EQ(queue->snapshot().cpu_steps, stopped);
    guard = {};
    lane->drain();
    account->drain_retirements();
    EXPECT_EQ(freed.load(), 3000U);
    EXPECT_EQ(epoch.pending_retired(), 0U);
    EXPECT_LE(queue->snapshot().max_step_nodes, 64U);
}

TEST_F(EpochRetirement, ConcurrentDefersAreCollectedWithoutAnExplicitSweep)
{
    EpochManager               epoch(credit);
    std::atomic<size_t>        freed{0};
    std::array<std::thread, 4> producers;
    for (auto &thread : producers) {
        thread = std::thread([&] {
            for (size_t i = 0; i < 3000; ++i) {
                enqueue(epoch, freed);
            }
        });
    }
    for (auto &thread : producers) {
        thread.join();
    }
    lane->drain();
    account->drain_retirements();
    EXPECT_EQ(freed.load(), 12000U);
    EXPECT_EQ(epoch.pending_retired(), 0U);
}

struct Observed
{
    static constexpr bool            observe_retirement = true;
    std::shared_ptr<TreeWorkMetrics> metrics;
    bool                            *destroyed;

    ~Observed()
    {
        EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)].inflight, 1U);
        *destroyed = true;
    }
};

TEST_F(EpochRetirement, AggregateWaitMetricsEndAfterPhysicalObjectRelease)
{
    auto metrics = std::make_shared<TreeWorkMetrics>();
    account->set_work_metrics(metrics);
    bool destroyed = false;
    auto value     = retired_shared<Observed>(credit, queue, metrics, &destroyed);
    value.reset();
    account->drain_retirements();
    const auto totals = metrics->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_TRUE(destroyed);
    EXPECT_EQ(totals.attempts, 1U);
    EXPECT_EQ(totals.inflight, 0U);
    EXPECT_EQ(totals.count, 1U);
    EXPECT_EQ(totals.outcomes[static_cast<size_t>(TreeOutcome::Success)], 1U);
}

struct LogicalObserved
{
    static constexpr bool observe_retirement = true;
    RetirementObserver    retirement;
};

TEST_F(EpochRetirement, LogicalRetirementIncludesOldOwnerHoldAndDoesNotDoubleCount)
{
    auto metrics   = std::make_shared<TreeWorkMetrics>();
    auto resources = std::make_shared<TreeResourceMetrics>();
    account->set_work_metrics(metrics);
    account->set_resource_metrics(resources);
    auto value      = retired_shared<LogicalObserved>(credit, queue);
    auto old_reader = value;
    value->retirement.mark();
    value->retirement.mark();
    value.reset();
    account->drain_retirements();
    const auto held = metrics->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_EQ(held.attempts, 1U);
    EXPECT_EQ(held.inflight, 1U);
    EXPECT_EQ(held.count, 0U);
    EXPECT_EQ(resources->snapshot().retired_bytes, sizeof(RetiredObject<LogicalObserved>));
    old_reader.reset();
    account->drain_retirements();
    const auto released = metrics->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_EQ(released.attempts, 1U);
    EXPECT_EQ(released.inflight, 0U);
    EXPECT_EQ(released.count, 1U);
    EXPECT_EQ(resources->snapshot().retired_bytes, 0U);
}

TEST_F(EpochRetirement, CopiedImmutableDescriptorsReceiveIndependentObservers)
{
    auto metrics   = std::make_shared<TreeWorkMetrics>();
    auto resources = std::make_shared<TreeResourceMetrics>();
    account->set_work_metrics(metrics);
    account->set_resource_metrics(resources);
    auto first  = retired_shared<LogicalObserved>(credit, queue);
    auto second = retired_shared<LogicalObserved>(credit, queue, *first);
    first->retirement.mark();
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)].attempts, 1U);
    second->retirement.mark();
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)].attempts, 2U);
    EXPECT_EQ(resources->snapshot().retired_bytes, 2 * sizeof(RetiredObject<LogicalObserved>));
    first.reset();
    account->drain_retirements();
    EXPECT_EQ(resources->snapshot().retired_bytes, sizeof(RetiredObject<LogicalObserved>));
    second.reset();
    account->drain_retirements();
    EXPECT_EQ(resources->snapshot().retired_bytes, 0U);
}

TEST_F(EpochRetirement, EveryDeferralDuringCollectorCompletionHasItsOwnProgress)
{
    EpochManager        epoch(credit);
    std::atomic<size_t> freed{0};
    constexpr size_t    entries      = 50000;
    const auto          deadline     = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    size_t              acknowledged = 0;
    for (size_t index = 1; index <= entries; ++index) {
        enqueue(epoch, freed);
        while (freed.load(std::memory_order_acquire) < index && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        if (freed.load(std::memory_order_acquire) != index) {
            break;
        }
        acknowledged = index;
    }
    EXPECT_EQ(acknowledged, entries) << "pending=" << epoch.pending_retired()
                                     << " queue=" << (queue->snapshot().enqueued - queue->snapshot().freed)
                                     << " account=" << account->retirements_pending_for_tests();
    // Cleanup must remain possible after recording a missing autonomous wake.
    if (acknowledged != entries) {
        (void)epoch.try_reclaim();
    }
    lane->drain();
    account->drain_retirements();
}
} // namespace
} // namespace crowdb::tree::detail
