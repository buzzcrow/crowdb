// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_runtime.h"
#include "runtime/bootstrap_budget.h"
#include "runtime/pool_storage.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <exec/static_thread_pool.hpp>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
class TestPoolBudget final : public AllocationBudget
{
  public:
    explicit TestPoolBudget(size_t limit, size_t fail_at = SIZE_MAX) : limit(limit), fail_at(fail_at)
    {
    }

    bool claim(size_t bytes) noexcept override
    {
        if (calls.fetch_add(1) == fail_at || bytes > limit - used) {
            return false;
        }
        used += bytes;
        peak = std::max(peak, used);
        return true;
    }

    void release(size_t bytes) noexcept override
    {
        used -= bytes;
    }

    void record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept override
    {
        if (kind != AllocationKind::kMetadata) {
            wrong_kind = true;
        }
        if (acquired) {
            physical += bytes;
        }
        else {
            physical -= bytes;
        }
    }

    size_t              limit;
    size_t              fail_at;
    size_t              used     = 0;
    size_t              physical = 0;
    size_t              peak     = 0;
    std::atomic<size_t> calls{0};
    bool                wrong_kind = false;
};

TEST(RuntimePoolStorage, FixedPoolAndNativeWorkerControlsUseActualMetadataBudget)
{
    const auto bound = FlushRuntime::allocation_bound(2, 4);
    if (!bound) {
        FAIL() << "pool storage bound overflow";
    }
    auto budget = std::make_shared<TestPoolBudget>(*bound);
    {
        FlushRuntime pool(2, 4, budget);
        EXPECT_GT(budget->used, 2 * PoolStorage::worker_bytes());
        EXPECT_EQ(budget->used, budget->physical);
        EXPECT_LE(budget->peak, *bound);
        EXPECT_FALSE(budget->wrong_kind);
        auto                lane = std::make_shared<FlushRuntime::Lane>(1);
        std::atomic<size_t> ran{0};
        const auto          calls = budget->calls.load();
        const auto          used  = budget->used;
        for (size_t index = 0; index < 64; ++index) {
            std::thread submitter([&] {
                EXPECT_TRUE(
                    pool.try_submit(
                            lane, [](void *p) noexcept { static_cast<std::atomic<size_t> *>(p)->fetch_add(1); }, &ran)
                        .ok());
            });
            submitter.join();
            lane->drain();
        }
        EXPECT_EQ(ran.load(), 64U);
        EXPECT_EQ(budget->calls.load(), calls);
        EXPECT_EQ(budget->used, used);
    }
    EXPECT_EQ(budget->used, 0U);
    EXPECT_EQ(budget->physical, 0U);
}

TEST(RuntimePoolStorage, FailureAtEveryStartupAllocationReturnsActualCapacity)
{
    const auto bound = FlushRuntime::allocation_bound(2, 4);
    if (!bound) {
        FAIL() << "pool storage bound overflow";
    }
    auto successful = std::make_shared<TestPoolBudget>(*bound);
    {
        FlushRuntime pool(2, 4, successful);
    }
    ASSERT_EQ(successful->used, 0U);
    const auto allocations = successful->calls.load();
    ASSERT_GT(allocations, 64U);
    for (size_t fail = 0; fail < allocations; ++fail) {
        SCOPED_TRACE(fail);
        auto budget   = std::make_shared<TestPoolBudget>(*bound, fail);
        bool rejected = false;
        try {
            FlushRuntime pool(2, 4, budget);
        }
        catch (const std::bad_alloc &) {
            rejected = true;
        }
        EXPECT_TRUE(rejected);
        EXPECT_EQ(budget->used, 0U);
        EXPECT_EQ(budget->physical, 0U);
    }
}

TEST(RuntimePoolStorage, OveralignedAllocationsFreeBeforeReturningBudget)
{
    auto        budget = BootstrapBudget::create(65536);
    const auto  before = budget->snapshot();
    PoolStorage storage(budget);
    auto       *value = storage.allocate(1024, 256);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(value) % 256, 0U);
    EXPECT_EQ(budget->snapshot().allocated, before.allocated + 1024);
    storage.deallocate(value, 1024, 256);
    EXPECT_EQ(budget->snapshot().allocated, before.allocated);
    EXPECT_EQ(budget->snapshot().physical, before.physical);
}

TEST(RuntimePoolStorage, UnrepresentableWorkerGeometryRejectsWithoutStartingThreads)
{
    EXPECT_FALSE(FlushRuntime::allocation_bound(size_t{UINT16_MAX} + 1, 1));
    EXPECT_FALSE(exec::static_thread_pool::allocation_bound(UINT32_MAX, {SIZE_MAX, 8}));
    EXPECT_FALSE(exec::static_thread_pool::allocation_bound(UINT32_MAX, {32, SIZE_MAX}));
}
} // namespace
} // namespace crowdb::tree::detail
