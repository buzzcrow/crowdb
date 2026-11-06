// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-rpc/control_allocator.h"

#include <gtest/gtest.h>

#include <new>

namespace crowdb::rpc
{
namespace
{
struct TestBudget
{
    size_t claimed               = 0;
    size_t allocated             = 0;
    bool   released_after_delete = false;
    bool   deleted               = false;
    bool   refuse                = false;

    static int32_t claim(void *context, size_t size, size_t /*alignment*/)
    {
        auto &budget = *static_cast<TestBudget *>(context);
        if (budget.refuse) {
            return 0;
        }
        budget.claimed += size;
        return 1;
    }

    static void record(void *context, size_t size, size_t /*alignment*/, int32_t acquired)
    {
        auto &budget = *static_cast<TestBudget *>(context);
        if (acquired != 0) {
            budget.allocated += size;
        }
        else {
            EXPECT_TRUE(budget.deleted);
            budget.allocated -= size;
        }
    }

    static void release(void *context, size_t size, size_t /*alignment*/)
    {
        auto &budget                 = *static_cast<TestBudget *>(context);
        budget.released_after_delete = budget.deleted;
        budget.claimed -= size;
    }

    ControlAllocator allocator()
    {
        return {
            .callbacks = {.version = 1, .context = this, .claim = claim, .record = record, .release = release}
        };
    }
};

struct TestControl
{
    static inline TestBudget *budget = nullptr;

    static void *operator new(size_t size)
    {
        return ::operator new(size);
    }

    static void operator delete(void *memory) noexcept
    {
        ::operator delete(memory);
        budget->deleted = true;
    }
};

TEST(ControlAllocator, PhysicalAndAdmissionRefundFollowActualDelete)
{
    TestBudget budget;
    TestControl::budget  = &budget;
    const auto allocator = budget.allocator();
    auto      *control   = allocator.create<TestControl>();
    ASSERT_NE(control, nullptr);
    EXPECT_EQ(budget.allocated, sizeof(TestControl));
    EXPECT_EQ(budget.claimed, sizeof(TestControl));
    allocator.destroy(control);
    EXPECT_TRUE(budget.released_after_delete);
    EXPECT_EQ(budget.allocated, 0);
    EXPECT_EQ(budget.claimed, 0);
}

TEST(ControlAllocator, RefusalCreatesNoAllocationAndNoRefund)
{
    TestBudget budget;
    budget.refuse        = true;
    const auto allocator = budget.allocator();
    EXPECT_EQ(allocator.create<TestControl>(), nullptr);
    EXPECT_FALSE(budget.released_after_delete);
    EXPECT_EQ(budget.allocated, 0);
    EXPECT_EQ(budget.claimed, 0);
}
} // namespace
} // namespace crowdb::rpc
