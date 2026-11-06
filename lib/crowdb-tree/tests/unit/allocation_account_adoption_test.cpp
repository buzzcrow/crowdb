// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"

#include <limits>

namespace crowdb::tree::detail
{
namespace
{
using namespace test_allocation_account;

TEST(AllocationAccountAdoption, FirstBindChargesEveryExistingOwnerBeforeFurtherGrowth)
{
    ResourceBudget                     host(limits(16384), 1);
    auto                               account = host.make_account(limits(8192));
    std::shared_ptr<AllocationAccount> budget;
    ASSERT_TRUE(AllocationAccount::create_unbound(&budget).ok());
    EXPECT_EQ(budget->domain(), nullptr);
    {
        MetadataAllocator<uint8_t, AllocationAccount> allocator(budget);
        auto                                         *data = allocator.allocate(103);
        EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
        ASSERT_TRUE(budget->rebind(account).ok());
        EXPECT_NE(budget->domain(), nullptr);
        const auto bound = host.snapshot().allocated[kRouting];
        EXPECT_GT(bound, 103U + sizeof(AllocationAccount));
        EXPECT_EQ(account->snapshot().physical[kMetadata], bound);
        EXPECT_FALSE(budget->claim(12288 - bound + 1));
        allocator.deallocate(data, 103);
        EXPECT_EQ(account->snapshot().allocated[kRouting], bound - 103);
    }
    budget.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(account->snapshot().physical[kMetadata], 0U);
}

TEST(AllocationAccountAdoption, InsufficientFirstQuotaLeavesUnboundStorageAndHostUntouched)
{
    ResourceBudget                     host(limits(16384), 1);
    auto                               small    = host.make_account(limits(1));
    auto                               adequate = host.make_account(limits(8192));
    std::shared_ptr<AllocationAccount> budget;
    ASSERT_TRUE(AllocationAccount::create_unbound(&budget).ok());
    ASSERT_TRUE(budget->claim(113));
    budget->record_allocation(AllocationKind::kFrame, 113, true);
    EXPECT_FALSE(budget->rebind(small).ok());
    EXPECT_EQ(budget->domain(), nullptr);
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    ASSERT_TRUE(budget->rebind(adequate).ok());
    EXPECT_EQ(adequate->snapshot().physical[kFrame], 113U);
    budget->record_allocation(AllocationKind::kFrame, 113, false);
    budget->release(113);
    budget.reset();
    EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(adequate->snapshot().physical[kFrame], 0U);
    EXPECT_EQ(adequate->snapshot().physical[kMetadata], 0U);
}

TEST(AllocationAccountAdoption, UnboundCapacityCannotWrapAndMayBeFreedWithoutBinding)
{
    std::shared_ptr<AllocationAccount> budget;
    ASSERT_TRUE(AllocationAccount::create_unbound(&budget).ok());
    EXPECT_FALSE(budget->claim(std::numeric_limits<size_t>::max()));
    ASSERT_TRUE(budget->claim(127));
    budget->record_allocation(AllocationKind::kFrame, 127, true);
    budget->record_allocation(AllocationKind::kFrame, 127, false);
    budget->release(127);
    budget.reset();
}
} // namespace
} // namespace crowdb::tree::detail
