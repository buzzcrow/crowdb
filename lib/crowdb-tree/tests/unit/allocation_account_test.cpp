// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"
#include "btree/source_write_budget.h"

namespace crowdb::tree::detail
{
namespace
{
using namespace test_allocation_account;

TEST(AllocationAccount, ActualMemoryCanReachTheMaximumAndStopsNewGrowthAboveOneAndAHalf)
{
    constexpr size_t       maximum = 1024 * 1024;
    ResourceBudget::Limits limits{};
    limits[kRouting]                                          = maximum / 2;
    limits[static_cast<size_t>(BudgetClass::kAdmittedWriter)] = maximum / 2;
    limits[static_cast<size_t>(BudgetClass::kIo)]             = 4;
    ResourceBudget                     host(limits, 4);
    auto                               account = host.make_account(limits);
    std::shared_ptr<AllocationAccount> owner;
    ASSERT_TRUE(AllocationAccount::create(account, BudgetClass::kRouting, &owner).ok());
    const auto baseline = account->snapshot().allocated[kRouting];
    auto       accepted = ResourceBudget::try_reserve(account, BudgetClass::kRouting, 16);
    if (!accepted) {
        FAIL() << "missing accepted completion credit";
    }
    {
        BudgetAllocator<uint8_t, AllocationAccount, AllocationKind::kFrame> allocator(owner);
        auto *first = allocator.allocate(maximum - baseline);
        EXPECT_EQ(account->snapshot().allocated[kRouting], maximum);
        auto *second = allocator.allocate(maximum / 2);
        EXPECT_EQ(account->snapshot().allocated[kRouting], maximum + (maximum / 2));
        EXPECT_THROW(
            {
                auto *rejected = allocator.allocate(1);
                allocator.deallocate(rejected, 1);
            },
            std::bad_alloc);
        auto completion = accepted.value().allocate(16);
        ASSERT_TRUE(completion);
        EXPECT_FALSE(ResourceBudget::try_reserve(account, BudgetClass::kAdmittedWriter, 1));
        SourceWriteReservation rejected;
        EXPECT_EQ(SourceWriteReservation::create({.account = account, .max_entries = 1, .max_gap_entries = 1},
                                                 {.entries = 1, .key_bytes = 1}, BudgetClass::kAdmittedWriter,
                                                 &rejected)
                      .code(),
                  Code::kResourceExhausted);
        completion.reset();
        EXPECT_TRUE(ResourceBudget::try_reserve(account, BudgetClass::kAdmittedWriter, 1));
        allocator.deallocate(first, maximum - baseline);
        auto *resumed = allocator.allocate(1);
        allocator.deallocate(resumed, 1);
        allocator.deallocate(second, maximum / 2);
        EXPECT_EQ(account->snapshot().allocated[kRouting], baseline);
    }
    owner.reset();
    EXPECT_EQ(account->snapshot().allocated[kRouting], 0U);
}

TEST(AllocationAccount, UnusedHostReservationDoesNotBlockActualAllocationOrOwnershipTransfer)
{
    TestAccounts test;
    {
        BudgetAllocator<uint8_t, AllocationAccount, AllocationKind::kFrame> allocator(test.budget);
        auto                                                               *bytes = allocator.allocate(128);
        auto filler   = test.host.make_account(limits(16384));
        auto capacity = ResourceBudget::try_reserve(filler, BudgetClass::kRouting, 16384 - test.base - 128);
        ASSERT_TRUE(capacity);
        ASSERT_TRUE(test.budget->claim(1));
        test.budget->release(1);
        const auto before = test.host.snapshot();
        ASSERT_TRUE(test.budget->rebind(test.next).ok());
        EXPECT_EQ(test.host.snapshot().allocated[kRouting], before.allocated[kRouting]);
        EXPECT_EQ(test.host.snapshot().reserved[kRouting], before.reserved[kRouting]);
        EXPECT_EQ(test.old->snapshot().allocated[kRouting], 0U);
        EXPECT_EQ(test.old->snapshot().physical[kMetadata], 0U);
        EXPECT_EQ(test.old->snapshot().physical[kFrame], 0U);
        EXPECT_EQ(test.next->snapshot().allocated[kRouting], test.base + 128);
        EXPECT_EQ(test.next->snapshot().physical[kFrame], 128U);
        allocator.deallocate(bytes, 128);
        capacity.reset();
    }
    test.expect_only_control(test.next);
    test.finish();
}

TEST(AllocationAccount, TargetQuotaFailureLeavesOriginalAccountUsable)
{
    TestAccounts test;
    auto         small = test.host.make_account(limits(test.base / 2));
    EXPECT_FALSE(test.budget->rebind(small).ok());
    test.expect_only_control(test.old);
    EXPECT_EQ(small->snapshot().allocated[kRouting], 0U);
    ASSERT_TRUE(test.budget->claim(17));
    test.budget->release(17);
    ASSERT_TRUE(test.budget->rebind(test.next).ok());
    EXPECT_FALSE(test.budget->claim(12288 - test.base + 1));
    ASSERT_TRUE(test.budget->claim(12288 - test.base));
    test.budget->release(12288 - test.base);
    test.finish();
}

TEST(AllocationAccount, ClaimRecordAndFreeReleaseMayStraddleBothTransfers)
{
    TestAccounts test;
    ASSERT_TRUE(test.budget->claim(37));
    ASSERT_TRUE(test.budget->rebind(test.next).ok());
    EXPECT_EQ(test.next->snapshot().allocated[kRouting], test.base + 37);
    EXPECT_EQ(test.next->snapshot().physical[kFrame], 0U);
    test.budget->record_allocation(AllocationKind::kFrame, 37, true);
    EXPECT_EQ(test.next->snapshot().physical[kFrame], 37U);
    test.budget->record_allocation(AllocationKind::kFrame, 37, false);
    ASSERT_TRUE(test.budget->rebind(test.old).ok());
    EXPECT_EQ(test.old->snapshot().allocated[kRouting], test.base + 37);
    EXPECT_EQ(test.old->snapshot().physical[kFrame], 0U);
    test.budget->release(37);
    test.expect_only_control(test.old);
    test.finish();
}

TEST(AllocationAccount, WeakControlRetainsItsActualChargeUntilFinalFree)
{
    TestAccounts                     test;
    std::weak_ptr<AllocationAccount> weak = test.budget;
    ASSERT_TRUE(test.budget->rebind(test.next).ok());
    test.budget.reset();
    EXPECT_TRUE(weak.expired());
    test.expect_only_control(test.next);
    weak.reset();
    test.finish();
}

TEST(AllocationAccount, ForeignHostAndFailedBootstrapDoNotLeakAdmission)
{
    TestAccounts   test;
    ResourceBudget foreign(limits(16384), 1);
    auto           destination = foreign.make_account(limits(16384));
    EXPECT_FALSE(test.budget->rebind(destination).ok());
    test.expect_only_control(test.old);
    EXPECT_EQ(foreign.snapshot().allocated[kRouting], 0U);
    auto                               small = test.host.make_account(limits(sizeof(AllocationAccount) * 2 / 3));
    std::shared_ptr<AllocationAccount> result;
    EXPECT_FALSE(AllocationAccount::create(small, BudgetClass::kRouting, &result).ok());
    EXPECT_FALSE(result);
    EXPECT_EQ(small->snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(small->snapshot().reserved[kRouting], 0U);
    EXPECT_EQ(small->snapshot().physical[kMetadata], 0U);
    EXPECT_FALSE(AllocationAccount::create(test.old, BudgetClass::kIo, &result).ok());
    EXPECT_FALSE(AllocationAccount::create(test.old, BudgetClass::kRouting, nullptr).ok());
    test.expect_only_control(test.old);
    test.finish();
}
} // namespace
} // namespace crowdb::tree::detail
