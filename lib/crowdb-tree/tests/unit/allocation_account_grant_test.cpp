// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"
#include "btree/allocation_account_grant.h"

namespace crowdb::tree::detail
{
namespace
{
using namespace test_allocation_account;

TEST(BackendAllocationGrant, UsesExistingAdmissionAndRecyclesTemporaryWindows)
{
    TestAccounts test;
    auto         credit = ResourceBudget::try_reserve(test.old, BudgetClass::kRouting, 8192 - test.base);
    ASSERT_TRUE(credit);
    if (!credit) {
        return;
    }
    std::shared_ptr<BackendAllocationGrant> grant;
    ASSERT_TRUE(BackendAllocationGrant::create(test.budget, std::move(*credit), &grant).ok());
    const auto control = test.old->snapshot().allocated[kRouting] - test.base;
    ASSERT_LE(control, BackendAllocationGrant::allocation_bound());
    const auto window = 8192 - test.base - control;
    ASSERT_TRUE(test.budget->claim(1));
    test.budget->release(1);
    {
        MetadataAllocator<uint8_t, BackendAllocationGrant> allocator(grant);
        for (size_t i = 0; i != 3; ++i) {
            auto *bytes = allocator.allocate(window);
            EXPECT_EQ(test.host.snapshot().allocated[kRouting], 8192U);
            EXPECT_EQ(test.old->snapshot().physical[kMetadata], 8192U);
            allocator.deallocate(bytes, window);
            EXPECT_EQ(test.host.snapshot().reserved[kRouting], window);
        }
    }
    grant->seal();
    EXPECT_EQ(test.host.snapshot().reserved[kRouting], 0U);
    grant.reset();
    test.expect_only_control(test.old);
    test.finish();
}

TEST(BackendAllocationGrant, RetainedAllocationMovesWithBackendAfterOperationSeals)
{
    TestAccounts test;
    auto         credit = ResourceBudget::try_reserve(test.old, BudgetClass::kRouting, 2048);
    ASSERT_TRUE(credit);
    if (!credit) {
        return;
    }
    std::shared_ptr<BackendAllocationGrant> grant;
    ASSERT_TRUE(BackendAllocationGrant::create(test.budget, std::move(*credit), &grant).ok());
    {
        MetadataAllocator<uint8_t, BackendAllocationGrant> allocator(grant);
        auto                                              *bytes = allocator.allocate(133);
        grant->seal();
        const auto retained = test.host.snapshot().allocated[kRouting];
        ASSERT_TRUE(test.budget->rebind(test.next).ok());
        EXPECT_EQ(test.old->snapshot().allocated[kRouting], 0U);
        EXPECT_EQ(test.next->snapshot().physical[kMetadata], retained);
        grant.reset();
        allocator.deallocate(bytes, 133);
    }
    test.expect_only_control(test.next);
    test.finish();
}

TEST(BackendAllocationGrant, TargetQuotaRefusalReturnsSourceCreditWithoutDoubleCharge)
{
    TestAccounts test;
    const auto   blocker = 12288 - test.base;
    ASSERT_TRUE(test.budget->claim(blocker));
    auto source = ResourceBudget::try_reserve(test.next, BudgetClass::kRouting, 2048);
    ASSERT_TRUE(source);
    if (!source) {
        return;
    }
    const auto                              host_before = test.host.snapshot();
    std::shared_ptr<BackendAllocationGrant> grant;
    EXPECT_FALSE(BackendAllocationGrant::create(test.budget, std::move(*source), &grant).ok());
    EXPECT_FALSE(grant);
    EXPECT_EQ(test.next->snapshot().allocated[kRouting], 0U);
    EXPECT_EQ(test.next->snapshot().reserved[kRouting], 0U);
    EXPECT_EQ(test.host.snapshot().allocated[kRouting], host_before.allocated[kRouting]);
    EXPECT_EQ(test.host.snapshot().reserved[kRouting], host_before.reserved[kRouting] - 2048);
    test.budget->release(blocker);
    test.finish();
}

TEST(BackendAllocationGrant, FinalFreeDuringTransferUsesPendingDeltaAndNeverRevivesReserve)
{
    TestAccounts test;
    auto         credit = ResourceBudget::try_reserve(test.old, BudgetClass::kRouting, 2048);
    ASSERT_TRUE(credit);
    if (!credit) {
        return;
    }
    std::shared_ptr<BackendAllocationGrant> grant;
    ASSERT_TRUE(BackendAllocationGrant::create(test.budget, std::move(*credit), &grant).ok());
    {
        MetadataAllocator<uint8_t, BackendAllocationGrant> allocator(grant);
        auto                                              *bytes = allocator.allocate(149);
        grant->seal();
        bool freed = false;
        ASSERT_TRUE(AllocationAccountTestAccess::rebind_for_tests(*test.budget, test.next, [&](Step step, size_t) {
                        if (step == Step::kClosed) {
                            allocator.deallocate(bytes, 149);
                            freed = true;
                        }
                    }).ok());
        EXPECT_TRUE(freed);
        EXPECT_EQ(test.host.snapshot().reserved[kRouting], 0U);
    }
    grant.reset();
    test.expect_only_control(test.next);
    test.finish();
}
} // namespace
} // namespace crowdb::tree::detail
