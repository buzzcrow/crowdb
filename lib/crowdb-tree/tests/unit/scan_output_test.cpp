// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/scan_output.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
TEST(ScanOutput, GrowthClaimsBothBuffersAndFailurePreservesThePreviousAllocation)
{
    constexpr size_t                   capacity = 16384;
    ResourceBudget                     budget({0, capacity, 0, 0, 0, 1}, 1);
    auto                               account = budget.make_account({0, capacity, 0, 0, 0, 1});
    std::shared_ptr<AllocationAccount> output;
    ASSERT_TRUE(AllocationAccount::create(account, BudgetClass::kRouting, &output).ok());
    constexpr auto category = static_cast<size_t>(BudgetClass::kRouting);
    const auto     baseline = account->snapshot().allocated[category];
    ScanPackedBuf  packed(output);
    reserve_scan_output(packed, 100, 128);
    packed.append(std::string(100, 'v'));
    EXPECT_EQ(packed.capacity(), 128U);
    const auto      occupied = capacity + (capacity / 2) - baseline - 128;
    AllocationLease pressure(output, occupied);
    bool            rejected = false;
    try {
        reserve_scan_output(packed, 100, 128);
    }
    catch (const std::bad_alloc &) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
    EXPECT_EQ(packed.capacity(), 128U);
    EXPECT_EQ(packed.size(), 100U);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(packed.data()), packed.size()), std::string(100, 'v'));
    EXPECT_EQ(account->snapshot().allocated[category], baseline + 128 + occupied);
    pressure.reset();
    reserve_scan_output(packed, 100, 128);
    EXPECT_EQ(packed.capacity(), 256U);
    EXPECT_EQ(account->snapshot().allocated[category], baseline + 256);
    output.reset();
    EXPECT_EQ(account->snapshot().allocated[category], baseline + 256);
    packed = {};
    EXPECT_EQ(account->snapshot().allocated[category], 0U);
    EXPECT_EQ(account->snapshot().reserved[category], 0U);
}
} // namespace crowdb::tree::detail
