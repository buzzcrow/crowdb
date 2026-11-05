// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/scan_output.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
TEST(ScanOutput, GrowthClaimsBothBuffersAndFailurePreservesThePreviousAllocation)
{
    constexpr size_t capacity = 16384;
    ResourceBudget   budget({capacity, capacity, capacity, capacity, capacity, 1}, 1);
    auto             account = budget.make_account({capacity, capacity, capacity, capacity, capacity, 1});
    std::shared_ptr<AllocationAccount> output;
    ASSERT_TRUE(AllocationAccount::create(account, BudgetClass::kRouting, &output).ok());
    constexpr auto category = static_cast<size_t>(BudgetClass::kRouting);
    const auto     baseline = account->snapshot().allocated[category];
    ScanPackedBuf  packed(output);
    reserve_scan_output(packed, 100, 128);
    packed.append(std::string(100, 'v'));
    EXPECT_EQ(packed.capacity(), 128U);
    auto pressure = ResourceBudget::try_reserve(account, BudgetClass::kRouting, capacity - baseline - 128);
    ASSERT_TRUE(pressure);
    bool rejected = false;
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
    EXPECT_EQ(account->snapshot().allocated[category], baseline + 128);
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
