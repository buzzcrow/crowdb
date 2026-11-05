// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
TEST(ResourceAccountStats, ReservationTransfersAndSealsRemainAccountLocal)
{
    ResourceBudget budget({1024, 1024, 1024, 1024, 1024, 16}, 4);
    auto           first    = budget.make_account({1024, 1024, 1024, 1024, 1024, 16});
    auto           second   = budget.make_account({1024, 1024, 1024, 1024, 1024, 16});
    constexpr auto category = static_cast<size_t>(BudgetClass::kRouting);
    auto           a        = ResourceBudget::try_reserve(first, BudgetClass::kRouting, 300);
    auto           b        = ResourceBudget::try_reserve(second, BudgetClass::kRouting, 500);
    if (!a || !b) {
        ADD_FAILURE() << "both account reservations must succeed";
        return;
    }
    auto allocated = a.value().allocate(100);
    ASSERT_TRUE(allocated);
    auto split = a.value().split(50);
    ASSERT_TRUE(split);
    EXPECT_EQ(first->snapshot().reserved[category], 200U);
    EXPECT_EQ(first->snapshot().allocated[category], 100U);
    split.reset();
    AllocationReservation remaining(std::move(a.value()));
    ASSERT_TRUE(remaining.claim(64));
    EXPECT_EQ(first->snapshot().reserved[category], 86U);
    EXPECT_EQ(first->snapshot().allocated[category], 164U);
    auto transferred = remaining.take_reserved(16);
    ASSERT_TRUE(transferred);
    EXPECT_EQ(first->snapshot().reserved[category], 86U);
    transferred.reset();
    remaining.release(64);
    remaining.seal();
    EXPECT_EQ(first->snapshot().reserved[category], 0U);
    EXPECT_EQ(first->snapshot().allocated[category], 100U);
    EXPECT_EQ(second->snapshot().reserved[category], 500U);
    EXPECT_EQ(second->snapshot().allocated[category], 0U);
    allocated.reset();
    EXPECT_EQ(first->snapshot().allocated[category], 0U);
    b.reset();
    EXPECT_EQ(second->snapshot().reserved[category], 0U);
}
} // namespace crowdb::tree::detail
