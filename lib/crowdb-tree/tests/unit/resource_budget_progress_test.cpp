// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/resource_budget.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
ResourceBudget::Limits limits(size_t bytes)
{
    ResourceBudget::Limits result;
    result.fill(bytes);
    return result;
}

struct TestCompletion
{
    using receiver_concept = stdexec::receiver_tag;
    Status *status;
    bool   *done;

    void set_value() const noexcept
    {
        *done = true;
    }

    void set_error(Status error) const noexcept
    {
        *status = std::move(error);
        *done   = true;
    }

    void set_stopped() const noexcept
    {
        *status = Status::unavailable();
        *done   = true;
    }
};

TEST(ResourceBudgetProgress, ReservedCreditCanReleaseButConversionToLiveAllocationRejectsSoftWork)
{
    ResourceBudget budget(limits(100), 2);
    auto           account  = budget.make_account(limits(100));
    auto           occupied = ResourceBudget::try_reserve(account, BudgetClass::kReclaim, 100);
    if (!occupied) {
        FAIL() << "initial credit unavailable";
    }
    ResourceBudget::Request request(account, BudgetClass::kReclaim, 80, nullptr,
                                    ResourceBudget::Request::WaitPolicy::ReservationsOnly);
    Status                  status;
    bool                    done = false;
    auto operation               = stdexec::connect(request.sender(), TestCompletion{.status = &status, .done = &done});
    stdexec::start(operation);
    EXPECT_FALSE(done);
    const auto before = account->resource_usage()[static_cast<size_t>(BudgetClass::kReclaim)].denied_tree;
    auto       live   = occupied->allocate(25);
    ASSERT_TRUE(live);
    EXPECT_TRUE(done);
    EXPECT_EQ(status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(request.take_credit());
    EXPECT_EQ(budget.snapshot().waiters, 0U);
    EXPECT_EQ(account->resource_usage()[static_cast<size_t>(BudgetClass::kReclaim)].denied_tree, before + 1);
}

TEST(ResourceBudgetProgress, OtherTreeLiveAllocationsAlsoLimitSoftWorkHeadroom)
{
    ResourceBudget budget(limits(100), 2);
    auto           first    = budget.make_account(limits(100));
    auto           second   = budget.make_account(limits(100));
    auto           occupied = ResourceBudget::try_reserve(first, BudgetClass::kReclaim, 100);
    if (!occupied) {
        FAIL() << "initial credit unavailable";
    }
    auto live = occupied->allocate(60);
    ASSERT_TRUE(live);
    occupied.reset();
    ResourceBudget::Request request(second, BudgetClass::kReclaim, 50, nullptr,
                                    ResourceBudget::Request::WaitPolicy::ReservationsOnly);
    Status                  status;
    bool                    done = false;
    auto operation               = stdexec::connect(request.sender(), TestCompletion{.status = &status, .done = &done});
    stdexec::start(operation);
    EXPECT_TRUE(done);
    EXPECT_EQ(status.code(), Code::kResourceExhausted);
    EXPECT_EQ(budget.snapshot().waiters, 0U);
    EXPECT_EQ(second->resource_usage()[static_cast<size_t>(BudgetClass::kReclaim)].denied_host, 1U);
}
} // namespace
} // namespace crowdb::tree::detail
