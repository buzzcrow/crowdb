// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/resource_budget.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
ResourceBudget::Limits limits(size_t value)
{
    ResourceBudget::Limits result;
    result.fill(value);
    return result;
}

struct Result
{
    std::atomic<size_t> value{0};
    std::atomic<size_t> error{0};
    std::atomic<size_t> stopped{0};
};

struct Receiver
{
    using receiver_concept = stdexec::receiver_tag;
    Result *result;

    void set_value() const noexcept
    {
        result->value.fetch_add(1);
    }

    void set_error(Status /*unused*/) const noexcept
    {
        result->error.fetch_add(1);
    }

    void set_stopped() const noexcept
    {
        result->stopped.fetch_add(1);
    }
};

TEST(ResourceRegistration, PrepaidSlotSurvivesFullOrdinaryRegistrationAndCanBeReused)
{
    ResourceBudget budget(limits(1), 2);
    auto           account      = budget.make_account(limits(1));
    auto           registration = ResourceBudget::try_register(account, BudgetClass::kReclaim);
    if (!registration) {
        FAIL() << "budget registration unavailable";
    }
    auto &slot = *registration;
    for (size_t round = 0; round < 2; ++round) {
        auto occupied = ResourceBudget::try_reserve(account, BudgetClass::kReclaim, 1);
        ASSERT_TRUE(occupied);
        ResourceBudget::Request ordinary(account, BudgetClass::kReclaim, 1);
        ResourceBudget::Request excess(account, BudgetClass::kReclaim, 1);
        ResourceBudget::Request chosen(account, BudgetClass::kReclaim, 1, &slot);
        Result                  a;
        Result                  b;
        Result                  c;
        auto                    op_a = stdexec::connect(ordinary.sender(), Receiver{&a});
        auto                    op_b = stdexec::connect(excess.sender(), Receiver{&b});
        auto                    op_c = stdexec::connect(chosen.sender(), Receiver{&c});
        stdexec::start(op_a);
        stdexec::start(op_b);
        stdexec::start(op_c);
        EXPECT_EQ(b.error.load(), 1U);
        EXPECT_EQ(c.error.load(), 0U);
        EXPECT_EQ(budget.snapshot().waiters, 2U);
        ordinary.request_stop();
        occupied.reset();
        EXPECT_EQ(a.stopped.load(), 1U);
        EXPECT_EQ(c.value.load(), 1U);
        auto granted = chosen.take_credit();
        ASSERT_TRUE(granted);
        granted.reset();
    }
    registration.reset();
    EXPECT_TRUE(ResourceBudget::try_register(account, BudgetClass::kReclaim));
}

TEST(ResourceRegistration, CancelReturnsTheSameSlotWithoutAHostRegistrationRace)
{
    ResourceBudget budget(limits(1), 1);
    auto           account      = budget.make_account(limits(1));
    auto           registration = ResourceBudget::try_register(account, BudgetClass::kGapRepair);
    if (!registration) {
        FAIL() << "budget registration unavailable";
    }
    auto &slot     = *registration;
    auto  occupied = ResourceBudget::try_reserve(account, BudgetClass::kGapRepair, 1);
    ASSERT_TRUE(occupied);
    for (size_t round = 0; round < 100; ++round) {
        ResourceBudget::Request request(account, BudgetClass::kGapRepair, 1, &slot);
        Result                  result;
        auto                    operation = stdexec::connect(request.sender(), Receiver{&result});
        stdexec::start(operation);
        std::thread cancel([&request] { request.request_stop(); });
        cancel.join();
        EXPECT_EQ(result.stopped.load(), 1U);
        EXPECT_EQ(result.error.load(), 0U);
        EXPECT_EQ(budget.snapshot().waiters, 0U);
    }
    budget.close();
    EXPECT_FALSE(ResourceBudget::try_register(account, BudgetClass::kGapRepair));
    ResourceBudget::Request closed(account, BudgetClass::kGapRepair, 1, &slot);
    Result                  result;
    auto                    operation = stdexec::connect(closed.sender(), Receiver{&result});
    stdexec::start(operation);
    EXPECT_EQ(result.stopped.load(), 1U);
}

TEST(ResourceRegistration, StartupReservationFailureDoesNotConsumeAnotherCategory)
{
    ResourceBudget budget(limits(1), 1);
    auto           account      = budget.make_account(limits(1));
    auto           registration = ResourceBudget::try_register(account, BudgetClass::kReclaim);
    if (!registration) {
        FAIL() << "budget registration unavailable";
    }
    auto &slot = *registration;
    EXPECT_FALSE(ResourceBudget::try_register(account, BudgetClass::kReclaim));
    EXPECT_TRUE(ResourceBudget::try_register(account, BudgetClass::kIo));
    auto moved = std::move(slot);
    registration.reset();
    EXPECT_FALSE(ResourceBudget::try_register(account, BudgetClass::kReclaim));
    moved = {};
    EXPECT_TRUE(ResourceBudget::try_register(account, BudgetClass::kReclaim));
}
} // namespace
} // namespace crowdb::tree::detail
