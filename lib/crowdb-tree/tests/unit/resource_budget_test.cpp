// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/resource_budget.h"

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{
constexpr size_t kHandoff = static_cast<size_t>(BudgetClass::kHandoff);

ResourceBudget::Limits limits(size_t amount)
{
    ResourceBudget::Limits result;
    result.fill(amount);
    return result;
}

struct BudgetResult
{
    std::atomic<size_t> values{0};
    std::atomic<size_t> errors{0};
    std::atomic<size_t> stopped{0};
};

struct BudgetReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    BudgetResult *result;

    void set_value() const noexcept
    {
        result->values.fetch_add(1);
    }

    void set_error(Status /*unused*/) const noexcept
    {
        result->errors.fetch_add(1);
    }

    void set_stopped() const noexcept
    {
        result->stopped.fetch_add(1);
    }
};

struct ReentrantBudgetReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    BudgetReceiver                             receiver;
    ResourceBudget::Request                   *first;
    CallbackSender::Operation<BudgetReceiver> *next;

    void set_value() const noexcept
    {
        receiver.set_value();
        first->take_credit().reset();
        stdexec::start(*next);
    }

    void set_error(Status status) const noexcept
    {
        receiver.set_error(std::move(status));
    }

    void set_stopped() const noexcept
    {
        receiver.set_stopped();
    }
};

TEST(ResourceBudget, ReservationConvertsToAllocationWithoutDoubleCharging)
{
    ResourceBudget budget(limits(100), 4);
    auto           account     = budget.make_account(limits(100));
    auto           reservation = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 100);
    if (!reservation.has_value()) {
        FAIL() << "reservation rejected";
    }
    auto allocation = reservation.value().allocate(60);
    if (!allocation.has_value()) {
        FAIL() << "allocation transfer rejected";
    }
    EXPECT_EQ(budget.snapshot().reserved[kHandoff], 40U);
    EXPECT_EQ(budget.snapshot().allocated[kHandoff], 60U);
    EXPECT_FALSE(ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 1));
    reservation.reset();
    auto moved = std::move(allocation.value());
    allocation.reset();
    EXPECT_EQ(budget.snapshot().allocated[kHandoff], 60U);
    auto split = moved.split(20);
    ASSERT_TRUE(split);
    moved.release();
    EXPECT_EQ(budget.snapshot().allocated[kHandoff], 20U);
    split.reset();
    EXPECT_EQ(budget.snapshot().allocated[kHandoff], 0U);
}

TEST(ResourceBudget, HostTreeAndProgressPartitionsAreIndependentlyBounded)
{
    ResourceBudget budget(limits(100), 4);
    auto           a        = budget.make_account(limits(60));
    auto           b        = budget.make_account(limits(60));
    auto           a_credit = ResourceBudget::try_reserve(a, BudgetClass::kHandoff, 60);
    ASSERT_TRUE(a_credit);
    EXPECT_FALSE(ResourceBudget::try_reserve(a, BudgetClass::kHandoff, 1));
    EXPECT_FALSE(ResourceBudget::try_reserve(b, BudgetClass::kHandoff, 41));
    auto b_credit = ResourceBudget::try_reserve(b, BudgetClass::kHandoff, 40);
    ASSERT_TRUE(b_credit);
    EXPECT_TRUE(ResourceBudget::try_reserve(a, BudgetClass::kReclaim, 60));
    EXPECT_TRUE(ResourceBudget::try_reserve(a, BudgetClass::kGapRepair, 60));
    EXPECT_EQ(budget.snapshot().reserved[kHandoff], 100U);
}

TEST(ResourceBudget, AvailableCapacityIncludesHostUsageAndCreditConversion)
{
    ResourceBudget budget(limits(100), 2);
    auto           a      = budget.make_account(limits(80));
    auto           b      = budget.make_account(limits(80));
    auto           first  = ResourceBudget::try_reserve(a, BudgetClass::kHandoff, 60);
    auto           second = ResourceBudget::try_reserve(b, BudgetClass::kHandoff, 30);
    if (!first.has_value()) {
        FAIL() << "initial reservation rejected";
    }
    ASSERT_TRUE(second);
    EXPECT_EQ(ResourceBudget::available(a, BudgetClass::kHandoff), 10U);
    auto allocated = first.value().allocate(60);
    ASSERT_TRUE(allocated);
    EXPECT_EQ(ResourceBudget::available(a, BudgetClass::kHandoff), 10U);
    second.reset();
    EXPECT_EQ(ResourceBudget::available(a, BudgetClass::kHandoff), 20U);
    EXPECT_EQ(ResourceBudget::available(b, BudgetClass::kHandoff), 40U);
    budget.close();
    EXPECT_EQ(ResourceBudget::available(a, BudgetClass::kHandoff), 0U);
}

TEST(ResourceBudget, ReleasingAnotherTreeWakesRegisteredWaiter)
{
    ResourceBudget          budget(limits(100), 2);
    auto                    a        = budget.make_account(limits(100));
    auto                    b        = budget.make_account(limits(100));
    auto                    occupied = ResourceBudget::try_reserve(a, BudgetClass::kHandoff, 100);
    ResourceBudget::Request request(b, BudgetClass::kHandoff, 100);
    BudgetResult            result;
    auto                    operation = stdexec::connect(request.sender(), BudgetReceiver{&result});
    stdexec::start(operation);
    EXPECT_EQ(result.values.load(), 0U);
    EXPECT_EQ(budget.snapshot().waiters, 1U);
    occupied.reset();
    EXPECT_EQ(result.values.load(), 1U);
    EXPECT_EQ(budget.snapshot().waiters, 0U);
    auto credit = request.take_credit();
    if (!credit.has_value()) {
        FAIL() << "completed waiter did not receive credit";
    }
    EXPECT_FALSE(request.take_credit());
    EXPECT_EQ(credit.value().amount(), 100U);
    credit.reset();
    EXPECT_EQ(budget.snapshot().reserved[kHandoff], 0U);
}

TEST(ResourceBudget, CancellationAndCloseDrainWaitersButKeepOwnedAllocation)
{
    ResourceBudget budget(limits(10), 2);
    auto           account  = budget.make_account(limits(10));
    auto           occupied = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 10);
    if (!occupied.has_value()) {
        FAIL() << "initial reservation rejected";
    }
    auto                    allocation = occupied.value().allocate(10);
    ResourceBudget::Request first(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request second(account, BudgetClass::kHandoff, 1);
    BudgetResult            a;
    BudgetResult            b;
    auto                    op_a = stdexec::connect(first.sender(), BudgetReceiver{&a});
    auto                    op_b = stdexec::connect(second.sender(), BudgetReceiver{&b});
    stdexec::start(op_a);
    stdexec::start(op_b);
    first.request_stop();
    first.request_stop();
    EXPECT_EQ(a.stopped.load(), 1U);
    EXPECT_EQ(b.stopped.load(), 0U);
    budget.close();
    EXPECT_EQ(b.stopped.load(), 1U);
    EXPECT_EQ(budget.snapshot().allocated[kHandoff], 10U);
    EXPECT_FALSE(ResourceBudget::try_reserve(account, BudgetClass::kReclaim, 1));
    allocation.reset();
    EXPECT_EQ(budget.snapshot().allocated[kHandoff], 0U);
}

TEST(ResourceBudget, ImpossibleAndExcessWaitersFailInsteadOfWaitingForever)
{
    ResourceBudget          budget(limits(10), 1);
    auto                    account  = budget.make_account(limits(10));
    auto                    occupied = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 10);
    ResourceBudget::Request waiting(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request overflow(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request impossible(account, BudgetClass::kHandoff, std::numeric_limits<size_t>::max());
    BudgetResult            a;
    BudgetResult            b;
    BudgetResult            c;
    auto                    op_a = stdexec::connect(waiting.sender(), BudgetReceiver{&a});
    auto                    op_b = stdexec::connect(overflow.sender(), BudgetReceiver{&b});
    auto                    op_c = stdexec::connect(impossible.sender(), BudgetReceiver{&c});
    stdexec::start(op_a);
    stdexec::start(op_b);
    stdexec::start(op_c);
    EXPECT_EQ(b.errors.load(), 1U);
    EXPECT_EQ(c.errors.load(), 1U);
    waiting.request_stop();
    EXPECT_EQ(a.stopped.load(), 1U);
}

TEST(ResourceBudget, ConcurrentRegistrationAndReleaseCannotLoseWakeup)
{
    ResourceBudget budget(limits(1), 1);
    auto           account = budget.make_account(limits(1));
    for (size_t attempt = 0; attempt < 100; ++attempt) {
        auto occupied = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 1);
        ASSERT_TRUE(occupied);
        ResourceBudget::Request request(account, BudgetClass::kHandoff, 1);
        BudgetResult            result;
        auto                    operation = stdexec::connect(request.sender(), BudgetReceiver{&result});
        std::thread             register_waiter([&] { stdexec::start(operation); });
        occupied.reset();
        register_waiter.join();
        EXPECT_EQ(result.values.load(), 1U);
        request.request_stop();
    }
    EXPECT_EQ(budget.snapshot().reserved[kHandoff], 0U);
    EXPECT_EQ(budget.snapshot().waiters, 0U);
}

TEST(ResourceBudget, CompletionCanRegisterAnotherWaiterAfterTheQueueBecomesEmpty)
{
    ResourceBudget          budget(limits(1), 1);
    auto                    account  = budget.make_account(limits(1));
    auto                    occupied = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request first(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request second(account, BudgetClass::kHandoff, 1);
    BudgetResult            result;
    auto                    next = stdexec::connect(second.sender(), BudgetReceiver{&result});

    auto operation = stdexec::connect(first.sender(),
                                      ReentrantBudgetReceiver{.receiver = {&result}, .first = &first, .next = &next});
    stdexec::start(operation);
    EXPECT_EQ(result.values.load(), 0U);
    occupied.reset();
    EXPECT_EQ(result.values.load(), 2U);
    EXPECT_EQ(result.errors.load(), 0U);
    EXPECT_EQ(result.stopped.load(), 0U);
    EXPECT_EQ(budget.snapshot().waiters, 0U);
    EXPECT_TRUE(second.take_credit());
}

TEST(ResourceBudget, HandoffWaitersCannotConsumeReclaimRegistrationSlots)
{
    ResourceBudget          budget(limits(1), 1);
    auto                    account = budget.make_account(limits(1));
    auto                    handoff = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 1);
    auto                    reclaim = ResourceBudget::try_reserve(account, BudgetClass::kReclaim, 1);
    ResourceBudget::Request handoff_request(account, BudgetClass::kHandoff, 1);
    ResourceBudget::Request reclaim_request(account, BudgetClass::kReclaim, 1);
    BudgetResult            a;
    BudgetResult            b;
    auto                    op_a = stdexec::connect(handoff_request.sender(), BudgetReceiver{&a});
    auto                    op_b = stdexec::connect(reclaim_request.sender(), BudgetReceiver{&b});
    stdexec::start(op_a);
    stdexec::start(op_b);
    EXPECT_EQ(b.errors.load(), 0U);
    reclaim.reset();
    EXPECT_EQ(b.values.load(), 1U);
    EXPECT_EQ(a.values.load(), 0U);
    handoff_request.request_stop();
}

TEST(ResourceBudget, AllocationOwnerCanOutliveClosedBudget)
{
    std::optional<ResourceBudget::Credit> allocation;
    {
        ResourceBudget budget(limits(100), 1);
        auto           account     = budget.make_account(limits(100));
        auto           reservation = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, 100);
        if (!reservation.has_value()) {
            FAIL() << "initial reservation rejected";
        }
        allocation = reservation.value().allocate(100);
    }
    if (!allocation.has_value()) {
        FAIL() << "allocation transfer rejected";
    }
    EXPECT_EQ(allocation.value().amount(), 100U);
    allocation.reset();
}

} // namespace
} // namespace crowdb::tree::detail
