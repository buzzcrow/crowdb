// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "load_budget_test_support.h"
#include "runtime/tree_operation_lifetime.h"

#include <future>
#include <thread>

namespace crowdb::tree
{
namespace
{
using test_read::LoadCredits;

TEST(TreeOperationLifetime, CloseRejectsIngressAndWaitsForEveryAcceptedOwner)
{
    LoadCredits                                    credits;
    TreeOperationLifetime                          lifetime(2, credits.credits.reservation());
    std::shared_ptr<TreeOperationLifetime::Ticket> first;
    std::shared_ptr<TreeOperationLifetime::Ticket> second;
    std::shared_ptr<TreeOperationLifetime::Ticket> rejected;
    ASSERT_TRUE(lifetime.acquire(credits.credits.reservation(), &first).ok());
    ASSERT_TRUE(lifetime.acquire(credits.credits.reservation(), &second).ok());
    EXPECT_EQ(lifetime.acquire(credits.credits.reservation(), &rejected).code(), Code::kResourceExhausted);
    lifetime.close();
    EXPECT_EQ(lifetime.acquire(credits.credits.reservation(), &rejected).code(), Code::kUnavailable);
    std::atomic<bool>  drained{false};
    std::promise<void> started;
    auto               ready = started.get_future();
    std::jthread       closer([&] {
        started.set_value();
        lifetime.close_and_drain();
        drained.store(true, std::memory_order_release);
    });
    ready.wait();
    EXPECT_FALSE(drained.load(std::memory_order_acquire));
    first.reset();
    EXPECT_EQ(lifetime.outstanding(), 1U);
    EXPECT_FALSE(drained.load(std::memory_order_acquire));
    second.reset();
    closer.join();
    EXPECT_TRUE(drained.load(std::memory_order_acquire));
    EXPECT_EQ(lifetime.outstanding(), 0U);
}

TEST(TreeOperationLifetime, ObjectAndControlBlockAllocationFailuresReturnCapacityExactlyOnce)
{
    LoadCredits           credits;
    TreeOperationLifetime lifetime(1);
    // Exercise both object allocation and the later shared control allocation.
    // The latter destroys the newly constructed Ticket during rollback.
    const auto bound    = TreeOperationLifetime::ticket_allocation_bound();
    size_t     rejected = 0;
    size_t     admitted = 0;
    for (size_t available = 0; available <= bound; ++available) {
        AllocationLease pressure(credits.credits.reservation(), credits.credits.reservation()->available() - available);
        const auto      baseline = credits.budget.snapshot().allocated[LoadCredits::kBytes];
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        const auto status = lifetime.acquire(credits.credits.reservation(), &ticket);
        if (status.ok()) {
            ++admitted;
            EXPECT_EQ(lifetime.outstanding(), 1U);
            ticket.reset();
        }
        else {
            ++rejected;
            EXPECT_EQ(status.code(), Code::kResourceExhausted);
            EXPECT_FALSE(ticket);
        }
        EXPECT_EQ(lifetime.outstanding(), 0U);
        EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], baseline);
    }
    EXPECT_GT(rejected, 0U);
    EXPECT_GT(admitted, 0U);
    lifetime.close_and_drain();
}

TEST(TreeOperationLifetime, SoftAdmissionReportsOccupiedCapacityWithoutChangingTheHardContract)
{
    TreeOperationLifetime                          lifetime(1);
    std::shared_ptr<TreeOperationLifetime::Ticket> first;
    std::shared_ptr<TreeOperationLifetime::Ticket> next;
    ASSERT_TRUE(lifetime.acquire({}, &first).ok());
    EXPECT_EQ(lifetime.try_acquire({}, &next).code(), Code::kUnavailable);
    EXPECT_EQ(lifetime.acquire({}, &next).code(), Code::kResourceExhausted);
    const auto usage = lifetime.resource_usage();
    EXPECT_EQ(usage.limit, 1U);
    EXPECT_EQ(usage.used, 1U);
    EXPECT_EQ(usage.denied_tree, 2U);
    first.reset();
    ASSERT_TRUE(lifetime.try_acquire({}, &next).ok());
    EXPECT_EQ(lifetime.resource_usage().used, 1U);
    next.reset();
    EXPECT_EQ(lifetime.resource_usage().used, 0U);
    EXPECT_EQ(lifetime.resource_usage().denied_tree, 2U);
}

TEST(TreeOperationLifetime, TicketRetainsChargedStateAndHostAfterIngressOwnerIsDropped)
{
    LoadCredits                                    credits;
    auto                                           host = std::make_shared<int>(17);
    const std::weak_ptr<int>                       weak = host;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    {
        TreeOperationLifetime lifetime(1, credits.credits.reservation(), host);
        ASSERT_TRUE(lifetime.acquire(credits.credits.reservation(), &ticket).ok());
        host.reset();
        EXPECT_FALSE(weak.expired());
    }
    credits.seal();
    EXPECT_FALSE(weak.expired());
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    ticket.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace
} // namespace crowdb::tree
