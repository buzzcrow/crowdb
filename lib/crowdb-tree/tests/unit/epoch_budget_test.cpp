// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-tree/epoch.h"

#include <gtest/gtest.h>

#include <array>
#include <thread>

namespace crowdb::tree
{
namespace
{
constexpr size_t kBudget = static_cast<size_t>(detail::BudgetClass::kHandoff);

struct TestCredit
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result{};
        result[kBudget] = 65536;
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());

    [[nodiscard]] std::shared_ptr<detail::AllocationReservation> reserve(size_t bytes = 16384) const
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        return std::make_shared<detail::AllocationReservation>(std::move(*credit));
    }

    [[nodiscard]] size_t allocated() const
    {
        return budget.snapshot().allocated[kBudget];
    }
};

TEST(EpochBudget, DetachedGuardRetainsChargedLifetimeAndParticipantAfterManagerDestruction)
{
    TestCredit credit;
    auto       reservation    = credit.reserve();
    auto       manager        = std::make_unique<EpochManager>(reservation);
    const auto infrastructure = credit.allocated();
    EXPECT_GT(infrastructure, 0U);
    auto guard = manager->enter();
    EXPECT_GT(credit.allocated(), infrastructure);
    reservation->seal();
    manager.reset();
    EXPECT_GT(credit.allocated(), 0U);
    guard = {};
    EXPECT_EQ(credit.allocated(), 0U);
}

TEST(EpochBudget, CacheEvictionPreservesNestedParticipantWithoutAllocationAfterSeal)
{
    TestCredit credit;
    auto       reservation = credit.reserve();
    {
        EpochManager manager(reservation);
        auto         outer     = manager.enter();
        const auto   allocated = credit.allocated();
        reservation->seal();
        std::array<std::unique_ptr<EpochManager>, 64> others;
        for (auto &other : others) {
            other      = std::make_unique<EpochManager>();
            auto guard = other->enter();
        }
        auto nested = manager.enter();
        EXPECT_EQ(manager.active_guards(), 1U);
        EXPECT_EQ(credit.allocated(), allocated);
        outer = {};
        EXPECT_EQ(manager.active_guards(), 1U);
        nested = {};
        EXPECT_EQ(manager.active_guards(), 0U);
    }
    EXPECT_EQ(credit.allocated(), 0U);
}

TEST(EpochBudget, NewWorkerUsesItsCurrentCreditAndFailedRegistrationIsRetryable)
{
    TestCredit credit;
    auto       initial = credit.reserve();
    auto       work    = credit.reserve();
    {
        EpochManager manager(initial);
        initial->seal();
        const auto   before = credit.allocated();
        std::jthread worker([&] {
            EXPECT_THROW(manager.enter(), std::bad_alloc);
            EXPECT_EQ(credit.allocated(), before);
            auto guard = manager.enter(work);
            EXPECT_GT(credit.allocated(), before);
            work->seal();
            auto nested = manager.enter();
            EXPECT_EQ(manager.active_guards(), 1U);
        });
        worker.join();
        EXPECT_EQ(manager.active_guards(), 0U);
        EXPECT_GT(credit.allocated(), before);
    }
    EXPECT_EQ(credit.allocated(), 0U);
}

TEST(EpochBudget, ConstructionFailureReturnsAllPreviouslyClaimedInfrastructure)
{
    TestCredit credit;
    auto       tiny = credit.reserve(64);
    EXPECT_THROW(EpochManager manager(tiny), std::bad_alloc);
    EXPECT_EQ(credit.allocated(), 0U);
    EXPECT_EQ(tiny->available(), 64U);
}
} // namespace
} // namespace crowdb::tree
