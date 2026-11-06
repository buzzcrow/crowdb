// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/allocation_account.h"

#include <gtest/gtest.h>

#include <type_traits>

namespace crowdb::tree::detail
{
struct AllocationAccountTestAccess
{
    using Step = AllocationAccount::Step;

    template <class F> static void release_for_tests(AllocationAccount &budget, size_t bytes, F &&callback)
    {
        budget.release(
            bytes,
            [](void *context, Step step, size_t kind) noexcept {
                (*static_cast<std::remove_reference_t<F> *>(context))(step, kind);
            },
            &callback);
    }

    template <class F>
    static Status rebind_for_tests(AllocationAccount &budget, std::shared_ptr<ResourceBudget::Account> account,
                                   F &&callback)
    {
        return budget.rebind(
            std::move(account),
            [](void *context, Step step, size_t kind) noexcept {
                (*static_cast<std::remove_reference_t<F> *>(context))(step, kind);
            },
            &callback);
    }
};

namespace test_allocation_account
{
constexpr size_t kRouting  = static_cast<size_t>(BudgetClass::kRouting);
constexpr size_t kMetadata = static_cast<size_t>(AllocationKind::kMetadata);
constexpr size_t kFrame    = static_cast<size_t>(AllocationKind::kFrame);
using Step                 = AllocationAccountTestAccess::Step;

inline ResourceBudget::Limits limits(size_t amount)
{
    ResourceBudget::Limits result{};
    result[kRouting] = amount;
    return result;
}

inline AllocationLease use_remaining_memory(const std::shared_ptr<ResourceBudget::Account> &account,
                                            const std::shared_ptr<AllocationBudget>        &budget)
{
    const auto usage     = account->resource_usage();
    size_t     maximum   = 0;
    size_t     allocated = 0;
    for (size_t index = 0; index < static_cast<size_t>(BudgetClass::kIo); ++index) {
        maximum += usage[index].limit;
        allocated += usage[index].allocated;
    }
    // Simulate actual allocator charges without allocating the production limit.
    return {budget, maximum + (maximum / 2) - allocated};
}

struct TestAccounts
{
    ResourceBudget                           host{limits(16384), 1};
    std::shared_ptr<ResourceBudget::Account> old  = host.make_account(limits(8192));
    std::shared_ptr<ResourceBudget::Account> next = host.make_account(limits(8192));
    std::shared_ptr<AllocationAccount>       budget;
    size_t                                   base = 0;

    TestAccounts()
    {
        const auto status = AllocationAccount::create(old, BudgetClass::kRouting, &budget);
        EXPECT_TRUE(status.ok()) << status.to_string();
        base = old->snapshot().allocated[kRouting];
        EXPECT_GE(base, sizeof(AllocationAccount));
        EXPECT_EQ(old->snapshot().physical[kMetadata], base);
    }

    void expect_only_control(const std::shared_ptr<ResourceBudget::Account> &current) const
    {
        EXPECT_EQ(host.snapshot().allocated[kRouting], base);
        EXPECT_EQ(host.snapshot().reserved[kRouting], 0U);
        EXPECT_EQ(current->snapshot().allocated[kRouting], base);
        EXPECT_EQ(current->snapshot().physical[kMetadata], base);
        EXPECT_EQ(current->snapshot().physical[kFrame], 0U);
    }

    void finish()
    {
        budget.reset();
        EXPECT_EQ(host.snapshot().allocated[kRouting], 0U);
        EXPECT_EQ(host.snapshot().reserved[kRouting], 0U);
        for (const auto &account : {old, next}) {
            EXPECT_EQ(account->snapshot().allocated[kRouting], 0U);
            for (const auto bytes : account->snapshot().physical) {
                EXPECT_EQ(bytes, 0U);
            }
        }
    }
};
} // namespace test_allocation_account
} // namespace crowdb::tree::detail
