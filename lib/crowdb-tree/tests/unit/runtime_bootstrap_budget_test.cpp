// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/resource_budget.h"
#include "runtime/bootstrap_budget.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
TEST(RuntimeBootstrapBudget, ActualRootAndControlStorageConsumeTheConfiguredPartition)
{
    const auto capacity = size_t{65536};
    auto       budget   = BootstrapBudget::create(capacity);
    const auto initial  = budget->snapshot();
    EXPECT_GT(initial.allocated, sizeof(BootstrapBudget));
    EXPECT_EQ(initial.allocated + initial.reserved, capacity);
    EXPECT_EQ(initial.physical[static_cast<size_t>(AllocationKind::kMetadata)], initial.allocated);
    MetadataAllocator<uint64_t> allocator(budget);
    auto                       *value = allocator.allocate(17);
    EXPECT_EQ(budget->snapshot().allocated, initial.allocated + (17 * sizeof(uint64_t)));
    budget->seal();
    EXPECT_EQ(budget->available(), 0U);
    EXPECT_EQ(budget->snapshot().reserved, 0U);
    EXPECT_FALSE(budget->claim(1));
    allocator.deallocate(value, 17);
    EXPECT_EQ(budget->snapshot().allocated, initial.allocated);
    EXPECT_EQ(budget->snapshot().physical, initial.physical);
}

TEST(RuntimeBootstrapBudget, LedgerWaitersAndAccountControlsReleaseAtTheirActualOwners)
{
    auto                   bootstrap = BootstrapBudget::create(1024 * 1024);
    const auto             initial   = bootstrap->snapshot();
    ResourceBudget::Limits limits{};
    limits[1] = 65536;
    std::shared_ptr<ResourceBudget::Account> account;
    {
        ResourceBudget ledger(limits, 32, bootstrap);
        const auto     ledger_bytes = bootstrap->snapshot().allocated - initial.allocated;
        EXPECT_GT(ledger_bytes, 32 * ResourceBudget::kClasses * sizeof(std::atomic<ResourceBudget::Request *>));
        account = ledger.make_account(limits, bootstrap);
        EXPECT_GT(bootstrap->snapshot().allocated, initial.allocated + ledger_bytes);
    }
    EXPECT_GT(bootstrap->snapshot().allocated, initial.allocated);
    account.reset();
    EXPECT_EQ(bootstrap->snapshot().allocated, initial.allocated);
    EXPECT_EQ(bootstrap->snapshot().physical, initial.physical);
}

TEST(RuntimeBootstrapBudget, InsufficientControlAndWaiterCreditFailWithoutPartialOwnership)
{
    EXPECT_THROW((void)BootstrapBudget::create(sizeof(BootstrapBudget)), std::bad_alloc);
    auto                   bootstrap = BootstrapBudget::create(4096);
    const auto             initial   = bootstrap->snapshot();
    ResourceBudget::Limits limits{};
    EXPECT_THROW((void)ResourceBudget(limits, 1024, bootstrap), std::bad_alloc);
    EXPECT_EQ(bootstrap->snapshot().allocated, initial.allocated);
    EXPECT_EQ(bootstrap->snapshot().physical, initial.physical);
}
} // namespace
} // namespace crowdb::tree::detail
