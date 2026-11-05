// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/resource_budget.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/runtime_c_api.h"
#include "published_read_test_support.h"

namespace crowdb::tree
{
TEST(ResourceUsage, ClassifiesTreeAndHostPressureAndRetainsQueuedWaits)
{
    detail::ResourceBudget::Limits host{};
    host.fill(100);
    auto limits = host;
    limits[0]   = 80;
    detail::ResourceBudget budget(host, 4);
    auto                   first    = budget.make_account(limits);
    auto                   second   = budget.make_account(limits);
    auto                   occupied = detail::ResourceBudget::try_reserve(first, detail::BudgetClass::kHandoff, 70);
    if (!occupied) {
        FAIL() << "initial credit unavailable";
    }
    auto allocation = occupied->allocate(20);
    ASSERT_TRUE(allocation);
    EXPECT_FALSE(detail::ResourceBudget::try_reserve(first, detail::BudgetClass::kHandoff, 11));
    EXPECT_FALSE(detail::ResourceBudget::try_reserve(second, detail::BudgetClass::kHandoff, 40));
    auto observed = first->resource_usage()[0];
    EXPECT_EQ(observed.limit, 80U);
    EXPECT_EQ(observed.used, 70U);
    EXPECT_EQ(observed.reserved, 50U);
    EXPECT_EQ(observed.allocated, 20U);
    EXPECT_EQ(observed.denied_tree, 1U);
    EXPECT_EQ(second->resource_usage()[0].denied_host, 1U);
    EXPECT_EQ(budget.resource_usage()[0].used, 70U);
    detail::ResourceBudget::Request request(second, detail::BudgetClass::kHandoff, 40);
    test_read::Completion           done;
    auto                            operation = stdexec::connect(request.sender(), test_read::Receiver{.done = &done});
    stdexec::start(operation);
    EXPECT_EQ(second->resource_usage()[0].waiters, 1U);
    EXPECT_EQ(budget.resource_usage()[0].waits, 1U);
    occupied.reset();
    EXPECT_EQ(done.values.load(), 1U);
    EXPECT_EQ(second->resource_usage()[0].waiters, 0U);
    EXPECT_GT(second->resource_usage()[0].wait_ns, 0U);
    EXPECT_EQ(second->resource_usage()[1].waits, 0U);
}

TEST(ResourceUsage, VersionedHostAndTreeSnapshotsAreNonDraining)
{
    ct_runtime *raw_runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &raw_runtime), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(raw_runtime, &ct_runtime_free);
    ct_options                                              options{};
    ct_tree                                                *raw_tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime.get(), nullptr, &raw_tree), 0);
    std::unique_ptr<ct_tree, decltype(&ct_close)> tree(raw_tree, &ct_close);
    ct_resource_usage_snapshot_v1                 host{
        .struct_size = sizeof(host), .version = 1, .resource_mask = 0, .scope_id = 0, .resources = {}};
    auto local = host;
    ASSERT_EQ(ct_runtime_resource_usage(runtime.get(), &host), 0);
    ASSERT_EQ(ct_tree_resource_usage(tree.get(), &local), 0);
    EXPECT_EQ(host.resource_mask, 0x3ffU);
    EXPECT_EQ(local.resource_mask, 0xffcffU);
    EXPECT_EQ(local.resources[12].reserved, local.resources[12].used);
    EXPECT_NE(local.scope_id, 0U);
    EXPECT_GT(local.resources[1].used, 0U);
    EXPECT_GE(host.resources[1].used, local.resources[1].used);
    EXPECT_GT(local.resources[6].used, 0U);
    EXPECT_GT(host.resources[8].limit, 0U);
    EXPECT_EQ(local.resources[13].limit, local.resources[6].limit);
    EXPECT_EQ(local.resources[14].limit, local.resources[6].limit);
    EXPECT_EQ(local.resources[15].limit, local.resources[6].limit + 2);
    EXPECT_EQ(local.resources[16].limit, 4096U);
    EXPECT_EQ(local.resources[17].limit, 1024U * 1024U);
    EXPECT_EQ(local.resources[18].limit, 1024U);
    EXPECT_EQ(local.resources[19].limit, 1024U * 1024U);
    const auto saved = local;
    ASSERT_EQ(ct_tree_resource_usage(tree.get(), &local), 0);
    EXPECT_EQ(local.resources[0].denied_host, saved.resources[0].denied_host);
    local.version = 2;
    EXPECT_NE(ct_tree_resource_usage(tree.get(), &local), 0);
    EXPECT_EQ(local.version, 2U);
    EXPECT_EQ(local.scope_id, saved.scope_id);
}
} // namespace crowdb::tree
