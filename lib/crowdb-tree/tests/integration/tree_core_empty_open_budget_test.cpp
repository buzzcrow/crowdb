// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/resource_budget.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"
#include "runtime/tree_runtime_binding.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
TEST(TreeCoreEmptyOpenBudget, ConcurrentEmptyDomainsPayOnlyTheirRecoveryWindows)
{
    TreeRuntimeOptions options;
    options.limits.routing_bytes = 16 * 1024 * 1024;
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create(options, &runtime).ok());
    TreeBindingOptions binding_options;
    binding_options.limits.routing_bytes = 8 * 1024 * 1024;
    std::shared_ptr<TreeRuntimeBinding> other_tree;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, binding_options, &other_tree).ok());
    auto retained =
        detail::ResourceBudget::try_reserve(other_tree->account(), detail::BudgetClass::kRouting, 4 * 1024 * 1024);
    ASSERT_TRUE(retained);
    MemPageStore first_store(1);
    MemPageStore second_store(1);
    auto         open = [&](MemPageStore *store) {
        Config config;
        config.runtime         = runtime;
        config.runtime_binding = binding_options;
        config.page_store      = store;
        std::unique_ptr<Crowdbtree> tree;
        auto                        status = Crowdbtree::open(config, &tree);
        return std::pair{status, std::move(tree)};
    };
    auto first  = std::async(std::launch::async, open, &first_store);
    auto second = std::async(std::launch::async, open, &second_store);
    auto left   = first.get();
    auto right  = second.get();
    ASSERT_TRUE(left.first.ok()) << left.first.to_string();
    ASSERT_TRUE(right.first.ok()) << right.first.to_string();
    EXPECT_TRUE(left.second->put("left", "value").ok());
    EXPECT_TRUE(right.second->put("right", "value").ok());
    EXPECT_EQ(left.second->memtable_count(), 1U);
    EXPECT_EQ(right.second->memtable_count(), 1U);
    EXPECT_EQ(runtime->snapshot().bindings, 3U);
}
} // namespace
} // namespace crowdb::tree
