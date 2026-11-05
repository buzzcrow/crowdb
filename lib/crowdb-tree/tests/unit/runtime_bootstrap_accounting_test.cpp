// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "runtime/tree_runtime_internal.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(RuntimeBootstrapAccounting, WaiterCapacityConsumesExactAdditionalHostStorage)
{
    TreeRuntimeOptions first_options;
    first_options.workers                = 1;
    first_options.backend_workers        = 1;
    first_options.max_waiters_per_class  = 8;
    auto second_options                  = first_options;
    second_options.max_waiters_per_class = 24;
    std::shared_ptr<TreeRuntime> first;
    std::shared_ptr<TreeRuntime> second;
    ASSERT_TRUE(TreeRuntime::create(first_options, &first).ok());
    ASSERT_TRUE(TreeRuntime::create(second_options, &second).ok());
    const auto     a = first->snapshot();
    const auto     b = second->snapshot();
    constexpr auto additional =
        16 * detail::ResourceBudget::kClasses * sizeof(std::atomic<detail::ResourceBudget::Request *>);
    EXPECT_EQ(b.allocated.routing_bytes - a.allocated.routing_bytes, additional);
    EXPECT_EQ(a.reserved.routing_bytes - b.reserved.routing_bytes, additional);
    EXPECT_EQ(a.allocated.routing_bytes + a.reserved.routing_bytes, first_options.lifecycle_bytes);
    EXPECT_EQ(b.allocated.routing_bytes + b.reserved.routing_bytes, second_options.lifecycle_bytes);
}
} // namespace
} // namespace crowdb::tree
