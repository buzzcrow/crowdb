// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/backing_stats_c_api.h"
#include "crowdb-tree/tree_work_metrics_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(BackingStatsCAbi, ClosedLocalTreeCollectorSamplesWithoutRetainingPagesOrBackend)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_page_store *store = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(1, &store), 0);
    ct_options options{};
    options.page_store = store;
    ct_tree *tree      = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto *facade = c_api_tree(tree);
    ASSERT_TRUE(facade->put("key", "value").ok());
    ASSERT_TRUE(facade->snapshot().ok());
    auto cpu = TreeCoreAccess::get(*facade)->binding()->cpu();
    cpu->drain();
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    ct_backing_stats_v1 value{};
    value.struct_size     = sizeof(value);
    value.version         = 2;
    value.allocated_bytes = 99;
    EXPECT_EQ(ct_get_backing_stats(tree, &value), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(value.allocated_bytes, 99U);
    value.version = 1;
    --value.struct_size;
    EXPECT_EQ(ct_work_metrics_collector_backing_stats(collector, &value), static_cast<int32_t>(Code::kInvalidArgument));
    ++value.struct_size;
    EXPECT_EQ(ct_get_backing_stats(nullptr, &value), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_get_backing_stats(tree, nullptr), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_work_metrics_collector_backing_stats(nullptr, &value), static_cast<int32_t>(Code::kInvalidArgument));
    ASSERT_EQ(ct_get_backing_stats(tree, &value), 0);
    ASSERT_NE(value.scope_id, 0U);
    ASSERT_GT(value.allocated_bytes, 0U);
    const auto scope  = value.scope_id;
    auto       old    = facade->snapshot_view();
    auto      *device = c_api_page_store(tree);
    ct_close(tree);
    cpu->drain();
    ASSERT_EQ(ct_work_metrics_collector_backing_stats(collector, &value), 0);
    EXPECT_EQ(value.scope_id, scope);
    EXPECT_GT(value.allocated_bytes, 0U);
    EXPECT_FALSE(device->try_claim_runtime_domain());
    old.reset();
    cpu->drain();
    EXPECT_TRUE(device->try_claim_runtime_domain());
    device->release_runtime_domain();
    ct_page_store_free(store);
    cpu->drain();
    cpu.reset();
    ct_runtime_free(runtime);
    ASSERT_EQ(ct_work_metrics_collector_backing_stats(collector, &value), 0);
    EXPECT_EQ(value.scope_id, scope);
    EXPECT_EQ(value.allocated_bytes, 0U);
    ct_work_metrics_collector_free(collector);
}

TEST(BackingStatsCAbi, VolatileTreeHasNoBackendScope)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    ct_backing_stats_v1 value{};
    value.struct_size = sizeof(value);
    value.version     = 1;
    EXPECT_EQ(ct_get_backing_stats(tree, &value), static_cast<int32_t>(Code::kUnavailable));
    ct_close(tree);
    ct_runtime_free(runtime);
}
} // namespace crowdb::tree
