// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/tree_work_metrics_c_api.h"

#include <gtest/gtest.h>

using namespace crowdb::tree;

TEST(TreeWorkMetricsCAbi, AgeSchemaValidatesWithoutMutationAndRetainsLedgerAfterClose)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto                       metrics   = TreeCoreAccess::get(*c_api_tree(tree))->publisher().work_metrics();
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    constexpr auto       index   = static_cast<size_t>(TreeStage::PageEncode);
    const auto           started = tree_metrics_now_ns();
    TreeMetricsOperation operation(metrics, TreeStage::PageEncode, started);
    ct_tree_work_ages_v1 ages{};
    ages.struct_size   = sizeof(ages);
    ages.version       = 2;
    ages.sampled_at_ns = 99;
    EXPECT_EQ(ct_work_metrics_ages(tree, &ages), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_work_metrics_collector_ages(collector, &ages), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ages.sampled_at_ns, 99U);
    ages.version = 1;
    --ages.struct_size;
    EXPECT_EQ(ct_work_metrics_ages(tree, &ages), static_cast<int32_t>(Code::kInvalidArgument));
    ++ages.struct_size;
    EXPECT_EQ(ct_work_metrics_ages(nullptr, &ages), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_work_metrics_ages(tree, nullptr), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_work_metrics_collector_ages(nullptr, &ages), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_work_metrics_collector_ages(collector, nullptr), static_cast<int32_t>(Code::kInvalidArgument));
    ASSERT_EQ(ct_work_metrics_ages(tree, &ages), 0);
    EXPECT_EQ(ages.stages[index].inflight, 1U);
    EXPECT_EQ(ages.stages[index].tracked, 1U);
    EXPECT_EQ(ages.stages[index].untracked, 0U);
    EXPECT_EQ(ages.stages[index].complete, 1U);
    EXPECT_EQ(ages.stages[index].observed_oldest_age_ns, ages.sampled_at_ns - started);
    ct_close(tree);
    ASSERT_EQ(ct_work_metrics_collector_ages(collector, &ages), 0);
    EXPECT_EQ(ages.stages[index].inflight, 1U);
    EXPECT_EQ(ages.stages[index].observed_oldest_age_ns, ages.sampled_at_ns - started);
    ASSERT_TRUE(operation.finish(TreeOutcome::Success, started + 30));
    ASSERT_EQ(ct_work_metrics_collector_ages(collector, &ages), 0);
    EXPECT_EQ(ages.stages[index].inflight, 0U);
    EXPECT_EQ(ages.stages[index].tracked, 0U);
    EXPECT_EQ(ages.stages[index].complete, 1U);
    ct_tree_work_snapshot_v1 window{};
    window.struct_size = sizeof(window);
    window.version     = 1;
    ASSERT_EQ(ct_work_metrics_capture(collector, &window), 0);
    EXPECT_EQ(window.stages[index].window_count, 1U);
    EXPECT_EQ(window.stages[index].window_sum_ns, 30U);
    ct_work_metrics_collector_free(collector);
    ct_runtime_free(runtime);
}
