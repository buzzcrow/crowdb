// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/tree_work_metrics_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(TreeWorkMetricsCAbi, DispatchSchemaRejectsInvalidOutputsAndPreservesCumulativeCounters)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto &dispatch = TreeCoreAccess::get(*c_api_tree(tree))->publisher().work_metrics()->dispatch;
    dispatch.inline_probe();
    dispatch.inline_range();
    dispatch.queued_step();
    dispatch.run(5, 19);
    dispatch.installed(37);
    dispatch.begin();
    dispatch.begin();
    dispatch.finish();
    ct_tree_flush_dispatch_v1 result{};
    result.struct_size = sizeof(result);
    result.version     = 2;
    result.cow_bytes   = 99;
    EXPECT_EQ(ct_flush_dispatch_totals(tree, &result), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(result.cow_bytes, 99U);
    result.version = 1;
    --result.struct_size;
    EXPECT_EQ(ct_flush_dispatch_totals(tree, &result), static_cast<int32_t>(Code::kInvalidArgument));
    ++result.struct_size;
    EXPECT_EQ(ct_flush_dispatch_totals(nullptr, &result), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_flush_dispatch_totals(tree, nullptr), static_cast<int32_t>(Code::kInvalidArgument));
    ASSERT_EQ(ct_flush_dispatch_totals(tree, &result), 0);
    EXPECT_EQ(result.inline_probes, 1U);
    EXPECT_EQ(result.inline_ranges, 1U);
    EXPECT_EQ(result.queued_steps, 1U);
    EXPECT_EQ(result.prepared_runs, 1U);
    EXPECT_EQ(result.prepared_entries, 5U);
    EXPECT_EQ(result.prepared_bytes, 19U);
    EXPECT_EQ(result.cow_bytes, 37U);
    EXPECT_EQ(result.active_steps, 1U);
    EXPECT_EQ(result.peak_steps, 2U);
    dispatch.finish();
    ASSERT_EQ(ct_flush_dispatch_totals(tree, &result), 0);
    EXPECT_EQ(result.active_steps, 0U);
    EXPECT_EQ(result.peak_steps, 2U);
    EXPECT_EQ(result.prepared_entries, 5U);
    ct_close(tree);
    ct_runtime_free(runtime);
}

TEST(TreeWorkMetricsCAbi, SingleCollectorSharesCopiesAndTotalsNeverResetWindow)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto          *core    = TreeCoreAccess::get(*c_api_tree(tree));
    auto           metrics = core->publisher().work_metrics();
    constexpr auto index   = static_cast<size_t>(TreeStage::MemoryRetireWait);
    core->publisher().reclaim();
    core->binding()->drain();
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    ct_work_metrics_collector *other = nullptr;
    EXPECT_EQ(ct_work_metrics_collector_create(tree, &other), static_cast<int32_t>(Code::kUnavailable));
    EXPECT_EQ(other, nullptr);
    ct_tree_work_snapshot_v1 totals{};
    totals.struct_size = sizeof(totals);
    totals.version     = 1;
    ASSERT_EQ(ct_work_metrics_totals(tree, &totals), 0);
    const auto initial_count = totals.stages[index].total_count;
    EXPECT_EQ(totals.stages[index].window_count, 0U);
    ct_tree_work_snapshot_v1 window = totals;
    ASSERT_EQ(ct_work_metrics_capture(collector, &window), 0);
    EXPECT_EQ(window.stages[index].window_count, initial_count);
    TreeMetricsOperation operation(metrics, TreeStage::MemoryRetireWait, 100);
    ASSERT_TRUE(operation.finish(TreeOutcome::Success, 150));
    ASSERT_EQ(ct_work_metrics_capture(collector, &window), 0);
    const auto consumer = window;
    EXPECT_EQ(window.window_sequence, 2U);
    EXPECT_EQ(window.stages[index].window_count, 1U);
    EXPECT_EQ(window.stages[index].window_sum_ns, 50U);
    ASSERT_EQ(ct_work_metrics_totals(tree, &totals), 0);
    EXPECT_EQ(totals.stages[index].total_count, initial_count + 1);
    ct_close(tree);
    ct_runtime_free(runtime);
    // Closing the Tree also retires its real roots. The retained collector
    // must include those completions while preserving the earlier window.
    const auto final_count = metrics->totals()[index].count;
    EXPECT_GT(final_count, initial_count + 1);
    ASSERT_EQ(ct_work_metrics_capture(collector, &window), 0);
    EXPECT_EQ(window.window_sequence, 3U);
    EXPECT_EQ(window.stages[index].window_count, final_count - initial_count - 1);
    EXPECT_EQ(window.stages[index].total_count, final_count);
    EXPECT_EQ(consumer.stages[index].window_count, 1U);
    ASSERT_EQ(ct_work_metrics_capture(collector, &window), 0);
    EXPECT_EQ(window.window_sequence, 4U);
    EXPECT_EQ(window.stages[index].window_count, 0U);
    EXPECT_EQ(window.stages[index].total_count, final_count);
    ct_work_metrics_collector_free(collector);
}

TEST(TreeWorkMetricsCAbi, InvalidCaptureDoesNotDrainAndReleasedClaimCanBeReused)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    ct_tree_work_snapshot_v1 window{};
    window.struct_size = sizeof(window);
    window.version     = 2;
    EXPECT_EQ(ct_work_metrics_capture(collector, &window), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(window.version, 2U);
    window.version = 1;
    ASSERT_EQ(ct_work_metrics_capture(collector, &window), 0);
    EXPECT_EQ(window.window_sequence, 1U);
    ct_work_metrics_collector_free(collector);
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    ct_work_metrics_collector_free(collector);
    ct_close(tree);
    ct_runtime_free(runtime);
}
} // namespace crowdb::tree
