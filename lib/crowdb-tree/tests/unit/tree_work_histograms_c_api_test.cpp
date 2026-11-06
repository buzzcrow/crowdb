// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_work_histograms.h"
#include "c_api_internal.h"
#include "crowdb-tree/tree_work_histograms_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestWorkCapture
{
    ct_tree_work_snapshot_v1   work{};
    ct_tree_work_ages_v1       ages{};
    ct_tree_work_counters_v1   counters{};
    ct_tree_work_histograms_v1 histograms{};

    TestWorkCapture()
    {
        work.struct_size       = sizeof(work);
        work.version           = 1;
        ages.struct_size       = sizeof(ages);
        ages.version           = 1;
        counters.struct_size   = sizeof(counters);
        counters.version       = 1;
        histograms.struct_size = sizeof(histograms);
        histograms.version     = 1;
    }

    int32_t capture(ct_work_metrics_collector *collector)
    {
        return ct_work_metrics_capture_v3(collector, &work, &ages, &counters, &histograms);
    }
};

TEST(TreeWorkHistogramsCAbi, InvalidCaptureDoesNotDrainAndOneImmutableWindowSurvivesTreeClose)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto                       metrics   = TreeCoreAccess::get(*c_api_tree(tree))->publisher().work_metrics();
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    TestWorkCapture sample;
    ASSERT_EQ(sample.capture(collector), 0);
    EXPECT_EQ(sample.histograms.enabled_stages, 0U);
    constexpr auto stage = static_cast<uint32_t>(TreeStage::FlushBatch);
    EXPECT_EQ(ct_work_histogram_enable_v1(nullptr, stage), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(ct_work_histogram_enable_v1(tree, CT_TREE_WORK_STAGES_V1), static_cast<int32_t>(Code::kInvalidArgument));
    ASSERT_EQ(ct_work_histogram_enable_v1(tree, stage), 0);
    ASSERT_EQ(ct_work_histogram_enable_v1(tree, stage), 0);
    TreeMetricsOperation operation(metrics, TreeStage::FlushBatch, 10);
    ASSERT_TRUE(operation.finish(TreeOutcome::Error, 110));
    sample.histograms.version   = 2;
    sample.work.window_sequence = 77;
    EXPECT_EQ(sample.capture(collector), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(sample.work.window_sequence, 77U);
    sample.histograms.version = 1;
    ASSERT_EQ(ct_work_histograms_totals_v1(tree, &sample.histograms), 0);
    EXPECT_EQ(sample.histograms.stages[stage].count, 1U);
    ASSERT_EQ(sample.capture(collector), 0);
    EXPECT_EQ(sample.work.window_sequence, 2U);
    EXPECT_EQ(sample.histograms.window_sequence, sample.work.window_sequence);
    EXPECT_EQ(sample.histograms.enabled_stages, uint64_t{1} << stage);
    EXPECT_EQ(sample.histograms.stages[stage].count, 1U);
    EXPECT_EQ(sample.histograms.stages[stage].p95_ns, 100U);
    EXPECT_EQ(sample.work.stages[stage].window_count, 1U);
    const auto fanout = sample.histograms;
    ASSERT_EQ(ct_work_histograms_totals_v1(tree, &sample.histograms), 0);
    EXPECT_EQ(sample.histograms.stages[stage].count, 1U);
    ct_close(tree);
    ct_runtime_free(runtime);
    metrics.reset();
    ASSERT_EQ(sample.capture(collector), 0);
    EXPECT_EQ(sample.histograms.stages[stage].count, 0U);
    EXPECT_EQ(sample.histograms.stages[stage].total_count, 1U);
    EXPECT_EQ(fanout.stages[stage].count, 1U);
    EXPECT_EQ(fanout.stages[stage].p95_ns, 100U);
    ct_work_metrics_collector_free(collector);
}

TEST(TreeWorkHistogramsCAbi, OlderCaptureLayoutsAdvanceTheSameHistogramWindow)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    constexpr auto stage = static_cast<uint32_t>(TreeStage::FlushBatch);
    ASSERT_EQ(ct_work_histogram_enable_v1(tree, stage), 0);
    auto                       metrics   = TreeCoreAccess::get(*c_api_tree(tree))->publisher().work_metrics();
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    TestWorkCapture sample;
    for (int version : {1, 2}) {
        TreeMetricsOperation previous(metrics, TreeStage::FlushBatch, 10);
        ASSERT_TRUE(previous.finish(TreeOutcome::Success, 110));
        const auto status = version == 1
                              ? ct_work_metrics_capture(collector, &sample.work)
                              : ct_work_metrics_capture_v2(collector, &sample.work, &sample.ages, &sample.counters);
        ASSERT_EQ(status, 0);
        TreeMetricsOperation current(metrics, TreeStage::FlushBatch, 20);
        ASSERT_TRUE(current.finish(TreeOutcome::Success, 50));
        ASSERT_EQ(sample.capture(collector), 0);
        EXPECT_EQ(sample.work.window_sequence, 2U * version);
        EXPECT_EQ(sample.histograms.window_sequence, sample.work.window_sequence);
        EXPECT_EQ(sample.work.stages[stage].window_count, 1U);
        EXPECT_EQ(sample.histograms.stages[stage].count, 1U);
        EXPECT_EQ(sample.histograms.stages[stage].sum_low, 30U);
        EXPECT_EQ(sample.histograms.stages[stage].total_count, 2U * version);
    }
    ct_work_metrics_collector_free(collector);
    ct_close(tree);
    ct_runtime_free(runtime);
}

TEST(TreeWorkHistogramsCAbi, EnableBudgetFailurePreservesThePublishedViewAndSubsequentBusinessProgress)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto      *core    = TreeCoreAccess::get(*c_api_tree(tree));
    const auto before  = core->capture();
    auto       account = core->binding()->account();
    const auto stats   = account->snapshot();
    const auto index   = static_cast<size_t>(detail::BudgetClass::kRouting);
    const auto available =
        core->binding()->options().limits.routing_bytes - stats.allocated[index] - stats.reserved[index];
    auto held = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, available);
    ASSERT_TRUE(held);
    EXPECT_EQ(ct_work_histogram_enable_v1(tree, 0), static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(core->capture(), before);
    EXPECT_EQ(core->publisher().work_metrics()->histogram_totals().enabled_stages, 0U);
    held.reset();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(core->flush().ok());
    EXPECT_EQ(core->capture()->coverage, 1U);
    ASSERT_EQ(ct_work_histogram_enable_v1(tree, 0), 0);
    ct_close(tree);
    ct_runtime_free(runtime);
}
} // namespace
} // namespace crowdb::tree
