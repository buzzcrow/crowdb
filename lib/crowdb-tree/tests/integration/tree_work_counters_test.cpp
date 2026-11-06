// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/tree_work_metrics_c_api.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(TreeWorkCountersIntegration, RegistryUsesPaidStorageBeyondTheBootstrapReserve)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    core->binding()->drain();
    const auto &bootstrap = core->binding()->lifecycle_budget();
    const auto  bytes     = bootstrap->available();
    ASSERT_TRUE(bootstrap->claim(bytes));
    auto release = [bytes](detail::AllocationReservation *owner) { owner->release(bytes); };
    std::unique_ptr<detail::AllocationReservation, decltype(release)> remaining(bootstrap.get(), release);
    const auto                                                        available = bootstrap->available();
    const auto                                                       &account   = core->binding()->account();
    const auto kind   = static_cast<size_t>(AllocationKind::kMetadata);
    const auto before = account->snapshot().physical[kind];
    ASSERT_NO_THROW(tree.init_metrics("paid-registry", ""));
    EXPECT_GT(account->snapshot().physical[kind], before);
    EXPECT_GE(bootstrap->available(), available);
    remaining.reset();
    const auto output = tree.flush_metrics_str(1, "test");
    EXPECT_NE(output.find("paid-registry.work.flush.batch.attempts.c"), std::string::npos);
    EXPECT_NE(output.find("paid-registry.memory.metadata.bytes.g"), std::string::npos);
}

TEST(TreeWorkCountersIntegration, ActualFlushAndSplitExposeCommittedOutputsWithoutPayloadCopies)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.frame_bytes      = 4096;
    config.leaf_split_bytes = 3000;
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    const auto work = core->publisher().work_metrics();
    ASSERT_TRUE(tree.put("a", "old").ok());
    ASSERT_TRUE(tree.put("a", "new").ok());
    ASSERT_TRUE(tree.put("b", "two").ok());
    ASSERT_TRUE(tree.flush().ok());
    auto counters = work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Flush)];
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::FlushWinningVersions)], 2U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::FlushCandidateVersions)], 2U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::FlushWinningKeyBytes)], 2U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::FlushWinningValueBytes)], 6U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::HandoffValueCopyBytes)], 0U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::SourceDetached)], 1U);
    for (size_t i = 0; i < 100; ++i) {
        ASSERT_TRUE(tree.put("key-" + std::to_string(i), std::string(96, 'x')).ok());
    }
    const auto status = tree.flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    counters          = work->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Flush)];
    const auto splits = counters[static_cast<size_t>(TreeWorkCounter::SplitOperations)];
    EXPECT_GT(splits, 0U);
    EXPECT_GE(counters[static_cast<size_t>(TreeWorkCounter::SplitOutputPages)], splits * 2);
    EXPECT_EQ(
        work->totals()[static_cast<size_t>(TreeStage::PageSplit)].outcomes[static_cast<size_t>(TreeOutcome::Success)],
        splits);
}

TEST(TreeWorkCountersIntegration, AdditiveCaptureRejectsInvalidHeaderWithoutDrainAndSurvivesClose)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_tree   *tree = nullptr;
    ct_options options{};
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    auto                      *core      = TreeCoreAccess::get(*c_api_tree(tree));
    auto                       work      = core->publisher().work_metrics();
    ct_work_metrics_collector *collector = nullptr;
    ASSERT_EQ(ct_work_metrics_collector_create(tree, &collector), 0);
    ct_tree_work_snapshot_v1 window{};
    window.struct_size = sizeof(window);
    window.version     = 1;
    ct_tree_work_ages_v1 ages{};
    ages.struct_size = sizeof(ages);
    ages.version     = 1;
    ct_tree_work_counters_v1 counters{};
    counters.struct_size = sizeof(counters);
    counters.version     = 1;
    ASSERT_EQ(ct_work_metrics_capture_v2(collector, &window, &ages, &counters), 0);
    ASSERT_TRUE(c_api_tree(tree)->put("a", "value").ok());
    ASSERT_TRUE(c_api_tree(tree)->flush().ok());
    counters.version = 2;
    const auto seq   = window.window_sequence;
    EXPECT_EQ(ct_work_metrics_capture_v2(collector, &window, &ages, &counters),
              static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(window.window_sequence, seq);
    counters.version = 1;
    ASSERT_EQ(ct_work_counters_totals(tree, &counters), 0);
    const auto index = static_cast<size_t>(TreeWorkCounter::FlushWinningValueBytes);
    const auto cause = static_cast<size_t>(TreeWorkCause::Flush);
    EXPECT_EQ(counters.values[cause][index], 5U);
    ct_close(tree);
    ct_runtime_free(runtime);
    ASSERT_EQ(ct_work_metrics_capture_v2(collector, &window, &ages, &counters), 0);
    EXPECT_EQ(window.window_sequence, seq + 1);
    EXPECT_GT(window.stages[static_cast<size_t>(TreeStage::FlushBatch)].window_count, 0U);
    EXPECT_EQ(counters.values[cause][index], 5U);
    EXPECT_STREQ(ct_work_counter_name_v1(static_cast<uint32_t>(index)), "flush.winning.value.bytes");
    EXPECT_EQ(ct_work_counter_name_v1(CT_TREE_WORK_COUNTERS_V1), nullptr);
    EXPECT_EQ(ct_work_cause_name_v1(CT_TREE_WORK_CAUSES_V1), nullptr);
    ct_work_metrics_collector_free(collector);
}
} // namespace
} // namespace crowdb::tree
