// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_metrics.h"

#include <gtest/gtest.h>

#include <limits>
#include <thread>
#include <vector>

using namespace crowdb::tree;

namespace
{
constexpr size_t kFlush = static_cast<size_t>(TreeStage::FlushBatch);
constexpr size_t kLoad  = static_cast<size_t>(TreeStage::PageLoadPipeline);
} // namespace

TEST(TreeWorkAge, TracksOldestAcrossOutOfOrderStartsAndSingleTerminalNotifications)
{
    auto                 metrics = std::make_shared<TreeWorkMetrics>();
    TreeMetricsOperation newer(metrics, TreeStage::FlushBatch, 40);
    TreeMetricsOperation oldest(metrics, TreeStage::FlushBatch, 10);
    TreeMetricsOperation load(metrics, TreeStage::PageLoadPipeline, 30);
    auto                 ages = metrics->ages(100);
    EXPECT_EQ(ages[kFlush].inflight, 2U);
    EXPECT_EQ(ages[kFlush].tracked, 2U);
    EXPECT_EQ(ages[kFlush].oldest_age_ns, 90U);
    EXPECT_TRUE(ages[kFlush].complete);
    EXPECT_EQ(ages[kLoad].oldest_age_ns, 70U);
    EXPECT_TRUE(oldest.finish(TreeOutcome::Success, 110));
    EXPECT_FALSE(oldest.finish(TreeOutcome::Error, 120));
    ages = metrics->ages(130);
    EXPECT_EQ(ages[kFlush].inflight, 1U);
    EXPECT_EQ(ages[kFlush].oldest_age_ns, 90U);
    EXPECT_TRUE(newer.finish(TreeOutcome::Cancelled, 140));
    EXPECT_TRUE(load.finish(TreeOutcome::Error, 150));
    for (const auto &age : metrics->ages(160)) {
        EXPECT_EQ(age.inflight, 0U);
        EXPECT_EQ(age.tracked, 0U);
        EXPECT_EQ(age.untracked, 0U);
        EXPECT_EQ(age.oldest_age_ns, 0U);
        EXPECT_TRUE(age.complete);
    }
}

TEST(TreeWorkAge, SaturationReportsIncompleteCoverageWithoutRejectingWork)
{
    auto                                               metrics = std::make_shared<TreeWorkMetrics>();
    std::vector<std::unique_ptr<TreeMetricsOperation>> operations;
    operations.reserve(kTreeWorkAgeCapacity + 2);
    for (size_t i = 0; i < kTreeWorkAgeCapacity + 2; ++i) {
        operations.push_back(std::make_unique<TreeMetricsOperation>(metrics, TreeStage::FlushBatch, i + 1));
    }
    auto age = metrics->ages(1000)[kFlush];
    EXPECT_EQ(age.inflight, kTreeWorkAgeCapacity + 2);
    EXPECT_EQ(age.tracked, kTreeWorkAgeCapacity);
    EXPECT_EQ(age.untracked, 2U);
    EXPECT_EQ(age.oldest_age_ns, 999U);
    EXPECT_FALSE(age.complete);
    for (size_t i = 0; i < kTreeWorkAgeCapacity; ++i) {
        ASSERT_TRUE(operations[i]->finish(TreeOutcome::Success, 1000));
    }
    age = metrics->ages(2000)[kFlush];
    EXPECT_EQ(age.inflight, 2U);
    EXPECT_EQ(age.tracked, 0U);
    EXPECT_EQ(age.untracked, 2U);
    EXPECT_EQ(age.oldest_age_ns, 0U);
    EXPECT_FALSE(age.complete);
    // Reused diagnostic slots cannot be released by an older terminal callback.
    TreeMetricsOperation reused(metrics, TreeStage::FlushBatch, 1500);
    for (size_t i = 0; i < kTreeWorkAgeCapacity; ++i) {
        ASSERT_FALSE(operations[i]->finish(TreeOutcome::Success, 2000));
    }
    EXPECT_EQ(metrics->ages(2000)[kFlush].oldest_age_ns, 500U);
    operations.clear();
    age = metrics->ages(2000)[kFlush];
    EXPECT_EQ(age.inflight, 1U);
    EXPECT_EQ(age.untracked, 0U);
    EXPECT_TRUE(age.complete);
    ASSERT_TRUE(reused.finish(TreeOutcome::Success, 2100));
    EXPECT_EQ(metrics->totals()[kFlush].count, kTreeWorkAgeCapacity + 3);
}

TEST(TreeWorkAge, HandlesUnencodableTimestampsAndClockRegression)
{
    auto                 metrics = std::make_shared<TreeWorkMetrics>();
    TreeMetricsOperation invalid(metrics, TreeStage::FlushBatch, std::numeric_limits<uint64_t>::max());
    TreeMetricsOperation future(metrics, TreeStage::PageLoadPipeline, 200);
    auto                 ages = metrics->ages(100);
    EXPECT_EQ(ages[kFlush].untracked, 1U);
    EXPECT_FALSE(ages[kFlush].complete);
    EXPECT_EQ(ages[kLoad].oldest_age_ns, 0U);
    EXPECT_TRUE(ages[kLoad].complete);
    EXPECT_TRUE(invalid.finish(TreeOutcome::Cancelled, 0));
    EXPECT_TRUE(future.finish(TreeOutcome::Cancelled, 100));
    EXPECT_TRUE(metrics->ages(100)[kFlush].complete);
    EXPECT_EQ(metrics->totals()[kFlush].sum_ns, 0U);
}

TEST(TreeWorkAge, ConcurrentSamplingDoesNotRetainOperationsAndQuiescentTotalsMatch)
{
    auto                     metrics     = std::make_shared<TreeWorkMetrics>();
    constexpr size_t         kThreads    = 4;
    constexpr size_t         kOperations = 2000;
    std::atomic<size_t>      running{kThreads};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (size_t i = 0; i < kThreads; ++i) {
        workers.emplace_back([&, i] {
            for (size_t j = 0; j < kOperations; ++j) {
                const auto           stage = static_cast<TreeStage>((i + j) % kTreeStageCount);
                TreeMetricsOperation operation(metrics, stage, j);
                EXPECT_TRUE(operation.finish(TreeOutcome::Success, j + 1));
            }
            running.fetch_sub(1);
        });
    }
    while (running.load() != 0) {
        const auto ages    = metrics->ages(kOperations);
        uint64_t   tracked = 0;
        for (const auto &age : ages) {
            tracked += age.tracked;
        }
        EXPECT_LE(tracked, kTreeWorkAgeCapacity);
    }
    for (auto &worker : workers) {
        worker.join();
    }
    uint64_t count = 0;
    for (const auto &stage : metrics->totals()) {
        count += stage.count;
    }
    EXPECT_EQ(count, kThreads * kOperations);
    for (const auto &age : metrics->ages(kOperations)) {
        EXPECT_EQ(age.inflight, 0U);
        EXPECT_EQ(age.tracked, 0U);
        EXPECT_EQ(age.untracked, 0U);
        EXPECT_TRUE(age.complete);
    }
}
