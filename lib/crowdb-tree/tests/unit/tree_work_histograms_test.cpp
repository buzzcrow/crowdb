// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "btree/tree_work_histograms.h"
#include "crowdb-common/metrics/latency_histogram.h"

#include <gtest/gtest.h>

#include <barrier>
#include <thread>

namespace crowdb::tree
{
namespace
{
constexpr size_t                         kFlush  = static_cast<size_t>(TreeStage::FlushBatch);
constexpr size_t                         kEncode = static_cast<size_t>(TreeStage::PageEncode);
constexpr detail::ResourceBudget::Limits kLimits{4 << 20, 4 << 20, 4 << 20, 4 << 20, 4 << 20, 1};

class TreeWorkHistogramsTest : public testing::Test
{
  protected:
    detail::ResourceBudget                           host{kLimits, 8};
    std::shared_ptr<detail::ResourceBudget::Account> account = host.make_account(kLimits);

    std::shared_ptr<detail::AllocationReservation> reserve(size_t bytes = 2 << 20)
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, bytes);
        if (!credit) {
            ADD_FAILURE() << "fixture credit must fit";
            return {};
        }
        return detail::AllocationReservation::create(std::move(*credit));
    }

    [[nodiscard]] size_t metadata() const
    {
        return account->snapshot().physical[static_cast<size_t>(AllocationKind::kMetadata)];
    }
};

TEST_F(TreeWorkHistogramsTest, DefaultSummaryDoesNotAllocateBucketsAndEnableFailureLeavesItUsable)
{
    auto metrics = std::make_shared<TreeWorkMetrics>();
    auto credit  = reserve();
    ASSERT_TRUE(credit);
    const auto bytes = TreeWorkMetrics::histogram_allocation_bytes();
    ASSERT_GE(credit->available(), bytes);
    auto unused = credit->take_reserved(credit->available() - bytes + 1);
    ASSERT_TRUE(unused);
    unused.reset();
    const auto initial = metadata();
    EXPECT_EQ(metrics->histogram_totals().enabled_stages, 0U);
    EXPECT_EQ(metrics->enable_histogram(TreeStage::FlushBatch, credit).code(), Code::kResourceExhausted);
    EXPECT_EQ(metrics->enable_histogram(TreeStage::Count, credit).code(), Code::kInvalidArgument);
    EXPECT_EQ(metadata(), initial);
    EXPECT_EQ(metrics->histogram_totals().enabled_stages, 0U);
    TreeMetricsOperation operation(metrics, TreeStage::FlushBatch, 10);
    ASSERT_TRUE(operation.finish(TreeOutcome::Error, 110));
    EXPECT_EQ(metrics->totals()[kFlush].count, 1U);
    EXPECT_EQ(metrics->totals()[kFlush].sum_ns, 100U);
    EXPECT_EQ(metadata(), initial);
}

TEST_F(TreeWorkHistogramsTest, ExactActualStorageSurvivesItsCollectorAndIsFreedWithTheLastMetricsOwner)
{
    auto metrics = std::make_shared<TreeWorkMetrics>();
    auto credit  = reserve();
    ASSERT_TRUE(credit);
    const auto initial = metadata();
    const auto bytes   = TreeWorkMetrics::histogram_allocation_bytes();
    EXPECT_GT(bytes, 64 * 1024U);
    ASSERT_TRUE(metrics->enable_histogram(TreeStage::FlushBatch, credit).ok());
    EXPECT_EQ(metadata(), initial + bytes);
    credit->seal();
    ASSERT_TRUE(metrics->enable_histogram(TreeStage::FlushBatch, credit).ok());
    EXPECT_EQ(metadata(), initial + bytes);
    auto collector = metrics->claim_collector();
    ASSERT_TRUE(collector);
    credit.reset();
    metrics.reset();
    EXPECT_EQ(metadata(), initial + bytes);
    EXPECT_EQ(collector->capture_histograms().enabled_stages, uint64_t{1} << kFlush);
    collector.reset();
    EXPECT_EQ(metadata(), 0U);
    EXPECT_EQ(account->snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kRouting)], 0U);
}

TEST_F(TreeWorkHistogramsTest, SelectedStageMatchesCommonQuantilesIncludingErrorsOverflowAndSingleTerminal)
{
    auto metrics = std::make_shared<TreeWorkMetrics>();
    auto credit  = reserve();
    ASSERT_TRUE(credit);
    ASSERT_TRUE(metrics->enable_histogram(TreeStage::FlushBatch, credit).ok());
    common::metrics::LatencyHistogram::Data expected;
    constexpr std::array<uint64_t, 8>       durations{0, 1, 127, 128, 5000, uint64_t{1} << 40, UINT64_MAX, UINT64_MAX};
    for (size_t i = 0; i < durations.size(); ++i) {
        TreeMetricsOperation operation(metrics, TreeStage::FlushBatch, 0);
        ASSERT_TRUE(operation.finish(static_cast<TreeOutcome>(i % kTreeOutcomeCount), durations[i]));
        EXPECT_FALSE(operation.finish(TreeOutcome::Success, durations[i]));
        expected.observe(durations[i]);
    }
    TreeMetricsOperation unselected(metrics, TreeStage::PageEncode, 0);
    ASSERT_TRUE(unselected.finish(TreeOutcome::Success, 999));
    const auto  result = metrics->histogram_totals();
    const auto  golden = expected.snapshot();
    const auto &actual = result.stages[kFlush];
    EXPECT_EQ(result.enabled_stages, uint64_t{1} << kFlush);
    EXPECT_EQ(result.stages[kEncode].count, 0U);
    EXPECT_EQ(actual.count, golden.count);
    EXPECT_EQ(actual.sum_low, golden.sum_low);
    EXPECT_EQ(actual.sum_high, golden.sum_high);
    EXPECT_EQ(actual.min_ns, golden.min);
    EXPECT_EQ(actual.max_ns, golden.max);
    EXPECT_EQ(actual.p50_ns, golden.p50);
    EXPECT_EQ(actual.p95_ns, golden.p95);
    EXPECT_EQ(actual.p99_ns, golden.p99);
    EXPECT_EQ(actual.overflow_count, 3U);
    EXPECT_EQ(actual.zero_count, 1U);
    auto collector = metrics->claim_collector();
    ASSERT_TRUE(collector);
    EXPECT_FALSE(metrics->claim_collector());
    const auto window = collector->capture_histograms();
    const auto reader = window;
    EXPECT_EQ(window.stages[kFlush].count, durations.size());
    EXPECT_EQ(collector->capture_histograms().stages[kFlush].count, 0U);
    EXPECT_EQ(reader.stages[kFlush].count, durations.size());
    EXPECT_EQ(metrics->histogram_totals().stages[kFlush].count, durations.size());
}

class TestEnableRaceBudget final : public AllocationBudget
{
  public:
    TestEnableRaceBudget(std::shared_ptr<AllocationBudget> budget, std::barrier<> &allocated)
        : budget_(std::move(budget)),
          allocated_(allocated)
    {
    }

    bool claim(size_t bytes) noexcept override
    {
        return budget_->claim(bytes);
    }

    void release(size_t bytes) noexcept override
    {
        budget_->release(bytes);
    }

    void record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept override
    {
        budget_->record_allocation(kind, bytes, acquired);
        if (acquired) {
            allocated_.arrive_and_wait();
        }
    }

  private:
    std::shared_ptr<AllocationBudget> budget_;
    std::barrier<>                   &allocated_;
};

TEST_F(TreeWorkHistogramsTest, ConcurrentEnablePublishesOnePaidInstanceAndReturnsEveryLosingAllocation)
{
    auto metrics = std::make_shared<TreeWorkMetrics>();
    auto credit  = reserve();
    ASSERT_TRUE(credit);
    const auto                 initial = metadata();
    std::barrier               allocated(8);
    auto                       gated = std::make_shared<TestEnableRaceBudget>(credit, allocated);
    std::array<std::thread, 8> workers;
    for (auto &worker : workers) {
        worker = std::thread([&] { EXPECT_TRUE(metrics->enable_histogram(TreeStage::FlushBatch, gated).ok()); });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    EXPECT_EQ(metadata(), initial + TreeWorkMetrics::histogram_allocation_bytes());
    EXPECT_EQ(metrics->histogram_totals().enabled_stages, uint64_t{1} << kFlush);
    metrics.reset();
    EXPECT_EQ(metadata(), initial);
    gated.reset();
    credit.reset();
    EXPECT_EQ(metadata(), 0U);
}

TEST_F(TreeWorkHistogramsTest, ConcurrentObserveAndSingleDrainKeepExactFinalCumulativeCounts)
{
    auto metrics = std::make_shared<TreeWorkMetrics>();
    auto credit  = reserve();
    ASSERT_TRUE(credit);
    ASSERT_TRUE(metrics->enable_histogram(TreeStage::PageEncode, credit).ok());
    auto collector = metrics->claim_collector();
    ASSERT_TRUE(collector);
    std::atomic<size_t>        active{4};
    std::array<std::thread, 4> workers;
    for (auto &worker : workers) {
        worker = std::thread([&] {
            for (size_t i = 0; i < 1000; ++i) {
                TreeMetricsOperation operation(metrics, TreeStage::PageEncode, 0);
                EXPECT_TRUE(operation.finish(TreeOutcome::Success, 10));
            }
            active.fetch_sub(1);
        });
    }
    uint64_t drained = 0;
    while (active.load() != 0) {
        drained += collector->capture_histograms().stages[kEncode].count;
    }
    for (auto &worker : workers) {
        worker.join();
    }
    drained += collector->capture_histograms().stages[kEncode].count;
    const auto total = metrics->histogram_totals().stages[kEncode];
    EXPECT_EQ(drained, 4000U);
    EXPECT_EQ(total.count, 4000U);
    EXPECT_EQ(total.sum_low, 40000U);
    EXPECT_EQ(total.p50_ns, 10U);
    EXPECT_EQ(total.p99_ns, 10U);
}
} // namespace
} // namespace crowdb::tree
