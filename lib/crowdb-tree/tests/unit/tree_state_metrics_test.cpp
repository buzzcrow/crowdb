// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/tree_metrics.h"

#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <thread>

namespace crowdb::tree
{
namespace
{
PageMetricsContribution known_leaf(uint64_t size, uint64_t count, uint64_t bytes)
{
    PageMetricsContribution page;
    page.page_size_known          = true;
    page.page_size_bytes          = size;
    page.page_size_estimate_bytes = size;
    page.kv_known                 = true;
    page.kv_count                 = count;
    page.kv_bytes                 = bytes;
    return page;
}

TEST(TreeStateMetrics, BlindOverlayInvalidatesExactSummariesWithoutInventingZeros)
{
    TreeLogicalStats stats;
    auto             old_page = known_leaf(4096, 8, 2048);
    ASSERT_TRUE(apply_page_metrics(stats, nullptr, &old_page));
    auto overlay                     = old_page;
    overlay.dirty                    = true;
    overlay.overlay_runs             = 2;
    overlay.overlay_entries          = 3;
    overlay.page_size_known          = false;
    overlay.kv_known                 = false;
    overlay.page_size_estimate_bytes = 8192;
    overlay.consolidate_candidate    = true;
    overlay.split_candidate          = true;
    ASSERT_TRUE(apply_page_metrics(stats, &old_page, &overlay));
    EXPECT_EQ(stats.page_count(), 1U);
    EXPECT_EQ(stats.dirty_pages, 1U);
    EXPECT_EQ(stats.overlay_pages, 1U);
    EXPECT_EQ(stats.overlay_runs, 2U);
    EXPECT_EQ(stats.overlay_entries, 3U);
    EXPECT_EQ(stats.page_size_known_bytes, 0U);
    EXPECT_EQ(stats.page_size_unknown_pages, 1U);
    EXPECT_EQ(stats.kv_known_count, 0U);
    EXPECT_EQ(stats.kv_known_bytes, 0U);
    EXPECT_EQ(stats.kv_unknown_pages, 1U);
    EXPECT_EQ(stats.consolidate_candidate_pages, 1U);
    EXPECT_EQ(stats.split_candidate_pages, 1U);
    EXPECT_EQ(stats.split_ready_pages, 0U);
}

TEST(TreeStateMetrics, MultiwaySplitAndInnerRootContributeOncePerLivePage)
{
    TreeLogicalStats stats;
    auto             original = known_leaf(12000, 12, 10000);
    ASSERT_TRUE(apply_page_metrics(stats, nullptr, &original));
    ASSERT_TRUE(apply_page_metrics(stats, &original, nullptr));
    auto child  = known_leaf(4000, 4, 3000);
    child.dirty = true;
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(apply_page_metrics(stats, nullptr, &child));
    }
    PageMetricsContribution root;
    root.leaf            = false;
    root.dirty           = true;
    root.page_size_known = true;
    root.page_size_bytes = 256;
    ASSERT_TRUE(apply_page_metrics(stats, nullptr, &root));
    stats.height = 2;
    EXPECT_EQ(stats.page_count(), 4U);
    EXPECT_EQ(stats.leaf_pages, 3U);
    EXPECT_EQ(stats.inner_pages, 1U);
    EXPECT_EQ(stats.dirty_pages, 4U);
    EXPECT_EQ(stats.kv_known_count, 12U);
    EXPECT_EQ(stats.kv_unknown_pages, 0U);
    EXPECT_EQ(stats.page_size_known_bytes, 12256U);
    EXPECT_EQ(stats.page_size_max_observed_bytes, 12000U);
}

TEST(TreeStateMetrics, InvalidDeltaDoesNotPartiallyChangeThePublishedStats)
{
    TreeLogicalStats stats;
    auto             page = known_leaf(4096, 1, 100);
    EXPECT_FALSE(apply_page_metrics(stats, &page, nullptr));
    EXPECT_EQ(stats.page_count(), 0U);
    page.split_ready = true;
    EXPECT_FALSE(apply_page_metrics(stats, nullptr, &page));
    EXPECT_EQ(stats.page_count(), 0U);
    page.split_ready     = false;
    page.overlay_entries = 1;
    EXPECT_FALSE(apply_page_metrics(stats, nullptr, &page));
    page.overlay_entries        = 0;
    stats.page_size_known_bytes = std::numeric_limits<uint64_t>::max();
    EXPECT_FALSE(apply_page_metrics(stats, nullptr, &page));
    EXPECT_EQ(stats.page_count(), 0U);
    EXPECT_EQ(stats.page_size_known_bytes, std::numeric_limits<uint64_t>::max());
}

TEST(TreeStateMetrics, MergeMembershipAndPairReadinessAreSeparate)
{
    TreeLogicalStats        stats;
    PageMetricsContribution page;
    page.merge_candidate = true;
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(apply_page_metrics(stats, nullptr, &page));
    }
    ASSERT_TRUE(apply_merge_pair_metrics(stats, 0, 0, 2, 1));
    EXPECT_EQ(stats.merge_candidate_pages, 3U);
    EXPECT_EQ(stats.merge_candidate_pairs, 2U);
    EXPECT_EQ(stats.merge_ready_pairs, 1U);
    EXPECT_FALSE(apply_merge_pair_metrics(stats, 2, 0, 0, 0));
    EXPECT_EQ(stats.merge_candidate_pairs, 2U);
    ASSERT_TRUE(apply_merge_pair_metrics(stats, 2, 1, 1, 1));
    EXPECT_EQ(stats.merge_candidate_pairs, 1U);
    EXPECT_EQ(stats.merge_ready_pairs, 1U);
}

TEST(TreeStateMetrics, SnapshotCommitKeepsContentChangedAfterCapturePending)
{
    CheckpointPendingMetrics pending;
    pending.change_page(1);
    pending.change_page(1);
    pending.change_page(2);
    ASSERT_TRUE(pending.capture(10));
    EXPECT_FALSE(pending.capture(11));
    pending.change_page(2);
    pending.change_page(3);
    EXPECT_FALSE(pending.finish(9, true));
    ASSERT_TRUE(pending.finish(10, true));
    EXPECT_FALSE(pending.finish(10, true));
    EXPECT_FALSE(pending.contains(1));
    EXPECT_TRUE(pending.contains(2));
    EXPECT_TRUE(pending.contains(3));
    TreeLogicalStats stats;
    pending.copy_to(stats);
    EXPECT_EQ(stats.checkpoint_pending_pages, 2U);
    EXPECT_FALSE(pending.capture(10));
    ASSERT_TRUE(pending.capture(11));
    ASSERT_TRUE(pending.finish(11, true));
    pending.copy_to(stats);
    EXPECT_EQ(stats.checkpoint_pending_pages, 0U);
}

TEST(TreeStateMetrics, FailedCheckpointPreservesOriginalAndNewPendingPages)
{
    CheckpointPendingMetrics pending;
    pending.change_page(1);
    ASSERT_TRUE(pending.capture(1));
    pending.change_page(2);
    ASSERT_TRUE(pending.finish(1, false));
    EXPECT_TRUE(pending.contains(1));
    EXPECT_TRUE(pending.contains(2));
    ASSERT_TRUE(pending.capture(2));
    ASSERT_TRUE(pending.finish(2, true));
    EXPECT_FALSE(pending.contains(1));
    EXPECT_FALSE(pending.contains(2));
}

class FailingMetricsResource : public std::pmr::memory_resource
{
  public:
    size_t allocations = 0;
    size_t fail_at     = 0;

  private:
    void *do_allocate(size_t bytes, size_t alignment) override
    {
        if (++allocations == fail_at) {
            throw std::bad_alloc();
        }
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }

    void do_deallocate(void *ptr, size_t bytes, size_t alignment) override
    {
        std::pmr::new_delete_resource()->deallocate(ptr, bytes, alignment);
    }

    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource &other) const noexcept override
    {
        return this == &other;
    }
};

TEST(TreeStateMetrics, AllocationFailureRollsBackBothCheckpointMembershipSets)
{
    size_t failures = 0;
    for (size_t offset = 1; offset <= 4; ++offset) {
        FailingMetricsResource   resource;
        CheckpointPendingMetrics pending(&resource);
        pending.change_page(1);
        ASSERT_TRUE(pending.capture(1));
        resource.fail_at = resource.allocations + offset;
        bool inserted    = false;
        try {
            pending.change_page(2);
            inserted = true;
        }
        catch (const std::bad_alloc &) {
            ++failures;
        }
        EXPECT_TRUE(pending.contains(1));
        EXPECT_EQ(pending.contains(2), inserted);
        ASSERT_TRUE(pending.finish(1, true));
        EXPECT_FALSE(pending.contains(1));
        EXPECT_EQ(pending.contains(2), inserted);
    }
    EXPECT_GE(failures, 2U);
}

TEST(TreeStateMetrics, DeletionRemainsPendingWithoutALivePage)
{
    CheckpointPendingMetrics pending;
    pending.change_page(7);
    ASSERT_TRUE(pending.capture(1));
    pending.remove_page(7);
    ASSERT_TRUE(pending.finish(1, true));
    TreeLogicalStats stats;
    pending.copy_to(stats);
    EXPECT_EQ(stats.checkpoint_pending_pages, 0U);
    EXPECT_TRUE(stats.checkpoint_mapping_pending);
    ASSERT_TRUE(pending.capture(2));
    ASSERT_TRUE(pending.finish(2, true));
    pending.copy_to(stats);
    EXPECT_FALSE(stats.checkpoint_mapping_pending);
}

TEST(TreeStateMetrics, ReloadableWritebackCanClearDirtyWithoutClearingCheckpointPending)
{
    TreeLogicalStats         stats;
    CheckpointPendingMetrics pending;
    auto                     page = known_leaf(4000, 8, 3000);
    page.dirty                    = true;
    page.writeback_candidate      = true;
    pending.change_page(1);
    ASSERT_TRUE(apply_page_metrics(stats, nullptr, &page));
    auto clean                = page;
    clean.dirty               = false;
    clean.writeback_candidate = false;
    ASSERT_TRUE(apply_page_metrics(stats, &page, &clean));
    pending.copy_to(stats);
    EXPECT_EQ(stats.dirty_pages, 0U);
    EXPECT_EQ(stats.writeback_candidate_pages, 0U);
    EXPECT_EQ(stats.checkpoint_pending_pages, 1U);
}

TEST(TreeResourceMetrics, PhysicalOccupancyRejectsUnderflowAndOverflow)
{
    TreeResourceMetrics metrics;
    ASSERT_TRUE(metrics.add(TreeResourceMetric::AllocatedBytes, 8192));
    auto snapshot = metrics.snapshot();
    EXPECT_EQ(snapshot.allocated_bytes, 8192U);
    EXPECT_FALSE(metrics.remove(TreeResourceMetric::AllocatedBytes, 8193));
    EXPECT_FALSE(metrics.add(TreeResourceMetric::AllocatedBytes, std::numeric_limits<uint64_t>::max()));
    EXPECT_EQ(metrics.snapshot().allocated_bytes, 8192U);
}

TEST(TreeResourceMetrics, ConcurrentOwnerAcquireAndReleaseReconcileAtQuiescence)
{
    TreeResourceMetrics        metrics;
    std::array<std::thread, 4> workers;
    for (auto &worker : workers) {
        worker = std::thread([&] {
            for (int i = 0; i < 1000; ++i) {
                EXPECT_TRUE(metrics.add(TreeResourceMetric::AllocatedBytes, 128));
                EXPECT_TRUE(metrics.remove(TreeResourceMetric::AllocatedBytes, 128));
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    EXPECT_EQ(metrics.snapshot().allocated_bytes, 0U);
}

TEST(TreeWorkMetrics, OutcomeAndLatencyRecordOnlyFirstTerminal)
{
    auto                 metrics = std::make_shared<TreeWorkMetrics>();
    TreeMetricsOperation operation(metrics, TreeStage::FlushBatch, 100);
    constexpr auto       stage = static_cast<size_t>(TreeStage::FlushBatch);
    EXPECT_EQ(metrics->totals()[stage].inflight, 1U);
    std::thread success([&] { (void)operation.finish(TreeOutcome::Success, 200); });
    std::thread error([&] { (void)operation.finish(TreeOutcome::Error, 200); });
    success.join();
    error.join();
    EXPECT_FALSE(operation.finish(TreeOutcome::Cancelled, 300));
    const auto totals = metrics->totals()[stage];
    EXPECT_EQ(totals.attempts, 1U);
    EXPECT_EQ(totals.inflight, 0U);
    EXPECT_EQ(totals.count, 1U);
    EXPECT_EQ(totals.sum_ns, 100U);
    EXPECT_EQ(totals.max_ns, 100U);
    EXPECT_EQ(totals.outcomes[static_cast<size_t>(TreeOutcome::Success)] +
                  totals.outcomes[static_cast<size_t>(TreeOutcome::Error)],
              1U);
}

TEST(TreeWorkMetrics, OneCollectorDrainsAndReadersKeepCumulativeTotals)
{
    auto metrics   = std::make_shared<TreeWorkMetrics>();
    auto collector = metrics->claim_collector();
    ASSERT_NE(collector, nullptr);
    EXPECT_EQ(metrics->claim_collector(), nullptr);
    TreeMetricsOperation first(metrics, TreeStage::PageSplit, 10);
    ASSERT_TRUE(first.finish(TreeOutcome::Success, 30));
    TreeMetricsOperation second(metrics, TreeStage::PageSplit, 40);
    ASSERT_TRUE(second.finish(TreeOutcome::Retry, 70));
    constexpr auto stage  = static_cast<size_t>(TreeStage::PageSplit);
    const auto     totals = metrics->totals()[stage];
    EXPECT_EQ(totals.count, 2U);
    const auto window = collector->capture_window()[stage];
    EXPECT_EQ(window.count, 2U);
    EXPECT_EQ(window.sum_ns, 50U);
    EXPECT_EQ(window.max_ns, 30U);
    EXPECT_EQ(window.total_count, 2U);
    const auto empty = collector->capture_window()[stage];
    EXPECT_EQ(empty.count, 0U);
    EXPECT_EQ(empty.sum_ns, 0U);
    EXPECT_EQ(empty.total_count, 2U);
    EXPECT_EQ(metrics->totals()[stage].sum_ns, 50U);
    collector.reset();
    EXPECT_NE(metrics->claim_collector(), nullptr);
}

TEST(TreeWorkMetrics, AbandonedOperationCancelsAndKeepsOwnerAlive)
{
    auto                                 metrics = std::make_shared<TreeWorkMetrics>();
    const std::weak_ptr<TreeWorkMetrics> weak    = metrics;
    auto                                 operation =
        std::make_unique<TreeMetricsOperation>(metrics, TreeStage::PageLoadPipeline, tree_metrics_now_ns());
    metrics.reset();
    ASSERT_FALSE(weak.expired());
    const auto observer = weak.lock();
    operation.reset();
    constexpr auto stage  = static_cast<size_t>(TreeStage::PageLoadPipeline);
    const auto     totals = observer->totals()[stage];
    EXPECT_EQ(totals.count, 1U);
    EXPECT_EQ(totals.inflight, 0U);
    EXPECT_EQ(totals.outcomes[static_cast<size_t>(TreeOutcome::Cancelled)], 1U);
}

} // namespace
} // namespace crowdb::tree
