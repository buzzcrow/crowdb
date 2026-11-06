// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <latch>
#include <memory>
#include <thread>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<InstallRecord> metric_record(uint64_t sequence, uint64_t generation = 1)
{
    return std::make_shared<InstallRecord>(
        InstallToken{.generation = generation, .batch = 1, .range = sequence, .sequence = 0});
}

std::shared_ptr<PublishedPage> metric_page(uint64_t revision)
{
    auto page              = std::make_shared<PublishedPage>();
    page->revision         = revision;
    page->leaf             = std::make_shared<PageState>();
    page->metrics.dirty    = revision % 2 != 0;
    page->metrics.kv_known = true;
    page->metrics.kv_count = revision;
    return page;
}

PublicationResult metric_publish(PublicationCoordinator &coordinator, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> done;
    auto                            result = done.get_future();
    request->on_done                       = [&done](const auto &value) { done.set_value(value); };
    if (!coordinator.try_submit(request)) {
        ADD_FAILURE() << "publication was not accepted";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

TEST(PublicationMetrics, RetryObservesAnAttemptWithoutRepeatingLogicalDeltas)
{
    auto                   work = std::make_shared<TreeWorkMetrics>();
    PublicationCoordinator coordinator(8, 1, work);
    TreeResourceMetrics    resources;
    auto                   page = metric_page(1);
    PublicationTransaction transaction{
        .record = metric_record(1), .pages = {{.page_id = 0, .expected = {}, .replacement = page}}, .root_page_id = 0};
    ASSERT_TRUE(metric_publish(coordinator, transaction).status.ok());
    const auto first = coordinator.stats(resources);
    ASSERT_TRUE(metric_publish(coordinator, transaction).already_committed);
    auto stale =
        metric_publish(coordinator, {.record = metric_record(2),
                                     .pages  = {{.page_id = 0, .expected = {}, .replacement = metric_page(2)}}});
    EXPECT_EQ(stale.status.code(), Code::kUnavailable);
    const auto after = coordinator.stats(resources);
    EXPECT_EQ(after.commit_seq, first.commit_seq);
    EXPECT_EQ(after.logical.page_count(), 1U);
    EXPECT_EQ(after.logical.kv_known_count, 1U);
    EXPECT_EQ(after.logical.dirty_pages, 1U);

    const auto totals = work->totals();
    const auto commit = totals[static_cast<size_t>(TreeStage::PublishCommit)];
    EXPECT_EQ(commit.attempts, 3U);
    EXPECT_EQ(commit.count, 3U);
    EXPECT_EQ(commit.inflight, 0U);
    EXPECT_EQ(commit.outcomes[static_cast<size_t>(TreeOutcome::Success)], 1U);
    EXPECT_EQ(commit.outcomes[static_cast<size_t>(TreeOutcome::Retry)], 1U);
    EXPECT_EQ(commit.outcomes[static_cast<size_t>(TreeOutcome::Stale)], 1U);
    EXPECT_EQ(totals[static_cast<size_t>(TreeStage::PublishQueueWait)].count, 3U);
}

TEST(PublicationMetrics, SnapshotCapturesOneLogicalCommitAndIndependentResourceGauges)
{
    PublicationCoordinator coordinator(8, 17);
    TreeResourceMetrics    resources;
    ASSERT_TRUE(resources.add(TreeResourceMetric::AllocatedBytes, 8192));
    ASSERT_TRUE(resources.add(TreeResourceMetric::ProxyPages, 1));
    auto page = metric_page(1);
    ASSERT_TRUE(metric_publish(coordinator, {.record       = metric_record(1, 17),
                                             .pages        = {{.page_id = 0, .expected = {}, .replacement = page}},
                                             .root_page_id = 0})
                    .status.ok());
    std::atomic<bool> finished{false};
    std::thread       reader([&] {
        do {
            const auto sampled = coordinator.stats(resources);
            EXPECT_EQ(sampled.instance_generation, 17U);
            EXPECT_EQ(sampled.logical.kv_known_count, sampled.commit_seq);
            EXPECT_EQ(sampled.logical.dirty_pages, sampled.commit_seq % 2);
            EXPECT_EQ(sampled.logical.height, 1U);
            EXPECT_EQ(sampled.logical.page_size_unknown_pages, 1U);
            EXPECT_EQ(sampled.resources.allocated_bytes, 8192U);
            EXPECT_EQ(sampled.resources.proxy_pages, 1U);
        } while (!finished.load(std::memory_order_acquire));
    });
    for (uint64_t revision = 2; revision <= 100; ++revision) {
        auto next = metric_page(revision);
        EXPECT_TRUE(metric_publish(coordinator, {.record = metric_record(revision, 17),
                                                 .pages  = {{.page_id = 0, .expected = page, .replacement = next}}})
                        .status.ok());
        page = std::move(next);
    }
    finished.store(true, std::memory_order_release);
    reader.join();
    EXPECT_EQ(coordinator.stats(resources).commit_seq, 100U);
}

TEST(PublicationMetrics, QueuedWorkIsVisibleBeforeItsCommitStarts)
{
    auto                   work = std::make_shared<TreeWorkMetrics>();
    PublicationCoordinator coordinator(2, 1, work);
    std::latch             first_completed(1);
    std::latch             release_callback(1);
    auto                   first = std::make_shared<PublicationRequest>();
    first->transaction.record    = metric_record(1);
    first->on_done               = [&](const auto &result) {
        EXPECT_TRUE(result.status.ok());
        first_completed.count_down();
        release_callback.wait();
    };
    std::thread publisher([&] { EXPECT_TRUE(coordinator.try_submit(first)); });
    first_completed.wait();
    auto second                = std::make_shared<PublicationRequest>();
    second->transaction.record = metric_record(2);
    EXPECT_TRUE(coordinator.try_submit(second));
    const auto waiting = work->totals();
    const auto queue   = waiting[static_cast<size_t>(TreeStage::PublishQueueWait)];
    EXPECT_EQ(queue.attempts, 2U);
    EXPECT_EQ(queue.inflight, 1U);
    EXPECT_EQ(queue.count, 1U);
    EXPECT_EQ(waiting[static_cast<size_t>(TreeStage::PublishCommit)].inflight, 0U);
    release_callback.count_down();
    publisher.join();
    const auto complete = work->totals();
    EXPECT_EQ(complete[static_cast<size_t>(TreeStage::PublishQueueWait)].count, 2U);
    EXPECT_EQ(complete[static_cast<size_t>(TreeStage::PublishQueueWait)].inflight, 0U);
    EXPECT_EQ(complete[static_cast<size_t>(TreeStage::PublishCommit)].count, 2U);
}

TEST(PublicationMetrics, InvalidDeltaRecordsErrorAndKeepsPreviousLogicalSnapshot)
{
    auto                   work = std::make_shared<TreeWorkMetrics>();
    PublicationCoordinator coordinator(2, 1, work);
    TreeResourceMetrics    resources;
    auto                   invalid   = metric_page(1);
    invalid->metrics.split_ready     = true;
    invalid->metrics.split_candidate = false;
    const auto result = metric_publish(coordinator, {.record = metric_record(1),
                                                     .pages  = {{.page_id = 0, .expected = {}, .replacement = invalid}},
                                                     .root_page_id = 0});
    EXPECT_EQ(result.status.code(), Code::kInvalidArgument);
    const auto stats = coordinator.stats(resources);
    EXPECT_EQ(stats.commit_seq, 0U);
    EXPECT_EQ(stats.logical.page_count(), 0U);
    EXPECT_EQ(stats.logical.split_ready_pages, 0U);
    const auto totals = work->totals()[static_cast<size_t>(TreeStage::PublishCommit)];
    EXPECT_EQ(totals.outcomes[static_cast<size_t>(TreeOutcome::Error)], 1U);
    EXPECT_EQ(totals.inflight, 0U);
}

} // namespace
} // namespace crowdb::tree
