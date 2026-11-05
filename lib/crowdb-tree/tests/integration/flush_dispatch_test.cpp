// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_batch.h"

#include <gtest/gtest.h>

#include <future>
#include <latch>

namespace crowdb::tree
{
namespace
{
void initialize_dispatch(PublicationCoordinator &publisher, size_t ranges, size_t entries, size_t value_bytes = 1)
{
    auto table                  = std::make_shared<MemTable>();
    auto inner                  = std::make_shared<InnerPageState>();
    auto request                = std::make_shared<PublicationRequest>();
    request->transaction.record = std::make_shared<InstallRecord>(InstallToken{});
    for (size_t index = 0; index < ranges; ++index) {
        auto logical = std::make_shared<PageState>();
        if (index != 0) {
            logical->range.lower = OwnedKey(std::to_string(1000 + (100 * index)));
            inner->separators.push_back(*logical->range.lower);
        }
        if (index + 1 < ranges) {
            logical->range.upper   = OwnedKey(std::to_string(1100 + (100 * index)));
            logical->right_sibling = index + 1;
        }
        inner->children.push_back(index);
        auto page  = std::make_shared<PublishedPage>();
        page->leaf = logical;
        request->transaction.pages.push_back({.page_id = index, .expected = {}, .replacement = page});
        for (size_t entry = 0; entry < entries; ++entry) {
            const auto key = std::to_string(1000 + (100 * index) + entry);
            ASSERT_TRUE(table->upsert(key, 1, encode_cell_buf(1, OpKind::kPut, std::string(value_bytes, 'v'))));
        }
    }
    table->close();
    auto sources = std::make_shared<PublishedSources>();
    sources->tables.push_back(table);
    request->transaction.expected_sources = publisher.capture()->sources;
    request->transaction.sources          = sources;
    auto root                             = std::make_shared<PublishedPage>();
    root->level                           = 1;
    root->metrics.leaf                    = false;
    root->inner                           = inner;
    request->transaction.pages.push_back({.page_id = ranges, .expected = {}, .replacement = root});
    request->transaction.root_page_id = ranges;
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(request));
}

TEST(FlushDispatch, SmallRangesFinishWithinTheRoutingTask)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize_dispatch(publisher, 2, 2);
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, {}, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok());
    const auto metrics = publisher.work_metrics()->dispatch.snapshot();
    EXPECT_EQ(metrics.inline_probes, 2U);
    EXPECT_EQ(metrics.inline_ranges, 2U);
    EXPECT_EQ(metrics.queued_steps, 0U);
    EXPECT_EQ(metrics.prepared_entries, 4U);
    EXPECT_EQ(metrics.active_steps, 0U);
    EXPECT_EQ(metrics.peak_steps, 1U);
    EXPECT_GT(metrics.cow_bytes, 0U);
}

TEST(FlushDispatch, LargeValuesYieldAfterEachByteBoundedRun)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize_dispatch(publisher, 1, 5, 128);
    FlushBatchOptions options;
    options.inline_bytes  = 64;
    options.max_run_bytes = 64;
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok());
    const auto metrics = publisher.work_metrics()->dispatch.snapshot();
    EXPECT_EQ(metrics.inline_probes, 1U);
    EXPECT_EQ(metrics.inline_ranges, 0U);
    EXPECT_EQ(metrics.queued_steps, 4U);
    EXPECT_EQ(metrics.prepared_runs, 5U);
    EXPECT_EQ(metrics.prepared_entries, 5U);
    EXPECT_EQ(metrics.prepared_bytes, 5U * (4 + 128 + kCellHeaderSize));
    EXPECT_EQ(publisher.capture()->coverage, 1U);
    for (const auto &run : publisher.capture()->mapping.get(0)->leaf->overlays) {
        EXPECT_EQ(run->entries().size(), 1U);
    }
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(FlushDispatch, InlineQuotaBoundsEachCpuTurn)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize_dispatch(publisher, 4, 1);
    FlushBatchOptions options;
    options.inline_ranges_per_step = 1;

    std::vector<uint64_t> completed;
    options.after_run_for_tests = [&](size_t, size_t) { completed.push_back(runtime.snapshot().completed); };
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()));
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok());
    ASSERT_EQ(completed.size(), 4U);
    for (size_t index = 1; index < completed.size(); ++index) {
        EXPECT_GT(completed[index], completed[index - 1]);
    }
    EXPECT_GT(publisher.work_metrics()->dispatch.snapshot().queued_steps, 0U);
}

TEST(FlushDispatch, ActualParallelStepsRemainSeparateFromCoverage)
{
    detail::FlushRuntime   runtime(2, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize_dispatch(publisher, 2, 2);
    FlushBatchOptions options;
    options.inline_ranges_per_step = 0;
    std::latch entered(2);
    std::latch release(1);
    options.after_run_for_tests = [&](size_t, size_t) {
        entered.count_down();
        release.wait();
    };
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
    auto done = std::async(std::launch::async, [&] { return stdexec::sync_wait(batch->sender()).has_value(); });
    entered.wait();
    const auto metrics = publisher.work_metrics()->dispatch.snapshot();
    EXPECT_EQ(metrics.active_steps, 2U);
    EXPECT_GE(metrics.peak_steps, 2U);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    release.count_down();
    EXPECT_TRUE(done.get());
    runtime.drain();
    ASSERT_TRUE(batch->result().handoff_status.ok());
    EXPECT_EQ(publisher.work_metrics()->dispatch.snapshot().active_steps, 0U);
    EXPECT_EQ(publisher.capture()->coverage, 1U);
}
#endif
} // namespace
} // namespace crowdb::tree
