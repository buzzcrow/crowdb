// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_batch.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
void initialize(PublicationCoordinator &publisher, const std::vector<std::shared_ptr<MemTable>> &tables)
{
    auto leaf    = std::make_shared<PublishedPage>();
    leaf->leaf   = std::make_shared<PageState>();
    auto sources = std::make_shared<PublishedSources>();
    sources->tables.assign(tables.begin(), tables.end());
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = {.record           = std::make_shared<InstallRecord>(InstallToken{}),
                            .pages            = {{.page_id = 0, .expected = {}, .replacement = leaf}},
                            .root_page_id     = 0,
                            .expected_sources = publisher.capture()->sources,
                            .sources          = sources};
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(request));
}

std::shared_ptr<MemTable> make_source(const std::vector<std::string> &keys, uint64_t slot, Slice value)
{
    auto table = std::make_shared<MemTable>();
    for (const auto &key : keys) {
        EXPECT_TRUE(table->upsert(key, slot, encode_cell_buf(slot, OpKind::kPut, value)));
    }
    table->close();
    return table;
}

TEST(FlushContinuation, EntryAndArenaWindowsPreserveBinaryKeysAndAllSourceWinners)
{
    for (const size_t workers : {1U, 4U}) {
        for (const RunLimits limits : {
                 RunLimits{.max_entries = 1},
                 RunLimits{.max_entries = 100, .max_key_bytes = 7, .key_block_bytes = 4}
        }) {
            const std::vector<std::string> keys{"", std::string("\0", 1), std::string("a\0b", 3), "aaaa", "zz"};
            auto                           first  = make_source(keys, 1, "older");
            auto                           second = make_source(keys, 2, "newer");
            auto                           future = make_source(keys, 3, "after-frontier");
            detail::FlushRuntime           runtime(workers, 8);
            auto                           lane = std::make_shared<detail::FlushRuntime::Lane>(8);
            PublicationCoordinator         publisher;
            initialize(publisher, {first, second, future});
            const auto        before = publisher.capture();
            FlushBatchOptions options;
            options.run_limits = limits;
            std::shared_ptr<FlushBatch> batch;
            ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 2, options, {}, &batch).ok());
            ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
            runtime.drain();
            ASSERT_TRUE(batch->result().handoff_status.ok()) << batch->result().handoff_status.to_string();
            const auto after = publisher.capture();
            EXPECT_EQ(after->coverage, 2U);
            EXPECT_EQ(after->stats.overlay_entries, keys.size());
            const auto &runs = after->mapping.get(0)->leaf->overlays;
            ASSERT_GT(runs.size(), 1U);
            std::vector<std::string> actual;
            for (const auto &run : runs) {
                EXPECT_LE(run->entries().size(), limits.max_entries);
                EXPECT_LE(run->key_bytes(), limits.max_key_bytes);
                for (const auto &entry : run->entries()) {
                    actual.push_back(entry.key().to_string());
                    EXPECT_EQ(entry.slot(), 2U);
                    EXPECT_EQ(entry.value().to_string(), "newer");
                    auto guard = second->read_guard();
                    EXPECT_EQ(entry.owner.get(), second->find(entry.key()));
                }
            }
            EXPECT_EQ(actual, keys);
            EXPECT_TRUE(before->mapping.get(0)->leaf->overlays.empty());
            EXPECT_EQ(before->sources->tables.size(), 3U);
            ASSERT_EQ(after->sources->tables.size(), 1U);
            EXPECT_EQ(after->sources->tables.front(), future);
        }
    }
}

TEST(FlushContinuation, OversizedKeyDoesNotPublishOrLoopAfterPreparedPrefix)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    initialize(publisher, {make_source({"a", "b", "cccc"}, 1, "v")});
    FlushBatchOptions options;
    options.run_limits.max_entries   = 1;
    options.run_limits.max_key_bytes = 3;
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
    for (size_t attempt = 0; attempt < 2; ++attempt) {
        ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
        runtime.drain();
        EXPECT_EQ(batch->result().handoff_status.code(), Code::kResourceExhausted);
        EXPECT_EQ(batch->result().phase, FlushPhase::kRepairRequired);
        EXPECT_EQ(publisher.capture()->coverage, 0U);
        EXPECT_EQ(publisher.capture()->stats.overlay_entries, 0U);
        EXPECT_EQ(publisher.capture()->sources->tables.size(), 1U);
    }
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(FlushContinuation, RejectedContinuationRetainsPrefixAndYieldsWorkerBetweenRuns)
{
    detail::FlushRuntime   runtime(1, 1);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(1);
    PublicationCoordinator publisher;
    initialize(publisher, {make_source({"a", "b", "c"}, 1, "v")});
    FlushBatchOptions options;
    options.run_limits.max_entries = 1;
    std::vector<size_t>   prepared;
    std::vector<uint64_t> completed;
    bool                  other_work_ran = false;
    options.after_run_for_tests          = [&](size_t, size_t runs) {
        prepared.push_back(runs);
        completed.push_back(runtime.snapshot().completed);
        EXPECT_EQ(publisher.capture()->coverage, 0U);
        EXPECT_EQ(publisher.capture()->stats.overlay_entries, 0U);
        if (runs == 1) {
            EXPECT_TRUE(
                runtime
                    .try_submit(
                        lane, [](void *context) noexcept { *static_cast<bool *>(context) = true; }, &other_work_ran)
                    .ok());
        }
    };
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(other_work_ran);
    EXPECT_EQ(batch->result().handoff_status.code(), Code::kResourceExhausted);
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    EXPECT_EQ(prepared, (std::vector<size_t>{1}));
    ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
    runtime.drain();
    EXPECT_TRUE(batch->result().handoff_status.ok());
    EXPECT_EQ(prepared, (std::vector<size_t>{1, 2, 3}));
    ASSERT_EQ(completed.size(), 3U);
    EXPECT_GT(completed[1], completed[0]);
    EXPECT_GT(completed[2], completed[1]);
    EXPECT_EQ(publisher.capture()->stats.overlay_entries, 3U);
    EXPECT_EQ(publisher.capture()->coverage, 1U);
}

TEST(FlushContinuation, CancellationBetweenRunsOrBeforePublicationResumesWithoutRebuilding)
{
    for (const size_t cancel_after : {1U, 3U}) {
        detail::FlushRuntime   runtime(1, 8);
        auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
        PublicationCoordinator publisher;
        initialize(publisher, {make_source({"a", "b", "c"}, 1, "v")});
        FlushBatchOptions options;
        options.run_limits.max_entries = 1;
        std::shared_ptr<FlushBatch> batch;
        std::vector<size_t>         prepared;
        options.after_run_for_tests = [&](size_t, size_t runs) {
            prepared.push_back(runs);
            if (runs == cancel_after) {
                batch->request_stop();
            }
        };
        ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
        ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
        runtime.drain();
        EXPECT_EQ(batch->result().phase, FlushPhase::kRepairRequired);
        EXPECT_EQ(publisher.capture()->coverage, 0U);
        EXPECT_EQ(publisher.capture()->sources->tables.size(), 1U);
        EXPECT_EQ(publisher.capture()->stats.overlay_entries, 0U);
        EXPECT_EQ(prepared.size(), cancel_after);
        ASSERT_TRUE(stdexec::sync_wait(batch->sender()).has_value());
        runtime.drain();
        EXPECT_TRUE(batch->result().handoff_status.ok());
        EXPECT_EQ(prepared, (std::vector<size_t>{1, 2, 3}));
        EXPECT_EQ(publisher.capture()->stats.overlay_entries, 3U);
        EXPECT_EQ(publisher.capture()->coverage, 1U);
        EXPECT_TRUE(publisher.capture()->sources->tables.empty());
    }
}
#endif
} // namespace
} // namespace crowdb::tree
