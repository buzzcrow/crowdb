// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "recovered_state_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

TEST(RecoveredState, BuildsColdMappingAndExactLeafMetricsThroughBoundedSteps)
{
    TestRecoveredGraph test;
    test.balanced();
    ASSERT_TRUE(test.prepare({.max_step_entries = 1}).ok());
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    const auto &result = test.operation->result();
    ASSERT_NE(result.state, nullptr);
    EXPECT_EQ(result.pages, 3U);
    EXPECT_EQ(result.loads, 3U);
    EXPECT_GT(result.cpu_steps, 12U);
    EXPECT_EQ(result.state->generation, 3U);
    EXPECT_EQ(result.state->coverage, 10U);
    EXPECT_EQ(result.state->sources->floor, 10U);
    EXPECT_TRUE(result.state->sources->tables.empty());
    EXPECT_EQ(result.state->next_page_id, 3U);
    EXPECT_EQ(result.state->stats.leaf_pages, 2U);
    EXPECT_EQ(result.state->stats.inner_pages, 1U);
    EXPECT_EQ(result.state->stats.kv_known_count, 2U);
    EXPECT_EQ(result.state->stats.kv_known_bytes, 8U);
    EXPECT_EQ(result.state->stats.kv_unknown_pages, 0U);
    EXPECT_EQ(result.state->stats.page_size_unknown_pages, 1U);
    EXPECT_EQ(result.state->stats.dirty_pages, 0U);
    EXPECT_EQ(result.state->mapping.get(0)->level, 1U);
    EXPECT_FALSE(result.state->mapping.get(0)->inner);
    EXPECT_TRUE(result.state->mapping.get(0)->inner_backing);
    for (uint64_t pid : {1, 2}) {
        const auto &base = result.state->mapping.get(pid)->leaf->bases.front();
        EXPECT_FALSE(base.resident);
        EXPECT_EQ(base.max_slot, 10U);
        EXPECT_EQ(base.backing, test.pages->frames.at(pid).backing);
    }
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(result.state, test.loader, "z", {}, &request).ok());
    test.complete(request->sender());
    EXPECT_TRUE(request->result().found);
    EXPECT_EQ(request->result().cell.value, Slice("two"));
}

TEST(RecoveredState, RejectsMissingRepeatedCyclicAndUnevenTreeEdges)
{
    for (size_t scenario = 0; scenario < 4; ++scenario) {
        SCOPED_TRACE(scenario);
        TestRecoveredGraph test;
        test.balanced();
        if (scenario == 0) {
            test.pages->frames.erase(2);
        }
        if (scenario == 1) {
            test.index(0, {1, 1}, {"m"});
        }
        if (scenario == 2) {
            test.index(0, {0, 2}, {"m"});
        }
        if (scenario == 3) {
            test.index(2, {3}, {});
            test.leaf(1,
                      {
                          {.key = "a", .value = "one"}
            },
                      3);
            test.leaf(3, {
                             {.key = "z", .value = "two"}
            });
        }
        ASSERT_TRUE(test.prepare().ok());
        Completion done;
        test.run(&done);
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), Code::kCorruption);
        EXPECT_FALSE(test.operation->result().state);
    }
}

TEST(RecoveredState, RejectsBadSiblingFencesOutOfRangeKeysAndUnreachableFrames)
{
    for (size_t scenario = 0; scenario < 5; ++scenario) {
        SCOPED_TRACE(scenario);
        TestRecoveredGraph test;
        test.balanced();
        if (scenario == 0) {
            test.leaf(1,
                      {
                          {.key = "a", .value = "one"}
            },
                      1);
        }
        if (scenario == 1) {
            test.leaf(2,
                      {
                          {.key = "z", .value = "two"}
            },
                      1);
        }
        if (scenario == 2) {
            test.index(0, {1, 2}, {"m"}, std::pair{2, 1});
        }
        if (scenario == 3) {
            test.leaf(1,
                      {
                          {.key = "m", .value = "out"}
            },
                      2);
        }
        if (scenario == 4) {
            test.leaf(3, {});
        }
        ASSERT_TRUE(test.prepare().ok());
        Completion done;
        test.run(&done);
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), Code::kCorruption);
        EXPECT_FALSE(test.operation->result().state);
    }
}

TEST(RecoveredState, RejectsMissingStoredDependencyProofAndEnforcesBounds)
{
    for (size_t scenario = 0; scenario < 5; ++scenario) {
        SCOPED_TRACE(scenario);
        TestRecoveredGraph test;
        test.balanced();
        RecoveredStateLimits limits;
        if (scenario == 0) {
            test.pages->frames.at(1).stored_overflow.reset();
        }
        if (scenario == 1) {
            limits.max_depth = 1;
        }
        if (scenario == 2) {
            limits.max_children = 1;
        }
        if (scenario == 3) {
            limits.max_leaf_entries = 1;
        }
        if (scenario == 4) {
            limits.max_key_bytes = 0;
        }
        ASSERT_TRUE(test.prepare(limits).ok());
        Completion done;
        test.run(&done);
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), scenario == 0 ? Code::kCorruption : Code::kResourceExhausted);
    }
}

TEST(RecoveredState, PreservesStoredOverflowHistoryHiddenByAnInlineDelta)
{
    TestRecoveredGraph   test;
    std::vector<uint8_t> raw(4096);
    LeafFrameBuilder     builder(raw.data(), raw.size());
    const auto           overflow = encode_overflow_cell_buf(2, 1, 3);
    ASSERT_TRUE(builder.try_append_sorted("a", Slice(overflow)));
    builder.finish(0, kInvalidPageId);
    std::vector<leaf_entry> delta;
    delta.push_back({.key = "a", .cell = encode_cell_buf(3, OpKind::kPut, "new")});
    std::vector<uint8_t> appended(raw.size());
    ASSERT_TRUE(leaf_frame_append_deltas(raw.data(), raw.size(), delta, appended.data()));
    test.add(0, appended);
    overflow_frame_build(raw.data(), raw.size(), 1, kInvalidPageId, Slice("old").bytes(), 3);
    test.add(1, raw);
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(test.operation->result().state->stats.kv_known_count, 1U);
    EXPECT_EQ(test.operation->result().state->stats.kv_known_bytes, 4U);
    EXPECT_FALSE(test.operation->result().state->mapping.get(1));
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(test.operation->result().state, test.loader, "a", {}, &request).ok());
    test.complete(request->sender());
    EXPECT_EQ(request->result().cell.value, Slice("new"));
}

TEST(RecoveredState, RejectsDuplicateAndEscapingSeparators)
{
    for (bool duplicate : {false, true}) {
        TestRecoveredGraph test;
        test.balanced();
        if (duplicate) {
            std::vector<uint8_t> raw(4096);
            ASSERT_TRUE(inner_frame_build(raw.data(), raw.size(), 0, {1, 2, 3}, {"m", "m"}));
            encode_durable_page(raw.data(), raw.size(), compress_algo::kNone, &test.store->blobs[0]);
        }
        else {
            test.index(1, {3, 4}, {"z"});
            test.leaf(3,
                      {
                          {.key = "a", .value = "one"}
            },
                      4);
            test.leaf(4, {}, 2);
        }
        ASSERT_TRUE(test.prepare().ok());
        Completion done;
        test.run(&done);
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), Code::kCorruption);
    }
}

TEST(RecoveredState, ExhaustedAdmissionFailsWithoutStartingIo)
{
    TestRecoveredGraph test;
    test.balanced();
    const auto held = test.credits.credits.reservation()->available();
    ASSERT_TRUE(test.credits.credits.reservation()->claim(held));
    EXPECT_EQ(test.prepare().code(), Code::kResourceExhausted);
    EXPECT_EQ(test.store->reads.load(), 0U);
    EXPECT_FALSE(test.operation);
    test.credits.credits.reservation()->release(held);
    test.loader.reset();
    test.pages.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(RecoveredState, CancelWaitsForAcceptedReadAndReleasesAllChargedMetadata)
{
    TestRecoveredGraph test;
    test.balanced();
    test.store->deferred = true;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{&done});
    stdexec::start(connected);
    test.runtime->drain();
    ASSERT_EQ(test.store->pending.size(), 1U);
    test.operation->request_stop();
    test.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 0U);
    test.store->finish();
    test.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_FALSE(test.operation->result().state);
    test.operation.reset();
    test.pages.reset();
    test.loader.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}
} // namespace crowdb::tree
