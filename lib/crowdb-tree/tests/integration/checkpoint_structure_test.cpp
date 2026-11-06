// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "checkpoint_structure_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_structure;

TEST(CheckpointStructure, OneWindowHasExactCreditAndPublishesOnlyItsFixedTarget)
{
    TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(structural_input()).ok());
    const auto before = test.publisher.capture();
    ASSERT_TRUE(test.prepare(before).ok());
    test.begin();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    ASSERT_TRUE(test.terminal.ok()) << test.terminal.to_string();
    const auto after = test.publisher.capture();
    EXPECT_EQ(after->commit_seq, before->commit_seq + 1);
    EXPECT_EQ(after->coverage, before->coverage);
    EXPECT_GT(after->stats.leaf_pages, 1U);
    EXPECT_LE(after->stats.leaf_pages, 4U);
    EXPECT_EQ(test.operation->commit_sequence(), after->commit_seq);
    EXPECT_EQ(before->mapping.get(0)->leaf->lookup("a").cell.value.to_string(), std::string(80, 'a'));
    EXPECT_EQ(test.publisher.work_metrics()->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Checkpoint)]
                                                                [static_cast<size_t>(TreeWorkCounter::SplitOperations)],
              1U);
}

TEST(CheckpointStructure, UnchangedTargetIsExplicitFailureAndNeverLoops)
{
    TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(test_read::page({
                                 {.key = "a", .value = "v"}
    }))
                    .ok());
    const auto before = test.publisher.capture();
    ASSERT_TRUE(test.prepare(before).ok());
    test.begin();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    EXPECT_EQ(test.terminal.code(), Code::kResourceExhausted);
    EXPECT_EQ(test.publisher.capture(), before);
}

TEST(CheckpointStructure, UnvisitedSuffixMayClassifyExactlyOnceWithoutSplitting)
{
    TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(test_read::page({
                                 {.key = "a", .value = "v"}
    }),
                             true)
                    .ok());
    const auto before = test.publisher.capture();
    ASSERT_TRUE(test.prepare(before).ok());
    test.begin();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    ASSERT_TRUE(test.terminal.ok()) << test.terminal.to_string();
    const auto after = test.publisher.capture();
    EXPECT_EQ(after->commit_seq, before->commit_seq + 1);
    EXPECT_EQ(after->stats.leaf_pages, before->stats.leaf_pages);
    EXPECT_FALSE(after->mapping.get(0)->metrics.split_candidate);
    EXPECT_TRUE(after->mapping.get(0)->metrics.page_size_known);
    EXPECT_TRUE(after->mapping.get(0)->metrics.kv_known);
    EXPECT_EQ(test.publisher.work_metrics()->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Checkpoint)]
                                                                [static_cast<size_t>(TreeWorkCounter::SplitOperations)],
              0U);
    test.attempt.reset();
    test.operation.reset();
    test.done.store(false, std::memory_order_release);
    ASSERT_TRUE(test.prepare(after).ok());
    test.begin();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    EXPECT_EQ(test.terminal.code(), Code::kResourceExhausted);
    EXPECT_EQ(test.publisher.capture(), after);
}

TEST(CheckpointStructure, ChangedViewRejectsBeforeReadingOrPublishing)
{
    TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(structural_input()).ok());
    ASSERT_TRUE(test.prepare(test.publisher.capture()).ok());
    ASSERT_TRUE(test.publish(test_read::page({
                                 {.key = "new", .value = "v"}
    }))
                    .ok());
    const auto current = test.publisher.capture();
    test.begin();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    EXPECT_EQ(test.terminal.code(), Code::kUnavailable);
    EXPECT_EQ(test.publisher.capture(), current);
    EXPECT_EQ(test.loader->calls, 0U);
}

TEST(CheckpointStructure, ColdBaseParticipatesInTheSuccessfulSplit)
{
    TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(structural_input(true)).ok());
    test.loader->frames.emplace(1, test_read::base({
                                       {.key = "b", .value = std::string(80, 'b')}
    }));
    ASSERT_TRUE(test.prepare(test.publisher.capture()).ok());
    test.begin();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    ASSERT_TRUE(test.terminal.ok()) << test.terminal.to_string();
    EXPECT_EQ(test.loader->calls, 1U);
    const auto view  = test.publisher.capture();
    uint64_t   count = 0;
    for (uint64_t pid = 0; pid < view->next_page_id; ++pid) {
        const auto page = view->mapping.get(pid);
        if (page && page->leaf) {
            auto found = page->leaf->lookup("b");
            if (found.state == LogicalLookupState::kValue) {
                EXPECT_EQ(found.cell.value.to_string(), std::string(80, 'b'));
                ++count;
            }
        }
    }
    EXPECT_EQ(count, 1U);
}

TEST(CheckpointStructure, ConcurrentPublicationDuringColdReadRejectsTheWholeWindow)
{
    TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(structural_input(true)).ok());
    test.loader->deferred = true;
    test.loader->frames.emplace(1, test_read::base({
                                       {.key = "b", .value = std::string(80, 'b')}
    }));
    ASSERT_TRUE(test.prepare(test.publisher.capture()).ok());
    test.begin();
    ASSERT_FALSE(test.done.load(std::memory_order_acquire));
    ASSERT_EQ(test.loader->pending.size(), 1U);
    ASSERT_TRUE(test.publish(test_read::page({
                                 {.key = "new", .value = "v"}
    }))
                    .ok());
    const auto current = test.publisher.capture();
    test.loader->finish();
    test.runtime->drain();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    EXPECT_EQ(test.terminal.code(), Code::kUnavailable);
    EXPECT_EQ(test.publisher.capture(), current);
}

TEST(CheckpointStructure, CancellationWaitsForAcceptedColdReadAndPreservesTheOldView)
{
    TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(structural_input(true)).ok());
    const auto before     = test.publisher.capture();
    test.loader->deferred = true;
    test.loader->frames.emplace(1, test_read::base({
                                       {.key = "b", .value = std::string(80, 'b')}
    }));
    ASSERT_TRUE(test.prepare(before).ok());
    test.begin();
    ASSERT_FALSE(test.done.load(std::memory_order_acquire));
    ASSERT_EQ(test.loader->pending.size(), 1U);
    test.operation->request_stop();
    test.runtime->drain();
    EXPECT_FALSE(test.done.load(std::memory_order_acquire));
    test.loader->finish();
    test.runtime->drain();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    EXPECT_EQ(test.terminal.code(), Code::kUnavailable);
    EXPECT_EQ(test.publisher.capture(), before);
    test.operation.reset();
    test.runtime->drain();
    EXPECT_EQ(test.account->snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kReclaim)], 0U);
}
} // namespace
} // namespace crowdb::tree
