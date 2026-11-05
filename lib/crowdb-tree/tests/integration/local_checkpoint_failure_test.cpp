// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_checkpoint_test_support.h"

namespace crowdb::tree
{
using namespace test_write;

TEST(LocalCheckpoint, CancellationDrainsMetadataButFinishesAnAttemptedAnchor)
{
    for (const size_t pause : {1U, 4U}) {
        TestCheckpoint test;
        ASSERT_TRUE(test.prepare().ok());
        test.store->defer_at = pause;
        Completion done;
        auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        test.source.runtime->drain();
        ASSERT_TRUE(test.store->pending);
        test.operation->request_stop();
        test.source.runtime->drain();
        EXPECT_EQ(done.stopped.load(), 0U);
        EXPECT_EQ(done.values.load(), 0U);
        EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
        test.store->finish();
        test.source.runtime->drain();
        EXPECT_EQ(done.stopped.load(), pause == 1 ? 1U : 0U);
        EXPECT_EQ(done.values.load(), pause == 4 ? 1U : 0U);
        EXPECT_EQ(test.registry->durable_coverage(), pause == 4 ? 10U : 0U);
        EXPECT_FALSE(test.registry->needs_recovery());
        EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    }
}

TEST(LocalCheckpoint, UnknownSubmissionRetainsBufferAndExtentsUntilLateCallback)
{
    for (const size_t unknown : {1U, 4U}) {
        TestCheckpoint test;
        ASSERT_TRUE(test.prepare().ok());
        test.store->throw_at = unknown;
        Completion done;
        auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        test.source.runtime->drain();
        ASSERT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(test.registry->durable_coverage(), 0U);
        EXPECT_EQ(test.registry->needs_recovery(), unknown == 4);
        test.operation.reset();
        test.source.operation.reset();
        EXPECT_EQ(test.source.ledger->stats().protected_ranges, unknown == 1 ? 2U : 3U);
        EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
        auto duplicate = test.store->pending;
        test.store->finish();
        duplicate.complete(Status::Ok());
        duplicate = {};
        test.source.runtime->drain();
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(test.store->events.size(), unknown);
        EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
        EXPECT_EQ(test.source.ledger->stats().protected_ranges, unknown == 1 ? 0U : 3U);
        test.registry.reset();
        test.source.ledger.reset();
        test.source.credits.seal();
        EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    }
}

TEST(LocalCheckpoint, InlineSuccessFollowedBySubmissionFailureCannotAdvance)
{
    TestCheckpoint test;
    ASSERT_TRUE(test.prepare().ok());
    test.store->inline_throw_at = 1;
    Completion done;
    test.run(&done);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.store->events.size(), 1U);
    EXPECT_FALSE(test.registry->needs_recovery());
    EXPECT_EQ(test.registry->snapshot_sequence(), 0U);
}

TEST(LocalCheckpoint, ConcurrentCommitAndCoverageRegressionAreRejected)
{
    TestCheckpoint test;
    auto           pages = test.materialize();
    ASSERT_TRUE(test.prepare(pages).ok());
    test.store->defer_at = 1;
    auto       first     = test.operation;
    Completion first_done;
    auto       connected = stdexec::connect(first->sender(), Receiver{.done = &first_done});
    stdexec::start(connected);
    test.source.runtime->drain();
    ASSERT_TRUE(test.prepare(pages).ok());
    Completion rejected;
    test.run(&rejected);
    EXPECT_EQ(rejected.errors.load(), 1U);
    EXPECT_EQ(test.store->events.size(), 1U);
    test.store->finish();
    test.source.runtime->drain();
    ASSERT_EQ(first_done.values.load(), 1U);
    auto older      = std::make_shared<CapturedDurablePages>(*pages);
    older->coverage = 9;
    ASSERT_TRUE(test.prepare(older).ok());
    Completion regressed;
    test.run(&regressed);
    EXPECT_EQ(regressed.errors.load(), 1U);
    EXPECT_EQ(test.registry->durable_coverage(), 10U);
    EXPECT_EQ(test.store->events.size(), 5U);
}

TEST(LocalCheckpoint, AllocationFailureAfterAnchorKeepsRecoveryProtection)
{
    TestCheckpoint test;
    ASSERT_TRUE(test.prepare().ok());
    test.store->defer_at = 4;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.source.runtime->drain();
    ASSERT_TRUE(test.store->pending);
    auto            retained_callback = test.store->pending;
    AllocationLease pressure(test.source.credits.credits.bytes, test.source.credits.credits.bytes->available());
    test.store->finish();
    test.source.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kResourceExhausted);
    EXPECT_TRUE(test.registry->needs_recovery());
    EXPECT_EQ(test.registry->snapshot_sequence(), 0U);
    EXPECT_EQ(test.source.ledger->stats().protected_ranges, 3U);
    EXPECT_EQ(test.source.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    EXPECT_TRUE(retained_callback);
}
} // namespace crowdb::tree
