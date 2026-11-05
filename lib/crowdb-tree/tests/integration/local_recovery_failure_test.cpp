// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_recovery_test_support.h"

namespace crowdb::tree
{
using namespace test_write;

TEST(LocalRecoveryFailure, CancellationDrainsAcceptedReadBeforeReleasingItsBuffer)
{
    for (const size_t pause : {1U, 2U, 3U, 4U}) {
        TestRecovery test;
        test.checkpoint();
        ASSERT_TRUE(test.prepare().ok());
        test.store->defer_at = pause;
        Completion done;
        auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        test.original.source.runtime->drain();
        ASSERT_TRUE(test.store->pending);
        test.operation->request_stop();
        test.original.source.runtime->drain();
        EXPECT_EQ(done.stopped.load(), 0U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
        test.store->finish();
        test.original.source.runtime->drain();
        EXPECT_EQ(done.stopped.load(), 1U);
        EXPECT_EQ(done.errors.load(), 0U);
        EXPECT_EQ(test.store->reads, pause);
        EXPECT_FALSE(test.operation->result().ledger);
        EXPECT_FALSE(test.operation->result().registry);
        test.operation.reset();
        test.credits.seal();
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    }
}

TEST(LocalRecoveryFailure, UnknownAcceptanceRetainsWindowUntilLateDuplicateCallbacksEnd)
{
    for (const size_t unknown : {1U, 4U}) {
        TestRecovery test;
        test.checkpoint();
        ASSERT_TRUE(test.prepare().ok());
        test.store->throw_at = unknown;
        Completion done;
        auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        test.original.source.runtime->drain();
        ASSERT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), Code::kIoError);
        EXPECT_FALSE(test.operation->result().ledger);
        test.operation.reset();
        test.credits.seal();
        EXPECT_GE(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 2U * 1024 * 1024);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 1U);
        auto duplicate = test.store->pending;
        test.store->finish();
        duplicate.complete(Status::corruption());
        duplicate = {};
        test.original.source.runtime->drain();
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(test.store->reads, unknown);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
        EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    }
}

TEST(LocalRecoveryFailure, InlineReadFollowedBySubmissionExceptionCannotAdvance)
{
    TestRecovery test;
    test.checkpoint();
    ASSERT_TRUE(test.prepare().ok());
    test.store->inline_throw_at = 1;
    Completion done;
    test.run(&done);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kIoError);
    EXPECT_EQ(test.store->reads, 1U);
    EXPECT_FALSE(test.operation->result().latest);
    EXPECT_FALSE(test.operation->result().registry);
}

TEST(LocalRecoveryFailure, ReadErrorsNeverTurnIntoAnEmptyOrOlderRecovery)
{
    for (const size_t failure : {1U, 2U, 3U, 4U}) {
        TestRecovery test;
        test.checkpoint();
        ASSERT_TRUE(test.prepare().ok());
        test.store->fail_at = failure;
        Completion done;
        test.run(&done);
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.status.code(), Code::kIoError);
        EXPECT_EQ(test.store->reads, failure);
        EXPECT_FALSE(test.operation->result().latest);
        EXPECT_FALSE(test.operation->result().registry);
    }
}

TEST(LocalRecoveryFailure, ClosingRuntimeDrainsPendingReadAndRejectsFurtherRecovery)
{
    TestRecovery test;
    test.checkpoint();
    ASSERT_TRUE(test.prepare().ok());
    test.store->defer_at = 1;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.original.source.runtime->drain();
    ASSERT_TRUE(test.store->pending);
    test.original.source.runtime->close();
    test.operation->request_stop();
    EXPECT_EQ(done.stopped.load(), 0U);
    test.store->finish();
    test.original.source.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_EQ(test.store->reads, 1U);
    test.operation.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
}

TEST(LocalRecoveryFailure, BudgetExhaustionAfterReadCannotPublishPartialOwnership)
{
    TestRecovery test;
    test.checkpoint();
    ASSERT_TRUE(test.prepare().ok());
    test.store->defer_at = 1;
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.original.source.runtime->drain();
    ASSERT_TRUE(test.store->pending);
    AllocationLease pressure(test.credits.credits.bytes, test.credits.credits.bytes->available());
    test.store->finish();
    test.original.source.runtime->drain();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(done.status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(test.operation->result().ledger);
    EXPECT_FALSE(test.operation->result().registry);
    EXPECT_EQ(test.store->reads, 1U);
    pressure.reset();
    test.operation.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace crowdb::tree
