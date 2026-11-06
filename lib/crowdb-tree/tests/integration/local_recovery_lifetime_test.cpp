// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_recovery_test_support.h"

namespace crowdb::tree
{
using namespace test_write;

TEST(LocalRecovery, CompletedIoCallbackDoesNotRetainRecoveredCheckpointMetadata)
{
    TestRecovery test;
    test.checkpoint();
    ASSERT_TRUE(test.prepare().ok());
    test.store->retain_completed = true;
    Completion done;
    test.run(&done);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    auto ledger = test.operation->result().ledger;
    auto pages  = test.operation->result().latest;
    ASSERT_TRUE(ledger);
    ASSERT_TRUE(pages);
    ASSERT_TRUE(test.store->retained_completion.owner);
    EXPECT_EQ(ledger->stats().protected_ranges, pages->frames.size() + 2);
    test.operation.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, pages->frames.size());
    pages.reset();
    EXPECT_EQ(ledger->stats().protected_ranges, 0U);
    test.store->retained_completion = {};
    ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace crowdb::tree
