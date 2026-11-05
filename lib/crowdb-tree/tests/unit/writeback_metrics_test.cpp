// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/writeback_metrics.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(WritebackMetrics, TerminalCleanupYieldsAndNeverCompletesOnePageTwice)
{
    auto             metrics = std::make_shared<TreeWorkMetrics>();
    WritebackMetrics writes(metrics, {}, 130);
    for (uint64_t pid = 0; pid < 130; ++pid) {
        ASSERT_TRUE(writes.begin(pid).ok());
        ASSERT_TRUE(writes.begin(pid).ok());
    }
    EXPECT_EQ(writes.begin(130).code(), Code::kResourceExhausted);
    writes.finish(0, TreeOutcome::Success);
    writes.finish(0, TreeOutcome::Error);
    EXPECT_FALSE(writes.finish_some(TreeOutcome::Error));
    auto sample = metrics->totals()[static_cast<size_t>(TreeStage::PageWritebackPipeline)];
    EXPECT_EQ(sample.attempts, 130U);
    EXPECT_EQ(sample.count, 65U);
    EXPECT_EQ(sample.inflight, 65U);
    EXPECT_EQ(sample.outcomes[static_cast<size_t>(TreeOutcome::Success)], 1U);
    EXPECT_EQ(sample.outcomes[static_cast<size_t>(TreeOutcome::Error)], 64U);
    EXPECT_FALSE(writes.retire_step());
    EXPECT_TRUE(writes.retire_step());
    sample = metrics->totals()[static_cast<size_t>(TreeStage::PageWritebackPipeline)];
    EXPECT_EQ(sample.count, 130U);
    EXPECT_EQ(sample.inflight, 0U);
    EXPECT_EQ(sample.outcomes[static_cast<size_t>(TreeOutcome::Cancelled)], 65U);
}
} // namespace
} // namespace crowdb::tree
