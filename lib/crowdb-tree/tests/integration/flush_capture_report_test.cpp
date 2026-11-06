// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
class FlushCaptureReport : public testing::Test
{
  protected:
    void SetUp() override
    {
        Config options;
        ASSERT_TRUE(TreeRuntime::create({}, &options.runtime).ok());
        options.memtable_flush_bytes   = SIZE_MAX;
        options.memtable_flush_entries = UINT32_MAX;
        tree                           = std::make_unique<Crowdbtree>(options);
        core                           = TreeCoreAccess::get(*tree);
        ASSERT_TRUE(tree->put("key", "value").ok());
    }

    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(FlushCaptureReport, CoveredCaptureKeepsItsFrontierAndNormalCompletion)
{
    ASSERT_TRUE(tree->flush().ok());
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(core->prepare_flush(&operation).ok());
    ASSERT_TRUE(tree->put("later", "successor").ok());
    FlushResult capture;
    ASSERT_TRUE(operation->wait_capture(&capture).ok());
    ASSERT_TRUE(capture.handoff_status.ok());
    EXPECT_EQ(capture.frontier, 1U);
    EXPECT_EQ(operation->frozen_sources().frontier, capture.frontier);
    EXPECT_EQ(operation->frozen_sources().base->coverage, capture.frontier);
    ASSERT_TRUE(operation->wait().ok());
    EXPECT_EQ(operation->result().phase, FlushPhase::kComplete);
    EXPECT_FALSE(operation->result().handoff_advanced);
    EXPECT_EQ(core->capture()->coverage, 1U);
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(core->capture()->coverage, 2U);
}

TEST_F(FlushCaptureReport, CapturingACompletedHandoffDoesNotCreditItAgain)
{
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(core->prepare_flush(&operation).ok());
    ASSERT_TRUE(operation->wait().ok());
    EXPECT_TRUE(operation->result().handoff_advanced);
    FlushResult capture;
    ASSERT_TRUE(operation->wait_capture(&capture).ok());
    EXPECT_EQ(capture.frontier, 1U);
    ASSERT_TRUE(operation->wait().ok());
    EXPECT_EQ(operation->result().phase, FlushPhase::kComplete);
    EXPECT_FALSE(operation->result().handoff_advanced);
}

TEST_F(FlushCaptureReport, RepeatedCapturePreservesTheOriginalPairAfterHandoff)
{
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(core->prepare_flush(&operation).ok());
    FlushResult capture;
    ASSERT_TRUE(operation->wait_capture(&capture).ok());
    const auto frozen = operation->frozen_sources();
    ASSERT_TRUE(frozen.catalog);
    ASSERT_TRUE(operation->wait().ok());
    EXPECT_TRUE(operation->result().handoff_advanced);
    ASSERT_TRUE(operation->wait_capture(&capture).ok());
    ASSERT_TRUE(operation->wait().ok());
    EXPECT_EQ(operation->result().phase, FlushPhase::kComplete);
    EXPECT_FALSE(operation->result().handoff_advanced);
    EXPECT_EQ(operation->frozen_sources().catalog, frozen.catalog);
    EXPECT_EQ(operation->frozen_sources().base, frozen.base);
    EXPECT_EQ(operation->frozen_sources().frontier, frozen.frontier);
}

TEST_F(FlushCaptureReport, CoveredFlushReportsRemainingDirtyPages)
{
    ASSERT_TRUE(tree->flush().ok());
    const auto remaining = core->capture()->stats.dirty_pages;
    ASSERT_GT(remaining, 0U);
    std::shared_ptr<FlushOperation> operation;
    ASSERT_TRUE(core->prepare_flush(&operation).ok());
    ASSERT_TRUE(operation->wait().ok());
    EXPECT_EQ(operation->result().deferred_pages, remaining);
    EXPECT_FALSE(operation->result().handoff_advanced);
}
} // namespace
} // namespace crowdb::tree
