// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/retirement_queue.h"
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(TreeCoreRetirement, PublishedOwnersRemainValidAfterTreeAndExecutorDestruction)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.frame_bytes                 = 4096;
    config.leaf_split_bytes            = 3000;
    std::weak_ptr<TreeRuntime> runtime = config.runtime;
    auto                       tree    = std::make_unique<Crowdbtree>(config);
    auto                      *core    = TreeCoreAccess::get(*tree);
    auto                       account = core->binding()->account();
    for (size_t index = 0; index < 1024; ++index) {
        ASSERT_TRUE(tree->put(std::to_string(index), "retained value").ok());
    }
    auto    view = core->capture();
    GetView value;
    ASSERT_TRUE(core->get("512", &value).ok());
    ASSERT_TRUE(value.found());
    tree.reset();
    EXPECT_EQ(value.value().to_string(), "retained value");
    EXPECT_NE(view->sources->tables.back()->find("512"), nullptr);
    config.runtime.reset();
    EXPECT_TRUE(runtime.expired());
    view.reset();
    value = {};
    account->drain_retirements();
    const auto after = account->snapshot();
    for (size_t i = 0; i < detail::ResourceBudget::kClasses; ++i) {
        EXPECT_EQ(after.allocated[i], 0U) << i;
        EXPECT_EQ(after.reserved[i], 0U) << i;
    }
    for (const auto physical : after.physical) {
        EXPECT_EQ(physical, 0U);
    }
}

TEST(TreeCoreRetirement, OldPublicationRootsAreCollectedWhileTreeRemainsOpen)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree                              tree(config);
    auto                                   *core    = TreeCoreAccess::get(tree);
    auto                                    old     = core->capture();
    std::weak_ptr<const PublishedTreeState> retired = old;
    old.reset();
    ASSERT_TRUE(tree.put("key", "value").ok());
    const auto flushed = tree.flush();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    core->binding()->drain();
    EXPECT_TRUE(retired.expired());
    const auto work = core->binding()->lifecycle_budget()->retirement_queue()->snapshot();
    EXPECT_GT(work.enqueued, 0U);
    EXPECT_EQ(work.enqueued, work.freed);
    EXPECT_LE(work.max_step_nodes, detail::RetirementQueue::kNodesPerStep);
    const auto totals = core->publisher().work_metrics()->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_GT(totals.attempts, 0U);
    EXPECT_EQ(totals.inflight, 0U);
}
} // namespace
} // namespace crowdb::tree
