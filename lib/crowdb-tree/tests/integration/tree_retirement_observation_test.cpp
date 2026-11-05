// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(TreeRetirementObservation, LogicalReplacementStartsBeforeOldViewIsReleased)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    auto       old  = core->capture();
    ASSERT_TRUE(tree.put("key", "value").ok());
    const auto status = tree.flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    core->binding()->drain();
    const auto work      = core->binding()->work_metrics();
    const auto resources = core->binding()->resource_metrics();
    const auto held      = work->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_GT(held.inflight, 0U);
    const auto held_age = work->ages(tree_metrics_now_ns())[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_EQ(held_age.inflight, held.inflight);
    EXPECT_EQ(held_age.tracked, held.inflight);
    EXPECT_TRUE(held_age.complete);
    EXPECT_GT(held_age.oldest_age_ns, 0U);
    EXPECT_GT(resources->snapshot().retired_bytes, 0U);
    old.reset();
    core->binding()->drain();
    const auto released = work->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_EQ(released.inflight, 0U);
    const auto released_age = work->ages(tree_metrics_now_ns())[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_EQ(released_age.tracked, 0U);
    EXPECT_EQ(released_age.untracked, 0U);
    EXPECT_EQ(released_age.oldest_age_ns, 0U);
    EXPECT_TRUE(released_age.complete);
    EXPECT_GT(released.count, held.count);
    EXPECT_EQ(resources->snapshot().retired_bytes, 0U);
}

TEST(TreeRetirementObservation, ResourceMetricsOwnerHasNoWeakControlCycle)
{
    std::shared_ptr<TreeRuntime> runtime;
    ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
    const auto                          baseline = runtime->snapshot();
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    auto account   = binding->account();
    auto resources = binding->resource_metrics();
    EXPECT_EQ(account->resource_metrics(), resources);
    binding.reset();
    EXPECT_GT(account->snapshot().allocated[1], 0U);
    resources.reset();
    EXPECT_EQ(account->snapshot().allocated[1], 0U);
    account.reset();
    EXPECT_EQ(runtime->snapshot().allocated.routing_bytes, baseline.allocated.routing_bytes);
    EXPECT_EQ(runtime->snapshot().reserved.routing_bytes, baseline.reserved.routing_bytes);
}
} // namespace
} // namespace crowdb::tree
