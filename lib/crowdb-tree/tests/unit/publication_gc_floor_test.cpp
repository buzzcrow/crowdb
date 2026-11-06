// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_state.h"
#include "btree/tree_core.h"
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
PublicationResult publish_floor(PublicationCoordinator &publisher, uint64_t sequence,
                                std::optional<uint64_t>              coverage   = {},
                                std::optional<CheckpointPublication> checkpoint = {})
{
    const auto current          = publisher.capture();
    auto       request          = std::make_shared<PublicationRequest>();
    request->transaction.record = std::make_shared<InstallRecord>(
        InstallToken{.generation = current->generation, .batch = 1, .sequence = sequence});
    request->transaction.expected_coverage = current->coverage;
    if (coverage) {
        auto sources                          = std::make_shared<PublishedSources>();
        sources->floor                        = *coverage;
        request->transaction.coverage         = coverage;
        request->transaction.expected_sources = current->sources;
        request->transaction.sources          = std::move(sources);
    }
    request->transaction.checkpoint = checkpoint;
    PublicationResult result;
    request->on_done = [&](PublicationResult value) { result = std::move(value); };
    EXPECT_TRUE(publisher.try_submit(request));
    return result;
}

TEST(PublicationGcFloor, ClampsToCoverageAndLeavesCapturedViewsUnchanged)
{
    PublicationCoordinator publisher;
    const auto             initial = publisher.capture();
    publisher.request_gc_floor(4);
    ASSERT_TRUE(publish_floor(publisher, 1, 2).status.ok());
    const auto captured = publisher.capture();
    EXPECT_EQ(captured->gc_floor, 2U);
    publisher.request_gc_floor(1);
    ASSERT_TRUE(publish_floor(publisher, 2, 6).status.ok());
    EXPECT_EQ(publisher.capture()->gc_floor, 4U);
    publisher.request_gc_floor(10);
    ASSERT_TRUE(publish_floor(publisher, 3).status.ok());
    EXPECT_EQ(publisher.capture()->gc_floor, 6U);
    EXPECT_EQ(captured->gc_floor, 2U);
    EXPECT_EQ(initial->gc_floor, 0U);
}

TEST(PublicationGcFloor, FloorOnlyCommitDoesNotCreatePendingLogicalChanges)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish_floor(publisher, 1, 10).status.ok());
    ASSERT_TRUE(publish_floor(publisher, 2, {}, CheckpointPublication{.action = CheckpointAction::Capture, .token = 1})
                    .status.ok());
    ASSERT_TRUE(publish_floor(publisher, 3, {}, CheckpointPublication{.action = CheckpointAction::Commit, .token = 1})
                    .status.ok());
    const auto before = publisher.capture();
    publisher.request_gc_floor(8);
    ASSERT_TRUE(publish_floor(publisher, 4).status.ok());
    const auto after = publisher.capture();
    EXPECT_EQ(after->gc_floor, 8U);
    EXPECT_EQ(after->stats.checkpoint_pending_pages, before->stats.checkpoint_pending_pages);
    EXPECT_EQ(after->stats.checkpoint_mapping_pending, before->stats.checkpoint_mapping_pending);
    publisher.request_gc_floor(9);
    const auto captured =
        publish_floor(publisher, 5, {}, CheckpointPublication{.action = CheckpointAction::Capture, .token = 2});
    ASSERT_TRUE(captured.status.ok());
    ASSERT_TRUE(captured.checkpoint_view);
    EXPECT_EQ(captured.checkpoint_view->gc_floor, 9U);
    EXPECT_EQ(after->gc_floor, 8U);
}

TEST(PublicationGcFloor, PublicWatermarkUsesBothLimitsAtFiniteHandoff)
{
    Config             config;
    TreeRuntimeOptions runtime;
    runtime.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(runtime, &config.runtime).ok());
    Crowdbtree tree(config);
    ASSERT_TRUE(tree.apply(1, {}).ok());
    ASSERT_TRUE(tree.apply(2, {}).ok());
    auto *const core = TreeCoreAccess::get(tree);
    const auto  old  = core->capture();
    tree.set_gc_watermark(10, 1);
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(core->capture()->coverage, 2U);
    EXPECT_EQ(core->capture()->gc_floor, 1U);
    EXPECT_EQ(old->gc_floor, 0U);
    tree.set_gc_watermark(1, 10);
    EXPECT_EQ(tree.gc_watermark(), 1U);
    tree.set_gc_watermark(10, 10);
    ASSERT_TRUE(tree.apply(3, {}).ok());
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(core->capture()->gc_floor, 3U);
}
} // namespace
} // namespace crowdb::tree
