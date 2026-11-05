// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "checkpoint_structure_test_support.h"
#include "snapshot_publication_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_snapshot_publication;

TEST(SnapshotChangeEffect, RealMaintenanceSplitRecordsEquivalentTopologyWithoutFullContent)
{
    test_structure::TestCheckpointStructure test;
    ASSERT_TRUE(test.publish(test_structure::structural_input()).ok());
    ASSERT_TRUE(checkpoint(test.publisher, CheckpointAction::Capture, 1).status.ok());
    ASSERT_TRUE(checkpoint(test.publisher, CheckpointAction::Commit, 1).status.ok());
    EXPECT_TRUE(test.publisher.capture()->snapshot_changes.empty());
    ASSERT_TRUE(test.prepare(test.publisher.capture()).ok());
    test.begin();
    ASSERT_TRUE(test.done.load(std::memory_order_acquire));
    ASSERT_TRUE(test.terminal.ok()) << test.terminal.to_string();
    const auto view = test.publisher.capture();
    EXPECT_GT(view->snapshot_changes.size(), 1);
    uint64_t next     = 0;
    bool     complete = false;
    while (!complete) {
        complete = view->snapshot_changes.visit_from(next, 4, nullptr, [](void *, const SnapshotPageChange &change) {
            EXPECT_EQ(change.effect, SnapshotChangeEffect::SourceStructure);
        });
    }
}

TEST(SnapshotChangeEffect, CapturedStructureCannotEraseLaterContentWhenTheOlderSnapshotCommits)
{
    PublicationCoordinator publisher;
    auto                   first = leaf(1);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .replacement = first}}, .root_page_id = 0}).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
    auto second = leaf(2);
    ASSERT_TRUE(publish(publisher, {.pages           = {{.page_id = 0, .expected = first, .replacement = second}},
                                    .snapshot_effect = SnapshotChangeEffect::SourceStructure})
                    .status.ok());
    const auto captured = checkpoint(publisher, CheckpointAction::Capture, 2);
    ASSERT_TRUE(captured.status.ok());
    auto third = leaf(3);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .expected = second, .replacement = third}}}).status.ok());
    auto fourth = leaf(4);
    ASSERT_TRUE(publish(publisher, {.pages           = {{.page_id = 0, .expected = third, .replacement = fourth}},
                                    .snapshot_effect = SnapshotChangeEffect::SourceStructure})
                    .status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 2).status.ok());
    const auto view = publisher.capture();
    ASSERT_TRUE(view->snapshot_changes.get(0));
    EXPECT_EQ(view->snapshot_changes.get(0)->effect,
              SnapshotChangeEffect::FullContent | SnapshotChangeEffect::SourceStructure);
    EXPECT_EQ(captured.checkpoint_view->snapshot_changes.get(0)->effect, SnapshotChangeEffect::SourceStructure);
}

TEST(SnapshotChangeEffect, UnfencedAcceptedDeltaCannotChangeThePublishedState)
{
    PublicationCoordinator publisher;
    auto                   first = leaf(1);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .replacement = first}}, .root_page_id = 0}).status.ok());
    const auto before = publisher.capture();
    const auto reject = [&](SnapshotChangeEffect effect) {
        return publish(publisher, {.pages           = {{.page_id = 0, .expected = first, .replacement = leaf(2)}},
                                   .snapshot_effect = effect})
            .status.code();
    };
    EXPECT_EQ(reject(SnapshotChangeEffect::AcceptedPrefix), Code::kInvalidArgument);
    EXPECT_EQ(publisher.capture(), before);
}
} // namespace
} // namespace crowdb::tree
