// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write.h"
#include "snapshot/snapshot_baseline.h"
#include "snapshot_publication_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_snapshot_publication;

TEST(SnapshotPublication, CapturedDeletionSurvivesCommitOfTheOlderView)
{
    PublicationCoordinator publisher;
    auto                   first = leaf(1);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .replacement = first}}, .root_page_id = 0}).status.ok());
    auto captured = checkpoint(publisher, CheckpointAction::Capture, 1);
    ASSERT_TRUE(captured.status.ok());
    ASSERT_TRUE(publish(publisher,
                        {.pages = {{.page_id = 0, .expected = first, .replacement = {}}}, .root_page_id = UINT64_MAX})
                    .status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
    const auto &changes = publisher.capture()->snapshot_changes;
    ASSERT_EQ(changes.size(), 1U);
    EXPECT_TRUE(changes.get(0)->before.present);
    EXPECT_EQ(changes.get(0)->before.content, 1U);
    EXPECT_FALSE(changes.get(0)->after.present);
    EXPECT_TRUE(captured.checkpoint_view->snapshot_changes.get(0)->after.present);
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
    auto second = checkpoint(publisher, CheckpointAction::Capture, 2);
    ASSERT_TRUE(second.status.ok());
    EXPECT_FALSE(second.checkpoint_view->snapshot_changes.get(0)->after.present);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 2).status.ok());
    EXPECT_TRUE(publisher.capture()->snapshot_changes.empty());
}

TEST(SnapshotPublication, AbortRetainsLatestChangesAndTheNextCaptureSharesTheirRoot)
{
    PublicationCoordinator publisher;
    auto                   first = leaf(1);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .replacement = first}}, .root_page_id = 0}).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    auto newer = leaf(2);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .expected = first, .replacement = newer}}}).status.ok());
    auto before_abort = publisher.capture();
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Abort, 1).status.ok());
    auto after_abort = publisher.capture();
    EXPECT_TRUE(after_abort->snapshot_changes.same_root(before_abort->snapshot_changes));
    EXPECT_TRUE(after_abort->snapshot_changes_since_capture.empty());
    EXPECT_EQ(after_abort->snapshot_changes.get(0)->after.content, 2U);
    EXPECT_FALSE(after_abort->snapshot_changes.get(0)->before.present);
    auto captured = checkpoint(publisher, CheckpointAction::Capture, 2);
    ASSERT_TRUE(captured.status.ok());
    EXPECT_TRUE(captured.checkpoint_view->snapshot_changes.same_root(after_abort->snapshot_changes));
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 2).status.ok());
    EXPECT_TRUE(publisher.capture()->snapshot_changes.empty());
}

TEST(SnapshotPublication, CompactChangesDoNotRetainOldPagesAndRepresentationChangesStayClean)
{
    PublicationCoordinator       publisher;
    auto                         first = leaf(1);
    std::weak_ptr<PublishedPage> old   = first;
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .replacement = first}}, .root_page_id = 0}).status.ok());
    auto second = leaf(2);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .expected = first, .replacement = second}}}).status.ok());
    first.reset();
    publisher.reclaim();
    EXPECT_TRUE(old.expired());
    auto selected    = publisher.capture()->snapshot_changes;
    auto represented = leaf(2);
    ASSERT_TRUE(
        publish(publisher, {.pages = {{.page_id = 0, .expected = second, .replacement = represented}}}).status.ok());
    EXPECT_TRUE(publisher.capture()->snapshot_changes.same_root(selected));
}

TEST(SnapshotPublication, BaselineMustBelongToTheExactCapturedFrontierAndToken)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    auto baseline               = std::make_shared<SnapshotBaseline>();
    auto pages                  = std::make_shared<CapturedDurablePages>(nullptr);
    pages->generation           = 1;
    baseline->generation        = 1;
    baseline->capture_token     = 1;
    baseline->snapshot_sequence = 1;
    baseline->backend_id        = 17;
    baseline->pages             = pages;
    auto wrong                  = std::make_shared<SnapshotBaseline>(*baseline);
    wrong->capture_token        = 2;
    EXPECT_EQ(publish(publisher,
                      {
                          .checkpoint =
                              CheckpointPublication{.action = CheckpointAction::Commit, .token = 1, .baseline = wrong}
    })
                  .status.code(),
              Code::kInvalidArgument);
    EXPECT_FALSE(publisher.capture()->snapshot_baseline);
    ASSERT_TRUE(
        publish(publisher,
                {
                    .checkpoint =
                        CheckpointPublication{.action = CheckpointAction::Commit, .token = 1, .baseline = baseline}
    })
            .status.ok());
    EXPECT_EQ(publisher.capture()->snapshot_baseline, baseline);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 2).status.ok());
    EXPECT_EQ(
        publish(publisher,
                {
                    .checkpoint =
                        CheckpointPublication{.action = CheckpointAction::Abort, .token = 2, .baseline = baseline}
    })
            .status.code(),
        Code::kInvalidArgument);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Abort, 2).status.ok());
    EXPECT_EQ(publisher.capture()->snapshot_baseline, baseline);
}

TEST(SnapshotPublication, UnchangedSequenceRequiresTheSameProtectedBaseline)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    auto pages                  = std::make_shared<CapturedDurablePages>(nullptr);
    pages->generation           = 1;
    auto baseline               = std::make_shared<SnapshotBaseline>();
    baseline->generation        = 1;
    baseline->capture_token     = 1;
    baseline->snapshot_sequence = 1;
    baseline->backend_id        = 17;
    baseline->pages             = pages;
    ASSERT_TRUE(
        publish(publisher,
                {
                    .checkpoint =
                        CheckpointPublication{.action = CheckpointAction::Commit, .token = 1, .baseline = baseline}
    })
            .status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 2).status.ok());
    auto reused           = std::make_shared<SnapshotBaseline>(*baseline);
    reused->capture_token = 2;
    for (const auto variant : {0, 1, 2}) {
        auto wrong = std::make_shared<SnapshotBaseline>(*reused);
        if (variant == 0) {
            wrong->pages = std::make_shared<CapturedDurablePages>(*pages);
        }
        else if (variant == 1) {
            ++wrong->wal_offset;
        }
        else {
            ++wrong->backend_id;
        }
        EXPECT_EQ(
            publish(publisher,
                    {
                        .checkpoint =
                            CheckpointPublication{.action = CheckpointAction::Commit, .token = 2, .baseline = wrong}
        })
                .status.code(),
            Code::kInvalidArgument);
        EXPECT_EQ(publisher.capture()->snapshot_baseline, baseline);
    }
    ASSERT_TRUE(
        publish(publisher,
                {
                    .checkpoint =
                        CheckpointPublication{.action = CheckpointAction::Commit, .token = 2, .baseline = reused}
    })
            .status.ok());
    EXPECT_EQ(publisher.capture()->snapshot_baseline, reused);
    EXPECT_EQ(reused->snapshot_sequence, 1U);
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(SnapshotPublication, FailureAfterPreparingChangesPublishesNeitherSelectionNorPendingState)
{
    PublicationCoordinator publisher;
    auto                   first = leaf(1);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .replacement = first}}, .root_page_id = 0}).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    const auto before = publisher.capture();
    publisher.set_before_root_for_tests([] { throw std::bad_alloc(); });
    EXPECT_EQ(publish(publisher, {.pages = {{.page_id = 0, .expected = first, .replacement = leaf(2)}}}).status.code(),
              Code::kResourceExhausted);
    publisher.set_before_root_for_tests({});
    EXPECT_EQ(publisher.capture(), before);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
    EXPECT_TRUE(publisher.capture()->snapshot_changes.empty());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
}
#endif

TEST(SnapshotPublication, PersistentMaintenanceSelectionUsesRoutingInsteadOfReclaimWorkingCredit)
{
    using detail::BudgetClass;
    using detail::ResourceBudget;
    constexpr auto         routing = static_cast<size_t>(BudgetClass::kRouting);
    constexpr auto         reclaim = static_cast<size_t>(BudgetClass::kReclaim);
    ResourceBudget::Limits limits{};
    limits[routing] = PublicationCoordinator::snapshot_change_reserve_bytes();
    limits[reclaim] = 1024 * 1024;
    ResourceBudget budget(limits, 1);
    auto           account       = budget.make_account(limits);
    auto           routing_grant = ResourceBudget::try_reserve(account, BudgetClass::kRouting, limits[routing]);
    auto           reclaim_grant = ResourceBudget::try_reserve(account, BudgetClass::kReclaim, limits[reclaim]);
    if (!routing_grant.has_value() || !reclaim_grant.has_value()) {
        ADD_FAILURE() << "separate maintenance partitions unavailable";
        return;
    }
    auto                   snapshot = std::make_shared<detail::AllocationReservation>(std::move(*routing_grant));
    auto                   working  = std::make_shared<detail::AllocationReservation>(std::move(*reclaim_grant));
    PublicationCoordinator publisher;
    auto                   page = leaf(1);
    ASSERT_TRUE(publish(publisher, {.pages             = {{.page_id = 0, .replacement = page}},
                                    .root_page_id      = 0,
                                    .metadata          = working,
                                    .snapshot_metadata = snapshot})
                    .status.ok());
    snapshot->seal();
    working->seal();
    EXPECT_GT(budget.snapshot().allocated[routing], 0U);
    const auto selected = checkpoint(publisher, CheckpointAction::Capture, 1);
    ASSERT_TRUE(selected.status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
    publisher.reclaim();
    // The independent captured selection, rather than a live page or its
    // Reclaim reservation, now owns the remaining Routing allocation.
    EXPECT_TRUE(publisher.capture()->snapshot_changes.empty());
    EXPECT_EQ(selected.checkpoint_view->snapshot_changes.size(), 1U);
    EXPECT_GT(budget.snapshot().allocated[routing], 0U);
}
} // namespace
} // namespace crowdb::tree
