// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/tree_state_reconcile.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
class PublicationReconcile : public ::testing::Test
{
  protected:
    PublicationCoordinator publisher{8, 1};

    PublicationResult publish(PublicationTransaction transaction)
    {
        transaction.record   = std::make_shared<InstallRecord>(InstallToken{.generation = 1});
        auto request         = std::make_shared<PublicationRequest>();
        request->transaction = std::move(transaction);
        PublicationResult result;
        bool              completed = false;
        request->on_done            = [&](PublicationResult value) {
            result    = std::move(value);
            completed = true;
        };
        EXPECT_TRUE(publisher.try_submit(request));
        EXPECT_TRUE(completed);
        return result;
    }

    Status advance(const std::shared_ptr<TreeStateReconciliation> &audit, size_t slots = 1)
    {
        auto request                  = std::make_shared<PublicationRequest>();
        request->reconciliation       = audit;
        request->reconciliation_slots = slots;
        Status status;
        bool   completed = false;
        request->on_done = [&](PublicationResult result) {
            status    = result.status;
            completed = true;
        };
        EXPECT_TRUE(publisher.try_submit(request));
        EXPECT_TRUE(completed);
        return status;
    }

    void reconcile()
    {
        const auto view  = publisher.capture();
        auto       audit = std::make_shared<TreeStateReconciliation>(view);
        for (size_t i = 0; i < 1024 && !audit->complete(); ++i) {
            const auto before = audit->visited_pages();
            const auto status = advance(audit);
            ASSERT_TRUE(status.ok()) << status.to_string();
            EXPECT_LE(audit->visited_pages() - before, 1U);
        }
        EXPECT_TRUE(audit->complete());
        EXPECT_EQ(publisher.capture(), view);
    }

    void SetUp() override
    {
        PublicationTransaction transaction;
        for (uint64_t pid = 1; pid <= 2; ++pid) {
            auto leaf              = std::make_shared<PageState>();
            leaf->content_id       = pid;
            leaf->route_generation = 1;
            leaf->right_sibling    = pid == 1 ? 2 : UINT64_MAX;
            auto page              = std::make_shared<PublishedPage>();
            page->leaf             = leaf;
            transaction.pages.push_back({.page_id = pid, .expected = {}, .replacement = page});
        }
        auto inner           = std::make_shared<InnerPageState>();
        inner->children      = {1, 2};
        inner->separators    = {OwnedKey("m")};
        auto parent          = std::make_shared<PublishedPage>();
        parent->level        = 1;
        parent->inner        = inner;
        parent->metrics.leaf = false;
        transaction.pages.push_back({.page_id = 10, .expected = {}, .replacement = parent});
        transaction.root_page_id = 10;
        ASSERT_TRUE(publish(std::move(transaction)).status.ok());
    }
};

TEST_F(PublicationReconcile, PendingMembershipTracksCaptureLaterChangesCommitAndAbort)
{
    reconcile();
    ASSERT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 3U);
    ASSERT_TRUE(publish({
                            .expected_coverage = 0,
                            .checkpoint = CheckpointPublication{.action = CheckpointAction::Capture, .token = 1}
    })
                    .status.ok());
    const auto before = publisher.capture()->mapping.get(1);
    auto       leaf   = std::make_shared<PageState>(*before->leaf);
    ++leaf->content_id;
    auto after  = std::make_shared<PublishedPage>(*before);
    after->leaf = leaf;
    ASSERT_TRUE(publish({.pages = {{.page_id = 1, .expected = before, .replacement = after}}}).status.ok());
    reconcile();
    ASSERT_TRUE(publish({
                            .checkpoint = CheckpointPublication{.action = CheckpointAction::Commit, .token = 1}
    })
                    .status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 1U);
    reconcile();
    ASSERT_TRUE(publish({
                            .expected_coverage = 0,
                            .checkpoint = CheckpointPublication{.action = CheckpointAction::Capture, .token = 2}
    })
                    .status.ok());
    ASSERT_TRUE(publish({
                            .checkpoint = CheckpointPublication{.action = CheckpointAction::Abort, .token = 2}
    })
                    .status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 1U);
    reconcile();
}

TEST_F(PublicationReconcile, CanonicalPairsAndUniqueEndpointsMatchTheirPublication)
{
    const auto view = publisher.capture();
    ASSERT_TRUE(publish({
                            .merge_pair = MergePairPublication{.left            = 1,
                                                               .expected_left   = view->mapping.get(1),
                                                               .expected_right  = view->mapping.get(2),
                                                               .expected_parent = view->mapping.get(10),
                                                               .candidate       = MergePairProof{.left          = 1,
                                                                                                 .right         = 2,
                                                                                                 .parent        = 10,
                                                                                                 .left_content  = 1,
                                                                                                 .right_content = 2,
                                                                                                 .left_route    = 1,
                                                                                                 .right_route   = 1}}
    })
                    .status.ok());
    EXPECT_EQ(publisher.capture()->stats.merge_candidate_pages, 2U);
    EXPECT_EQ(publisher.capture()->stats.merge_candidate_pairs, 1U);
    reconcile();
    const auto classified = publisher.capture();
    ASSERT_TRUE(publish({
                            .merge_pair = MergePairPublication{.left = 1, .expected_left = classified->mapping.get(1)}
    })
                    .status.ok());
    reconcile();
}

TEST_F(PublicationReconcile, ConcurrentPublicationRejectsTheOldAuditWithoutMutatingItsView)
{
    const auto old   = publisher.capture();
    auto       audit = std::make_shared<TreeStateReconciliation>(old);
    ASSERT_TRUE(advance(audit).ok());
    auto page = std::make_shared<PublishedPage>(*old->mapping.get(1));
    auto leaf = std::make_shared<PageState>(*page->leaf);
    ++leaf->content_id;
    page->leaf = leaf;
    ASSERT_TRUE(publish({.pages = {{.page_id = 1, .expected = old->mapping.get(1), .replacement = page}}}).status.ok());
    EXPECT_EQ(advance(audit).code(), Code::kUnavailable);
    EXPECT_EQ(old->mapping.get(1)->leaf->content_id, 1U);
    EXPECT_EQ(publisher.capture()->mapping.get(1)->leaf->content_id, 2U);
    reconcile();
}

TEST_F(PublicationReconcile, InvalidBoundDoesNotConsumeCursorOrPublishAnEmptyTransaction)
{
    const auto view  = publisher.capture();
    auto       audit = std::make_shared<TreeStateReconciliation>(view);
    EXPECT_EQ(advance(audit, 0).code(), Code::kInvalidArgument);
    EXPECT_EQ(advance(audit, 65).code(), Code::kInvalidArgument);
    EXPECT_EQ(audit->visited_pages(), 0U);
    EXPECT_EQ(publisher.capture(), view);
    reconcile();
}
} // namespace
} // namespace crowdb::tree
