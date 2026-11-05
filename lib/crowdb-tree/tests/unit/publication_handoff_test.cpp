// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<InstallRecord> record(uint64_t batch, uint64_t sequence)
{
    return std::make_shared<InstallRecord>(InstallToken{.batch = batch, .sequence = sequence});
}

PublicationResult publish(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    PublicationResult result;
    auto              request = std::make_shared<PublicationRequest>();
    request->transaction      = std::move(transaction);
    request->on_done          = [&](PublicationResult value) { result = std::move(value); };
    EXPECT_TRUE(publisher.try_submit(request));
    return result;
}

std::shared_ptr<PublishedPage> seed(PublicationCoordinator &publisher)
{
    auto page  = std::make_shared<PublishedPage>();
    page->leaf = std::make_shared<PageState>();
    EXPECT_TRUE(publish(publisher, {.record       = record(0, 0),
                                    .pages        = {{.page_id = 0, .expected = {}, .replacement = page}},
                                    .root_page_id = 0})
                    .status.ok());
    return page;
}

PublicationTransaction capture(uint64_t token)
{
    return {
        .record            = record(9, token),
        .expected_coverage = 0,
        .checkpoint        = CheckpointPublication{.action = CheckpointAction::Capture, .token = token}
    };
}

TEST(PublicationHandoff, PartialInstallBlocksCheckpointAndForeignMutationUntilCoverage)
{
    PublicationCoordinator publisher;
    auto                   before    = seed(publisher);
    auto                   after     = std::make_shared<PublishedPage>(*before);
    auto                   state     = std::make_shared<PageState>(*before->leaf);
    state->content_id                = 2;
    after->leaf                      = state;
    auto                   installed = record(1, 0);
    PublicationTransaction install{
        .record            = installed,
        .pages             = {{.page_id = 0, .expected = before, .replacement = after}},
        .expected_coverage = 0,
        .handoff           = PendingHandoff{.batch = 1, .frontier = 3}
    };
    ASSERT_TRUE(publish(publisher, install).status.ok());
    auto partial = publisher.capture();
    ASSERT_TRUE(partial->pending_handoff);
    EXPECT_EQ(partial->coverage, 0U);
    EXPECT_EQ(publish(publisher, capture(1)).status.code(), Code::kUnavailable);
    EXPECT_EQ(publish(publisher,
                      {.record = record(2, 0), .pages = {{.page_id = 0, .expected = after, .replacement = before}}})
                  .status.code(),
              Code::kUnavailable);
    install.record         = record(2, 1);
    install.handoff->batch = 2;
    EXPECT_EQ(publish(publisher, install).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), partial);
    auto sources                      = std::make_shared<PublishedSources>();
    sources->floor                    = 3;
    auto                   unfinished = record(1, 1);
    PublicationTransaction coverage{
        .record            = record(1, 2),
        .expected_coverage = 0,
        .coverage          = 3,
        .expected_sources  = partial->sources,
        .sources           = sources,
        .required_installs = {installed,  unfinished   },
        .handoff           = PendingHandoff{.batch = 1, .frontier = 3}
    };
    EXPECT_EQ(publish(publisher, coverage).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), partial);
    ASSERT_TRUE(
        publish(publisher,
                {
                    .record = unfinished, .expected_coverage = 0, .handoff = PendingHandoff{.batch = 1, .frontier = 3}
    })
            .status.ok());
    ASSERT_TRUE(publish(publisher, coverage).status.ok());
    EXPECT_FALSE(publisher.capture()->pending_handoff);
    EXPECT_TRUE(partial->pending_handoff);
    EXPECT_EQ(partial->mapping.get(0), after);
    auto checkpoint              = capture(2);
    checkpoint.expected_coverage = 3;
    auto result                  = publish(publisher, checkpoint);
    ASSERT_TRUE(result.status.ok());
    ASSERT_TRUE(result.checkpoint_view);
    EXPECT_EQ(result.checkpoint_view->coverage, 3U);
    EXPECT_EQ(result.checkpoint_view->mapping.get(0), after);
}

TEST(PublicationHandoff, PriorCheckpointCanCommitDuringNewHandoff)
{
    PublicationCoordinator publisher;
    auto                   page     = seed(publisher);
    auto                   captured = publish(publisher, capture(1));
    ASSERT_TRUE(captured.status.ok());
    auto changed      = std::make_shared<PublishedPage>(*page);
    auto state        = std::make_shared<PageState>();
    state->content_id = 9;
    changed->leaf     = state;
    ASSERT_TRUE(publish(publisher,
                        {
                            .record  = record(1, 0),
                            .pages   = {{.page_id = 0, .expected = page, .replacement = changed}},
                            .handoff = PendingHandoff{.batch = 1, .frontier = 2}
    })
                    .status.ok());
    ASSERT_TRUE(publish(publisher,
                        {
                            .record     = record(9, 2),
                            .checkpoint = CheckpointPublication{.action = CheckpointAction::Commit, .token = 1}
    })
                    .status.ok());
    EXPECT_TRUE(publisher.capture()->pending_handoff);
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 1U);
    EXPECT_EQ(captured.checkpoint_view->mapping.get(0), page);
    EXPECT_EQ(publish(publisher, capture(2)).status.code(), Code::kUnavailable);
}

TEST(PublicationHandoff, RejectsMismatchedBatchAndCoverageWithoutPublishing)
{
    PublicationCoordinator publisher;
    seed(publisher);
    const auto before = publisher.capture();
    EXPECT_EQ(publish(publisher,
                      {
                          .record = record(1, 0), .handoff = PendingHandoff{.batch = 2, .frontier = 4}
    })
                  .status.code(),
              Code::kInvalidArgument);
    EXPECT_EQ(publish(publisher,
                      {
                          .record = record(1, 0), .handoff = PendingHandoff{.batch = 1, .frontier = 0}
    })
                  .status.code(),
              Code::kInvalidArgument);
    auto sources   = std::make_shared<PublishedSources>();
    sources->floor = 3;
    EXPECT_EQ(publish(publisher,
                      {
                          .record           = record(1, 0),
                          .coverage         = 3,
                          .expected_sources = before->sources,
                          .sources          = sources,
                          .handoff          = PendingHandoff{.batch = 1, .frontier = 4}
    })
                  .status.code(),
              Code::kInvalidArgument);
    EXPECT_EQ(publisher.capture(), before);
}
} // namespace
} // namespace crowdb::tree
