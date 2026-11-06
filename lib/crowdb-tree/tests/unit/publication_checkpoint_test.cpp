// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <future>
#include <memory_resource>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<InstallRecord> checkpoint_record(uint64_t sequence)
{
    return std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 1, .range = 0, .sequence = sequence});
}

std::shared_ptr<PublishedPage> checkpoint_leaf(uint64_t content = 1, uint64_t route = 1, uint64_t representation = 1)
{
    auto state               = std::make_shared<PageState>();
    state->content_id        = content;
    state->route_generation  = route;
    state->representation_id = representation;
    auto page                = std::make_shared<PublishedPage>();
    page->leaf               = std::move(state);
    page->metrics.dirty      = true;
    return page;
}

PublicationResult publish_checkpoint(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> completed;
    auto                            result = completed.get_future();
    request->on_done                       = [&](PublicationResult value) { completed.set_value(std::move(value)); };
    if (!publisher.try_submit(request)) {
        ADD_FAILURE() << "checkpoint publication was rejected";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

PublicationResult checkpoint(PublicationCoordinator &publisher, CheckpointAction action, uint64_t token)
{
    PublicationTransaction transaction;
    transaction.record     = checkpoint_record(token);
    transaction.checkpoint = {.action = action, .token = token};
    if (action == CheckpointAction::Capture) {
        transaction.expected_coverage = publisher.capture()->coverage;
    }
    return publish_checkpoint(publisher, std::move(transaction));
}

void seed_checkpoint(PublicationCoordinator &publisher, const std::shared_ptr<PublishedPage> &page)
{
    ASSERT_TRUE(publish_checkpoint(publisher, {.record       = checkpoint_record(0),
                                               .pages        = {{.page_id = 0, .expected = {}, .replacement = page}},
                                               .root_page_id = 0})
                    .status.ok());
}

TEST(PublicationCheckpoint, CaptureArtifactReleasesL0AndRetryReturnsTheSamePinnedView)
{
    PublicationCoordinator  publisher;
    auto                    table    = std::make_shared<MemTable>();
    std::weak_ptr<MemTable> retained = table;
    auto                    sources  = std::make_shared<PublishedSources>();
    sources->floor                   = 3;
    sources->tables.push_back(table);
    auto page = checkpoint_leaf();
    ASSERT_TRUE(publish_checkpoint(publisher, {.record       = checkpoint_record(0),
                                               .pages        = {{.page_id = 0, .expected = {}, .replacement = page}},
                                               .root_page_id = 0,
                                               .coverage     = 3,
                                               .expected_sources = publisher.capture()->sources,
                                               .sources          = sources})
                    .status.ok());
    PublicationTransaction capture{
        .record            = checkpoint_record(1),
        .expected_coverage = 3,
        .checkpoint        = CheckpointPublication{.action = CheckpointAction::Capture, .token = 10}
    };
    auto first = publish_checkpoint(publisher, capture);
    ASSERT_TRUE(first.status.ok());
    ASSERT_TRUE(first.checkpoint_view);
    EXPECT_EQ(first.checkpoint_view->mapping.get(0), page);
    EXPECT_TRUE(first.checkpoint_view->sources->tables.empty());
    EXPECT_EQ(first.checkpoint_view->sources->floor, 3U);
    EXPECT_EQ(first.checkpoint_view->coverage, 3U);
    auto detached   = std::make_shared<PublishedSources>();
    detached->floor = 3;
    ASSERT_TRUE(publish_checkpoint(publisher,
                                   {.record = checkpoint_record(2), .expected_sources = sources, .sources = detached})
                    .status.ok());
    sources.reset();
    table.reset();
    publisher.reclaim();
    EXPECT_TRUE(retained.expired());
    auto retry = publish_checkpoint(publisher, capture);
    EXPECT_TRUE(retry.already_committed);
    EXPECT_EQ(retry.checkpoint_view, first.checkpoint_view);
    EXPECT_EQ(retry.commit_seq, first.commit_seq);
}

TEST(PublicationCheckpoint, L1FenceAllowsNewSourcesAndRejectsChangedPages)
{
    PublicationCoordinator publisher;
    auto                   first = checkpoint_leaf();
    seed_checkpoint(publisher, first);
    const auto before  = publisher.capture();
    auto       sources = std::make_shared<PublishedSources>();
    sources->tables.push_back(std::make_shared<MemTable>());
    const auto catalog = publish_checkpoint(
        publisher, {.record = checkpoint_record(1), .expected_sources = before->sources, .sources = sources});
    ASSERT_TRUE(catalog.status.ok());
    EXPECT_GT(catalog.commit_seq, before->commit_seq);
    EXPECT_EQ(catalog.l1_seq, before->l1_seq);
    PublicationTransaction change{.record = checkpoint_record(2),
                                  .pages  = {{.page_id = 0, .expected = first, .replacement = checkpoint_leaf(2)}},
                                  .expected_l1_seq = before->l1_seq};
    const auto             committed = publish_checkpoint(publisher, change);
    ASSERT_TRUE(committed.status.ok());
    EXPECT_EQ(committed.l1_seq, before->l1_seq + 1);
    const auto retry = publish_checkpoint(publisher, change);
    EXPECT_TRUE(retry.already_committed);
    EXPECT_EQ(retry.l1_seq, committed.l1_seq);
    EXPECT_EQ(publish_checkpoint(publisher, {.record = checkpoint_record(3), .expected_l1_seq = before->l1_seq})
                  .status.code(),
              Code::kUnavailable);
}

TEST(PublicationCheckpoint, ExhaustedL1SequenceStillAllowsSourcesButRejectsGraphMutation)
{
    auto initial    = std::make_shared<PublishedTreeState>();
    initial->l1_seq = UINT64_MAX;
    PublicationCoordinator publisher{
        4, 1, {},
          std::pmr::get_default_resource(), {},
          {.metadata = {}, .initial = initial, .source_catalog = {}}
    };
    auto sources = std::make_shared<PublishedSources>();
    sources->tables.push_back(std::make_shared<MemTable>());
    ASSERT_TRUE(
        publish_checkpoint(publisher,
                           {.record = checkpoint_record(1), .expected_sources = initial->sources, .sources = sources})
            .status.ok());
    auto       first = checkpoint_leaf();
    const auto rejected =
        publish_checkpoint(publisher, {.record       = checkpoint_record(2),
                                       .pages        = {{.page_id = 0, .expected = {}, .replacement = first}},
                                       .root_page_id = 0});
    EXPECT_EQ(rejected.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(publisher.capture()->l1_seq, UINT64_MAX);
    EXPECT_EQ(publisher.capture()->sources, sources);
    EXPECT_TRUE(publisher.capture()->mapping.empty());
}

TEST(PublicationCheckpoint, CommitRetainsLaterChangesAndRejectsWrongTokens)
{
    PublicationCoordinator publisher;
    auto                   first = checkpoint_leaf();
    seed_checkpoint(publisher, first);
    auto captured = checkpoint(publisher, CheckpointAction::Capture, 10);
    ASSERT_TRUE(captured.status.ok());
    auto next = checkpoint_leaf(2);
    ASSERT_TRUE(publish_checkpoint(publisher, {.record = checkpoint_record(1),
                                               .pages  = {{.page_id = 0, .expected = first, .replacement = next}}})
                    .status.ok());
    const auto before_stale = publisher.capture();
    EXPECT_EQ(checkpoint(publisher, CheckpointAction::Commit, 9).status.code(), Code::kUnavailable);
    EXPECT_EQ(checkpoint(publisher, CheckpointAction::Capture, 11).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), before_stale);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 10).status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 1U);
    EXPECT_TRUE(publisher.capture()->stats.checkpoint_mapping_pending);
    EXPECT_EQ(captured.checkpoint_view->mapping.get(0), first);
    EXPECT_EQ(checkpoint(publisher, CheckpointAction::Commit, 10).status.code(), Code::kUnavailable);
    EXPECT_EQ(checkpoint(publisher, CheckpointAction::Capture, 10).status.code(), Code::kUnavailable);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 11).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 11).status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
    EXPECT_FALSE(publisher.capture()->stats.checkpoint_mapping_pending);
    EXPECT_EQ(publisher.capture()->stats.dirty_pages, 1U);
    EXPECT_FALSE(publisher.capture()->checkpoint_inflight);
}

TEST(PublicationCheckpoint, RepresentationAndCleanWritebackDoNotCreateLogicalChanges)
{
    PublicationCoordinator publisher;
    auto                   first = checkpoint_leaf();
    seed_checkpoint(publisher, first);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    auto clean           = checkpoint_leaf(1, 1, 2);
    clean->metrics.dirty = false;
    ASSERT_TRUE(publish_checkpoint(publisher, {.record = checkpoint_record(2),
                                               .pages  = {{.page_id = 0, .expected = first, .replacement = clean}}})
                    .status.ok());
    EXPECT_EQ(publisher.capture()->stats.dirty_pages, 0U);
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 1U);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
    auto loaded           = checkpoint_leaf(1, 1, 3);
    loaded->metrics.dirty = false;
    ASSERT_TRUE(publish_checkpoint(publisher, {.record = checkpoint_record(3),
                                               .pages  = {{.page_id = 0, .expected = clean, .replacement = loaded}}})
                    .status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
    EXPECT_FALSE(publisher.capture()->stats.checkpoint_mapping_pending);
    auto routed = checkpoint_leaf(1, 2, 4);
    ASSERT_TRUE(publish_checkpoint(publisher, {.record = checkpoint_record(4),
                                               .pages  = {{.page_id = 0, .expected = loaded, .replacement = routed}}})
                    .status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 1U);
}

TEST(PublicationCheckpoint, DeleteAndCoverageOnlyAdvanceKeepAnchorPending)
{
    PublicationCoordinator publisher;
    auto                   first = checkpoint_leaf();
    seed_checkpoint(publisher, first);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
    ASSERT_TRUE(publish_checkpoint(publisher, {.record       = checkpoint_record(2),
                                               .pages        = {{.page_id = 0, .expected = first, .replacement = {}}},
                                               .root_page_id = UINT64_MAX})
                    .status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 1).status.ok());
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
    EXPECT_TRUE(publisher.capture()->stats.checkpoint_mapping_pending);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 2).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 2).status.ok());
    EXPECT_FALSE(publisher.capture()->stats.checkpoint_mapping_pending);
    auto sources   = std::make_shared<PublishedSources>();
    sources->floor = 9;
    ASSERT_TRUE(publish_checkpoint(publisher, {.record           = checkpoint_record(3),
                                               .coverage         = 9,
                                               .expected_sources = publisher.capture()->sources,
                                               .sources          = sources})
                    .status.ok());
    EXPECT_TRUE(publisher.capture()->stats.checkpoint_mapping_pending);
    EXPECT_EQ(publisher.capture()->stats.checkpoint_pending_pages, 0U);
    const auto before = publisher.capture();
    EXPECT_EQ(
        publish_checkpoint(publisher,
                           {
                               .record            = checkpoint_record(4),
                               .expected_coverage = 8,
                               .checkpoint = CheckpointPublication{.action = CheckpointAction::Capture, .token = 3}
    })
            .status.code(),
        Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), before);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 3).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Abort, 3).status.ok());
    EXPECT_TRUE(publisher.capture()->stats.checkpoint_mapping_pending);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 4).status.ok());
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Commit, 4).status.ok());
    EXPECT_FALSE(publisher.capture()->stats.checkpoint_mapping_pending);
    const auto totals = publisher.work_metrics()->totals();
    // Publication-only bookkeeping has no durable I/O pipeline to time.
    EXPECT_EQ(totals[static_cast<size_t>(TreeStage::SnapshotCommit)].attempts, 0U);
}

TEST(PublicationCheckpoint, CaptureRejectsMixedMutationAndMissingHandoffFrontier)
{
    PublicationCoordinator publisher;
    auto                   page   = checkpoint_leaf();
    const auto             before = publisher.capture();
    EXPECT_EQ(
        publish_checkpoint(publisher,
                           {
                               .record     = checkpoint_record(1),
                               .checkpoint = CheckpointPublication{.action = CheckpointAction::Capture, .token = 1}
    })
            .status.code(),
        Code::kInvalidArgument);
    EXPECT_EQ(
        publish_checkpoint(publisher,
                           {
                               .record            = checkpoint_record(2),
                               .pages             = {{.page_id = 0, .expected = {}, .replacement = page}},
                               .root_page_id      = 0,
                               .expected_coverage = 0,
                               .checkpoint = CheckpointPublication{.action = CheckpointAction::Capture, .token = 1}
    })
            .status.code(),
        Code::kInvalidArgument);
    EXPECT_EQ(publisher.capture(), before);
    ASSERT_TRUE(checkpoint(publisher, CheckpointAction::Capture, 1).status.ok());
}
} // namespace
} // namespace crowdb::tree
