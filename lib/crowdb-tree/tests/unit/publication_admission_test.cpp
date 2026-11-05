// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
PublicationResult publish(PublicationCoordinator &publisher, PublicationTransaction transaction)
{
    static std::atomic<uint64_t> next_record{1};
    if (!transaction.record) {
        transaction.record = std::make_shared<InstallRecord>(
            InstallToken{.generation = 1, .batch = 1, .range = 0, .sequence = next_record.fetch_add(1)});
    }
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = std::move(transaction);
    std::promise<PublicationResult> done;
    auto                            result = done.get_future();
    request->on_done                       = [&](PublicationResult value) { done.set_value(std::move(value)); };
    if (!publisher.try_submit(request)) {
        ADD_FAILURE() << "admission test publication rejected";
        return {.status = Status::unavailable()};
    }
    return result.get();
}

PublicationResult acquire(PublicationCoordinator &publisher, uint64_t sequence, uint64_t owner = 42)
{
    return publish(publisher, {
                                  .expected_commit_seq = sequence,
                                  .flush_admission = FlushAdmissionPublication{.action = FlushAdmissionAction::Acquire,
                                                                               .owner  = owner},
                                  .flush_admission_owner = owner
    });
}

PublicationResult release(PublicationCoordinator &publisher, uint64_t owner = 42)
{
    return publish(publisher, {
                                  .flush_admission = FlushAdmissionPublication{.action = FlushAdmissionAction::Release,
                                                                               .owner  = owner},
                                  .flush_admission_owner = owner
    });
}

std::shared_ptr<PublishedPage> leaf()
{
    auto page  = std::make_shared<PublishedPage>();
    page->leaf = std::make_shared<PageState>();
    return page;
}

TEST(PublicationAdmission, StaleReserveCannotAcquirePublicationOwnership)
{
    PublicationCoordinator publisher;
    const auto             before = publisher.capture();
    ASSERT_TRUE(
        publish(publisher, {.pages = {{.page_id = 0, .expected = {}, .replacement = leaf()}}, .root_page_id = 0})
            .status.ok());
    const auto changed = publisher.capture();
    EXPECT_EQ(acquire(publisher, before->commit_seq).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), changed);
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
    EXPECT_EQ(publish(publisher,
                      {
                          .flush_admission       = FlushAdmissionPublication{FlushAdmissionAction::Acquire, 42},
                          .flush_admission_owner = 42
    })
                  .status.code(),
              Code::kInvalidArgument);
}

TEST(PublicationAdmission, OnlyAdmittedWriterCanChangeMappingWhileOldReadersRemainValid)
{
    PublicationCoordinator publisher;
    auto                   original = leaf();
    ASSERT_TRUE(
        publish(publisher, {.pages = {{.page_id = 0, .expected = {}, .replacement = original}}, .root_page_id = 0})
            .status.ok());
    const auto old = publisher.capture();
    ASSERT_TRUE(acquire(publisher, old->commit_seq).status.ok());
    auto replacement = leaf();
    auto blocked = publish(publisher, {.pages = {{.page_id = 0, .expected = original, .replacement = replacement}}});
    EXPECT_EQ(blocked.status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture()->mapping.get(0), original);
    ASSERT_TRUE(publish(publisher, {.pages = {{.page_id = 0, .expected = original, .replacement = replacement}},
                                    .flush_admission_owner = 42})
                    .status.ok());
    EXPECT_EQ(old->mapping.get(0), original);
    EXPECT_EQ(publisher.capture()->mapping.get(0), replacement);
    EXPECT_EQ(release(publisher, 43).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 42U);
    ASSERT_TRUE(release(publisher).status.ok());
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
    EXPECT_TRUE(publish(publisher, {}).status.ok());
}

TEST(PublicationAdmission, PartialHandoffKeepsGateUntilAtomicCoverageCommit)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(acquire(publisher, 0).status.ok());
    ASSERT_TRUE(publish(publisher,
                        {
                            .handoff = PendingHandoff{.batch = 1, .frontier = 1},
                              .flush_admission_owner = 42
    })
                    .status.ok());
    EXPECT_EQ(release(publisher).status.code(), Code::kUnavailable);
    auto before    = publisher.capture();
    auto sources   = std::make_shared<PublishedSources>();
    sources->floor = 1;
    PublicationTransaction coverage{
        .expected_coverage     = 0,
        .coverage              = 1,
        .expected_sources      = before->sources,
        .sources               = sources,
        .handoff               = PendingHandoff{.batch = 1, .frontier = 1},
        .flush_admission_owner = 43
    };
    EXPECT_EQ(publish(publisher, coverage).status.code(), Code::kUnavailable);
    EXPECT_EQ(publisher.capture(), before);
    coverage.flush_admission_owner = 42;
    ASSERT_TRUE(publish(publisher, std::move(coverage)).status.ok());
    EXPECT_EQ(publisher.capture()->coverage, 1U);
    EXPECT_EQ(publisher.capture()->flush_admission_owner, 0U);
    EXPECT_FALSE(publisher.capture()->pending_handoff);
}

TEST(PublicationAdmission, SnapshotCaptureKeepsTheSourceFreeCompleteViewDuringPartialHandoff)
{
    PublicationCoordinator publisher;
    auto                   original        = leaf();
    auto                   initial_sources = std::make_shared<PublishedSources>();
    initial_sources->tables.push_back(std::make_shared<MemTable>());
    ASSERT_TRUE(publish(publisher, {.pages            = {{.page_id = 0, .expected = {}, .replacement = original}},
                                    .root_page_id     = 0,
                                    .expected_sources = publisher.capture()->sources,
                                    .sources          = initial_sources})
                    .status.ok());
    const auto before = publisher.capture_complete();
    ASSERT_TRUE(acquire(publisher, before->commit_seq).status.ok());
    const auto complete = publisher.capture_complete();
    EXPECT_EQ(complete->coverage, before->coverage);
    EXPECT_EQ(complete->mapping.get(0), original);
    EXPECT_TRUE(complete->sources->tables.empty());
    EXPECT_EQ(complete->sources->floor, complete->coverage);
    auto replacement = leaf();
    ASSERT_TRUE(publish(publisher,
                        {
                            .pages                 = {{.page_id = 0, .expected = original, .replacement = replacement}},
                            .handoff               = PendingHandoff{.batch = 1, .frontier = 1},
                            .flush_admission_owner = 42
    })
                    .status.ok());
    EXPECT_EQ(publisher.capture()->mapping.get(0), replacement);
    EXPECT_EQ(publisher.capture_complete(), complete);
    auto sources   = std::make_shared<PublishedSources>();
    sources->floor = 1;
    ASSERT_TRUE(publish(publisher,
                        {
                            .expected_coverage     = 0,
                            .coverage              = 1,
                            .expected_sources      = publisher.capture()->sources,
                            .sources               = sources,
                            .handoff               = PendingHandoff{.batch = 1, .frontier = 1},
                            .flush_admission_owner = 42
    })
                    .status.ok());
    EXPECT_EQ(publisher.capture_complete(), publisher.capture());
    EXPECT_EQ(publisher.capture_complete()->coverage, 1U);
    EXPECT_EQ(publisher.capture_complete()->mapping.get(0), replacement);
    EXPECT_EQ(complete->mapping.get(0), original);
}

TEST(PublicationAdmission, StandalonePartialHandoffAlsoRetainsTheLastCompleteView)
{
    PublicationCoordinator publisher;
    auto                   original = leaf();
    ASSERT_TRUE(
        publish(publisher, {.pages = {{.page_id = 0, .expected = {}, .replacement = original}}, .root_page_id = 0})
            .status.ok());
    auto replacement = leaf();
    ASSERT_TRUE(publish(publisher,
                        {
                            .pages   = {{.page_id = 0, .expected = original, .replacement = replacement}},
                            .handoff = PendingHandoff{.batch = 1, .frontier = 1}
    })
                    .status.ok());
    EXPECT_EQ(publisher.capture_complete()->mapping.get(0), original);
    EXPECT_EQ(publisher.capture_complete()->coverage, 0U);
    EXPECT_TRUE(publisher.capture_complete()->sources->tables.empty());
    EXPECT_EQ(publisher.capture()->mapping.get(0), replacement);
}

TEST(PublicationAdmission, AdmissionReleaseDropsTheUnusedCompleteView)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(acquire(publisher, publisher.capture()->commit_seq).status.ok());
    std::weak_ptr<const PublishedTreeState> complete = publisher.capture_complete();
    EXPECT_FALSE(complete.expired());
    ASSERT_TRUE(release(publisher).status.ok());
    EXPECT_EQ(publisher.capture_complete(), publisher.capture());
    (void)publisher.reclaim();
    EXPECT_TRUE(complete.expired());
}

TEST(PublicationAdmission, OldCheckpointMayFinishButNewCaptureWaits)
{
    PublicationCoordinator publisher;
    ASSERT_TRUE(publish(publisher,
                        {
                            .expected_coverage = 0, .checkpoint = CheckpointPublication{CheckpointAction::Capture, 1}
    })
                    .status.ok());
    EXPECT_EQ(acquire(publisher, publisher.capture()->commit_seq).status.code(), Code::kUnavailable);
    ASSERT_TRUE(publish(publisher,
                        {
                            .checkpoint = CheckpointPublication{CheckpointAction::Commit, 1}
    })
                    .status.ok());
    ASSERT_TRUE(acquire(publisher, publisher.capture()->commit_seq).status.ok());
    EXPECT_EQ(publish(publisher,
                      {
                          .expected_coverage     = 0,
                          .checkpoint            = CheckpointPublication{CheckpointAction::Capture, 2},
                          .flush_admission_owner = 42
    })
                  .status.code(),
              Code::kUnavailable);
    ASSERT_TRUE(release(publisher).status.ok());
    EXPECT_TRUE(publish(publisher,
                        {
                            .expected_coverage = 0, .checkpoint = CheckpointPublication{CheckpointAction::Capture, 2}
    })
                    .status.ok());
}
} // namespace
} // namespace crowdb::tree
