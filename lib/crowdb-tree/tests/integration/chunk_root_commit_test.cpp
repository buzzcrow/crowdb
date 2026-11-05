// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "common/test_chunk_root.h"
#include "snapshot/chunk_root_codec_internal.h"

namespace crowdb::tree
{
namespace
{
void complete(const std::shared_ptr<ChunkRootCommit> &operation)
{
    for (size_t i = 0; i < 100 && !operation->done(); ++i) {
        (void)operation->step();
    }
    ASSERT_TRUE(operation->done());
}
} // namespace

TEST(ChunkRootCommit, LostMutationRepliesResolveTheSameCandidateWithoutRepeatingReferenceWindows)
{
    test_chunk::TestRootBackend test;
    auto                        pages     = test.pages(65);
    auto                        operation = test.commit(pages);
    test.catalog->lose_begin              = true;
    test.catalog->lose_prepare            = true;
    test.catalog->lose_commit             = true;
    complete(operation);
    ASSERT_TRUE(operation->status().ok()) << operation->status().to_string();
    EXPECT_TRUE(operation->committed());
    EXPECT_FALSE(operation->needs_recovery());
    EXPECT_EQ(test.catalog->begin_calls, 1U);
    EXPECT_EQ(test.catalog->prepare_calls, 2U);
    EXPECT_EQ(test.catalog->commit_calls, 1U);
    EXPECT_EQ(test.catalog->abort_calls, 0U);
    ChunkRootManifest root(test.credits.credits.bytes);
    ASSERT_TRUE(decode_chunk_root(test.catalog->manifest, 19, 7, &root).ok());
    EXPECT_EQ(root.frame_count, 65U);
    EXPECT_EQ(root.indexes.size(), 2U);
    EXPECT_EQ(root.identity.coverage, 15U);
    EXPECT_EQ(root.identity.wal_replay_offset, 4096U);
    EXPECT_EQ(root.stored_references, 0U);
    EXPECT_EQ(root.dependency_entries, 0U);
    detail::ChunkStageId pin;
    detail::ChunkId      unused;
    ASSERT_TRUE(test.ids->next(PagePurpose::kPageIndex, &pin, &unused).ok());
    std::shared_ptr<const ChunkPackBacking> index;
    ASSERT_TRUE(pin_chunk_backing(test.domain, root.indexes[0].pack, pin, test.credits.credits.bytes, &index).ok());
    std::vector<uint8_t> bytes(index->pack.length);
    ASSERT_TRUE(read_chunk_backing(*index, bytes, test.credits.credits.bytes).ok());
    uint64_t count  = 0;
    size_t   offset = 0;
    ASSERT_TRUE(decode_chunk_index_header(bytes, root.identity, 0, &count, &offset).ok());
    EXPECT_EQ(count, 64U);
}

TEST(ChunkRootCommit, KnownFailedCommitAbortsAndReleasesEveryAcceptedWindow)
{
    test_chunk::TestRootBackend test;
    auto                        operation = test.commit(test.pages(65));
    test.catalog->fail_commit             = true;
    complete(operation);
    EXPECT_EQ(operation->status().code(), Code::kUnavailable);
    EXPECT_FALSE(operation->committed());
    EXPECT_FALSE(operation->needs_recovery());
    EXPECT_EQ(test.catalog->abort_calls, 1U);
    EXPECT_EQ(test.catalog->release_calls, 2U);
}

TEST(ChunkRootCommit, UnqueryableCommitKeepsCandidateProtectionAndDoesNotReportDurability)
{
    test_chunk::TestRootBackend test;
    auto                        operation = test.commit(test.pages(1));
    test.catalog->lose_commit             = true;
    test.catalog->fail_query              = true;
    complete(operation);
    EXPECT_EQ(operation->status().code(), Code::kUnavailable);
    EXPECT_FALSE(operation->committed());
    EXPECT_TRUE(operation->needs_recovery());
    if (!test.catalog->candidate) {
        FAIL() << "candidate protection disappeared";
    }
    EXPECT_EQ(test.catalog->candidate->state, detail::ChunkCandidateState::Committed);
    EXPECT_EQ(test.catalog->abort_calls, 0U);
    EXPECT_EQ(test.catalog->release_calls, 0U);
}

TEST(ChunkRootCommit, CancellationReleasesOnlyTheWindowsThatWereActuallyPrepared)
{
    test_chunk::TestRootBackend test;
    auto                        operation = test.commit(test.pages(65));
    for (size_t i = 0; i < 30 && test.catalog->prepare_calls == 0; ++i) {
        ASSERT_TRUE(operation->step().ok());
    }
    ASSERT_EQ(test.catalog->prepare_calls, 1U);
    ASSERT_TRUE(operation->step(true).ok());
    complete(operation);
    EXPECT_EQ(operation->status().code(), Code::kUnavailable);
    EXPECT_EQ(test.catalog->prepare_calls, 1U);
    EXPECT_EQ(test.catalog->commit_calls, 0U);
    EXPECT_EQ(test.catalog->release_calls, 1U);
}

TEST(ChunkRootCommit, AdmittedMetadataAndOneWindowFinishWithHostCreditExhausted)
{
    test_chunk::TestRootBackend test;
    auto                        pages    = test.pages(65);
    const auto                  metadata = ChunkRootCommit::allocation_bound(65);
    const auto                  window   = ChunkRootCommit::window_allocation_bound(0);
    if (!metadata || !window) {
        FAIL() << "checkpoint bound overflow";
    }
    detail::ResourceBudget::Limits limits{};
    const auto                     category = static_cast<size_t>(detail::BudgetClass::kRouting);
    limits[category] = *metadata + *window + detail::shared_allocation_bound<detail::AllocationReservation>();
    detail::ResourceBudget budget(limits, 4);
    auto                   account = budget.make_account(limits);
    auto grant = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, limits[category]);
    if (!grant) {
        FAIL() << "checkpoint reservation failed";
    }
    auto credit = detail::AllocationReservation::create(std::move(*grant));
    EXPECT_FALSE(detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, 1));
    ChunkRootIdentity                identity{.tree              = 19,
                                              .root_generation   = 7,
                                              .owner_epoch       = 1,
                                              .snapshot_sequence = 4,
                                              .page_generation   = 3,
                                              .coverage          = 15,
                                              .gc_floor          = 8,
                                              .root_page_id      = 0,
                                              .next_page_id      = 65,
                                              .wal_replay_offset = 4096};
    std::shared_ptr<ChunkRootCommit> operation;
    ASSERT_TRUE(ChunkRootCommit::create(pages, test.domain, test.ids, identity, credit, credit, &operation).ok());
    complete(operation);
    ASSERT_TRUE(operation->status().ok()) << operation->status().to_string();
    EXPECT_TRUE(operation->committed());
}
} // namespace crowdb::tree
