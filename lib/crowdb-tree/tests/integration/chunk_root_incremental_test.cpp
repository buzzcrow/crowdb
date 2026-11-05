// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_root.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree
{
namespace
{
std::shared_ptr<ChunkRootCommit> finish(std::shared_ptr<ChunkRootCommit> operation)
{
    for (size_t step = 0; step < 1000 && !operation->done(); ++step) {
        (void)operation->step();
    }
    EXPECT_TRUE(operation->done());
    EXPECT_TRUE(operation->status().ok()) << operation->status().to_string();
    EXPECT_TRUE(operation->committed());
    return operation;
}

std::shared_ptr<ChunkRootCommit> next(test_chunk::TestRootBackend                       &test,
                                      const std::shared_ptr<const CapturedDurablePages> &pages,
                                      const std::shared_ptr<const CapturedDurablePages> &baseline,
                                      const std::shared_ptr<const ChunkRootManifest> &previous, uint64_t wal = 4096)
{
    auto identity = previous->identity;
    ++identity.root_generation;
    ++identity.snapshot_sequence;
    identity.root_page_id      = pages->root_page_id;
    identity.next_page_id      = pages->next_page_id;
    identity.owner_epoch       = test.domain->epoch;
    identity.wal_replay_offset = wal;
    std::shared_ptr<ChunkRootCommit> operation;
    const auto                       status =
        ChunkRootCommit::create(pages, test.domain, test.ids, identity, test.credits.credits.bytes,
                                test.credits.credits.bytes, &operation, 1024 * 1024, previous, baseline);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return status.ok() ? finish(std::move(operation)) : nullptr;
}

std::shared_ptr<CapturedDurablePages> inherit(test_chunk::TestRootBackend &test, const CapturedDurablePages &source)
{
    auto pages = detail::reserved_shared<CapturedDurablePages>(test.credits.credits.bytes, test.credits.credits.bytes);
    pages->frames.inherit(source.frames);
    pages->backend_id   = source.backend_id;
    pages->generation   = source.generation;
    pages->coverage     = source.coverage;
    pages->root_page_id = source.root_page_id;
    pages->next_page_id = source.next_page_id;
    return pages;
}
} // namespace

TEST(ChunkRootIncremental, TwoUpdatesInOneSegmentWriteItOnceAndReuseOtherSegments)
{
    test_chunk::TestRootBackend test;
    const auto                  original = test.pages(129);
    const auto                  first    = finish(test.commit(original));
    const auto                  prior    = first->manifest();
    ASSERT_TRUE(prior);
    ASSERT_EQ(prior->indexes.size(), 3U);
    auto       pages        = inherit(test, *original);
    const auto replacements = test.pages(2);
    pages->frames.insert_or_assign(0, replacements->frames.at(0));
    pages->frames.insert_or_assign(1, replacements->frames.at(1));
    const auto allocations = test.transport->ids.size();
    const auto second      = next(test, pages, original, prior);
    ASSERT_TRUE(second);
    const auto root = second->manifest();
    ASSERT_TRUE(root);
    EXPECT_EQ(test.transport->ids.size() - allocations, 1U);
    EXPECT_NE(root->indexes[0].pack.stage, prior->indexes[0].pack.stage);
    EXPECT_EQ(root->indexes[1].pack.stage, prior->indexes[1].pack.stage);
    EXPECT_EQ(root->indexes[2].pack.stage, prior->indexes[2].pack.stage);
    EXPECT_EQ(root->indexes[1].pack.checksum, prior->indexes[1].pack.checksum);
    EXPECT_EQ(root->indexes[1].pack.offset, prior->indexes[1].pack.offset);
    EXPECT_EQ(prior->identity.root_generation, 7U);
    EXPECT_EQ(test.catalog->retained_roots.size(), 2U);
}

TEST(ChunkRootIncremental, DeletingAWholeSegmentKeepsLaterPackIdentityAndBirthOrdinal)
{
    test_chunk::TestRootBackend test;
    const auto                  original = test.pages(129);
    const auto                  prior    = finish(test.commit(original))->manifest();
    auto                        pages    = inherit(test, *original);
    for (uint64_t pid = 0; pid < 64; ++pid) {
        pages->frames.erase(pid);
    }
    pages->root_page_id    = 64;
    const auto allocations = test.transport->ids.size();
    const auto operation   = next(test, pages, original, prior);
    ASSERT_TRUE(operation);
    const auto root = operation->manifest();
    ASSERT_EQ(root->indexes.size(), 2U);
    EXPECT_EQ(test.transport->ids.size(), allocations);
    EXPECT_EQ(root->indexes[0].pack.stage, prior->indexes[1].pack.stage);
    detail::ChunkStageId pin;
    detail::ChunkId      unused;
    ASSERT_TRUE(test.ids->next(PagePurpose::kPageIndex, &pin, &unused).ok());
    std::shared_ptr<const ChunkPackBacking> pack;
    ASSERT_TRUE(pin_chunk_backing(test.domain, root->indexes[0].pack, pin, test.credits.credits.bytes, &pack).ok());
    std::vector<uint8_t> bytes(pack->pack.length);
    ASSERT_TRUE(read_chunk_backing(*pack, bytes, test.credits.credits.bytes).ok());
    uint64_t count  = 0;
    size_t   offset = 0;
    ASSERT_TRUE(decode_chunk_index_header(bytes, root->identity, 0, &count, &offset).ok());
    EXPECT_EQ(count, 64U);
    ChunkIndexFrame frame;
    ASSERT_TRUE(decode_chunk_index_frame(bytes, &offset, &frame).ok());
    EXPECT_EQ(frame.page_id, 64U);
    auto wrong = root->identity;
    wrong.tree++;
    EXPECT_EQ(decode_chunk_index_header(bytes, wrong, 0, &count, &offset).code(), Code::kCorruption);
    wrong                 = root->identity;
    wrong.root_generation = 6;
    EXPECT_EQ(decode_chunk_index_header(bytes, wrong, 0, &count, &offset).code(), Code::kCorruption);
}

TEST(ChunkRootIncremental, UnchangedRootIsZeroMutationButWalChangeCommitsWithReusedIndexes)
{
    test_chunk::TestRootBackend test;
    const auto                  pages       = test.pages(65);
    const auto                  prior       = finish(test.commit(pages))->manifest();
    const auto                  writes      = test.transport->writes;
    const auto                  allocations = test.transport->ids.size();
    const auto                  begins      = test.catalog->begin_calls;
    const auto                  prepares    = test.catalog->prepare_calls;
    const auto                  acquires    = test.catalog->acquire_calls;
    const auto                  unchanged   = next(test, pages, pages, prior);
    ASSERT_TRUE(unchanged);
    EXPECT_EQ(unchanged->manifest(), prior);
    EXPECT_EQ(unchanged->identity().snapshot_sequence, prior->identity.snapshot_sequence);
    EXPECT_EQ(unchanged->identity().root_generation, prior->identity.root_generation);
    EXPECT_EQ(test.transport->writes, writes);
    EXPECT_EQ(test.transport->ids.size(), allocations);
    EXPECT_EQ(test.catalog->begin_calls, begins);
    EXPECT_EQ(test.catalog->prepare_calls, prepares);
    EXPECT_EQ(test.catalog->acquire_calls, acquires);
    test.catalog->lose_commit = true;
    const auto scalar         = next(test, pages, pages, prior, 8192);
    ASSERT_TRUE(scalar);
    EXPECT_EQ(scalar->identity().snapshot_sequence, prior->identity.snapshot_sequence + 1);
    EXPECT_EQ(scalar->identity().wal_replay_offset, 8192U);
    EXPECT_EQ(test.transport->writes, writes);
    EXPECT_EQ(test.transport->ids.size(), allocations);
    EXPECT_EQ(test.catalog->begin_calls, begins + 1);
    EXPECT_GT(test.catalog->prepare_calls, prepares);
    EXPECT_EQ(scalar->manifest()->indexes[0].pack.stage, prior->indexes[0].pack.stage);
}

TEST(ChunkRootIncremental, OwnerTakeoverCommitsANewRootWhileReusingOldEpochPacks)
{
    test_chunk::TestRootBackend test;
    const auto                  pages  = test.pages(65);
    const auto                  prior  = finish(test.commit(pages))->manifest();
    const auto                  writes = test.transport->writes;
    test.domain                        = std::make_shared<ChunkBackingDomain>(*test.domain);
    test.domain->epoch                 = 2;
    test.ids                           = std::make_shared<detail::ChunkRuntimeIds>(2, 23, uint64_t{46} << 32U);
    const auto takeover                = next(test, pages, pages, prior);
    ASSERT_TRUE(takeover);
    EXPECT_EQ(takeover->identity().owner_epoch, 2U);
    EXPECT_EQ(takeover->identity().root_generation, 8U);
    EXPECT_EQ(takeover->identity().snapshot_sequence, 5U);
    EXPECT_EQ(test.transport->writes, writes);
    EXPECT_EQ(takeover->manifest()->indexes[0].pack.stage, prior->indexes[0].pack.stage);
    EXPECT_EQ(test.catalog->begin_calls, 2U);
}
} // namespace crowdb::tree
