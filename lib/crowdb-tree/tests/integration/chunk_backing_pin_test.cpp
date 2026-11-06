// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_root.h"

namespace crowdb::tree
{
TEST(ChunkBackingPin, RejectsStageAndPackIdentityMismatchBeforeAcquiringProtection)
{
    test_chunk::TestRootBackend test;
    auto                        pages    = test.pages(1);
    const auto                  original = pages->frames.begin()->second.backing->chunk->pack;
    for (size_t fault = 0; fault < 5; ++fault) {
        auto invalid = original;
        switch (fault) {
        case 0:
            ++invalid.chunk.low;
            break;
        case 1:
            ++invalid.offset;
            break;
        case 2:
            --invalid.length;
            break;
        case 3:
            ++invalid.checksum;
            break;
        case 4:
            invalid.purpose    = PagePurpose::kPageIndex;
            invalid.chunk.high = uint64_t{3} << 56U;
            break;
        default:
            FAIL() << "unexpected corruption case";
        }
        detail::ChunkStageId pin;
        detail::ChunkId      unused;
        ASSERT_TRUE(test.ids->next(PagePurpose::kBtreePage, &pin, &unused).ok());
        std::shared_ptr<const ChunkPackBacking> owner;
        EXPECT_EQ(pin_chunk_backing(test.domain, invalid, pin, test.credits.credits.reservation(), &owner).code(),
                  Code::kCorruption);
        EXPECT_FALSE(owner);
    }
    EXPECT_EQ(test.catalog->acquire_calls, 0U);
}

TEST(ChunkBackingPin, DeletingTransitionBetweenExactValidationAndAcquireRejectsTheReader)
{
    test_chunk::TestRootBackend test;
    auto                        pages   = test.pages(1);
    test.catalog->delete_before_acquire = true;
    detail::ChunkStageId pin;
    detail::ChunkId      unused;
    ASSERT_TRUE(test.ids->next(PagePurpose::kBtreePage, &pin, &unused).ok());
    std::shared_ptr<const ChunkPackBacking> owner;
    EXPECT_EQ(pin_chunk_backing(test.domain, pages->frames.begin()->second.backing->chunk->pack, pin,
                                test.credits.credits.reservation(), &owner)
                  .code(),
              Code::kUnavailable);
    EXPECT_FALSE(owner);
    EXPECT_EQ(test.catalog->acquire_calls, 1U);
    EXPECT_EQ(test.catalog->queue->pump(1).completed, 1U);
}
} // namespace crowdb::tree
