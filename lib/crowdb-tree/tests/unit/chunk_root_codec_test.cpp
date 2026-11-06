// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_runtime_ids.h"
#include "backend/chunk/chunk_runtime_page_store.h"
#include "common/test_chunk_backing.h"
#include "snapshot/chunk_root_codec_internal.h"

namespace crowdb::tree
{
namespace
{
ChunkRootIdentity test_identity()
{
    return {.tree              = 19,
            .root_generation   = 7,
            .owner_epoch       = 1,
            .snapshot_sequence = 4,
            .page_generation   = 3,
            .coverage          = 15,
            .gc_floor          = 8,
            .root_page_id      = 0,
            .next_page_id      = 10,
            .wal_replay_offset = 4096};
}

detail::ChunkFixedPackRef test_index_pack(uint32_t bytes)
{
    return {
        .stage    = {.owner_epoch = 1, .operation_high = 2, .operation_low = 3},
        .chunk    = {uint64_t{3} << 56U, 9},
        .offset   = 0,
        .length   = bytes,
        .checksum = 17,
        .purpose  = PagePurpose::kPageIndex
    };
}
} // namespace

TEST(ChunkRootCodec, RoundTripKeepsRootSequenceAndWalDistinctFromPageGeneration)
{
    test_read::LoadCredits credits;
    ChunkRootManifest      root(credits.credits.reservation());
    root.identity    = test_identity();
    root.frame_count = 2;
    root.indexes.push_back({.first_page = 0, .last_page = 9, .frame_count = 2, .pack = test_index_pack(512)});
    ChunkMetadataBytes bytes{detail::ReservationAllocator<uint8_t>(credits.credits.reservation())};
    ASSERT_TRUE(encode_chunk_root(root, &bytes).ok());
    ChunkRootManifest decoded(credits.credits.reservation());
    ASSERT_TRUE(decode_chunk_root(bytes, 19, 7, &decoded).ok());
    EXPECT_EQ(decoded.identity.root_generation, 7U);
    EXPECT_EQ(decoded.identity.snapshot_sequence, 4U);
    EXPECT_EQ(decoded.identity.page_generation, 3U);
    EXPECT_EQ(decoded.identity.coverage, 15U);
    EXPECT_EQ(decoded.identity.wal_replay_offset, 4096U);
    EXPECT_EQ(decoded.indexes.size(), 1U);
    ChunkRootManifest wrong_root(credits.credits.reservation());
    EXPECT_EQ(decode_chunk_root(bytes, 19, 8, &wrong_root).code(), Code::kCorruption);
    chunk_codec::put(bytes, 13, UINT64_MAX);
    chunk_codec::seal(bytes);
    EXPECT_EQ(decode_chunk_root(bytes, 19, 7, &wrong_root).code(), Code::kCorruption);
}

TEST(ChunkRootCodec, IndexValidatesExactNamespaceAndRejectsOversizedPackFields)
{
    test_chunk::TestChunkBackend      test;
    std::shared_ptr<ChunkPackBacking> pack;
    ASSERT_TRUE(test.prepare(8192, &pack).ok());
    auto backing        = detail::reserved_shared<BackingRef>(test.credits.credits.reservation());
    backing->backend_id = 17;
    backing->length     = 8192;
    backing->owner      = pack;
    backing->chunk      = pack;
    backing->storage    = BackingStorage::kChunkPack;
    auto refs           = detail::reserved_shared<StoredOverflowReferences>(
        test.credits.credits.reservation(),
        detail::ReservationAllocator<StoredOverflowReference>(test.credits.credits.reservation()));
    refs->push_back({.head = 4, .bytes = 123});
    DurableCapturedFrame        frame{.page_id         = 0,
                                      .type            = page_type::kLeafBase,
                                      .frame_bytes     = 4096,
                                      .logical_bytes   = 4132,
                                      .next_overflow   = kInvalidPageId,
                                      .overflow_bytes  = 0,
                                      .backing         = backing,
                                      .stored_overflow = refs,
                                      .summary         = {}};
    const DurableCapturedFrame *frames[]{&frame};
    ChunkMetadataBytes          bytes{detail::ReservationAllocator<uint8_t>(test.credits.credits.reservation())};
    ASSERT_TRUE(encode_chunk_index(test_identity(), 2, frames, &bytes).ok());
    uint64_t count  = 0;
    size_t   offset = 0;
    EXPECT_EQ(decode_chunk_index_header(bytes, test_identity(), 3, &count, &offset).code(), Code::kCorruption);
    ASSERT_TRUE(decode_chunk_index_header(bytes, test_identity(), 2, &count, &offset).ok());
    ASSERT_EQ(count, 1U);
    ChunkIndexFrame parsed;
    ASSERT_TRUE(decode_chunk_index_frame(bytes, &offset, &parsed).ok());
    EXPECT_EQ(parsed.page_id, 0U);
    EXPECT_EQ(parsed.stored_reference_words.size(), 16U);
    EXPECT_EQ(offset + 4, bytes.size());
    chunk_codec::put(std::span(bytes).subspan(kChunkIndexHeaderBytes), 12, uint64_t{1} << 32U);
    chunk_codec::seal(bytes);
    ASSERT_TRUE(decode_chunk_index_header(bytes, test_identity(), 2, &count, &offset).ok());
    EXPECT_EQ(decode_chunk_index_frame(bytes, &offset, &parsed).code(), Code::kCorruption);
}

TEST(ChunkRuntimeIds, PurposeAndCounterShareOneNamespaceWithoutReusingOperations)
{
    detail::ChunkRuntimeIds ids(7, 23, uint64_t{45} << 32U);
    detail::ChunkStageId    first;
    detail::ChunkStageId    second;
    detail::ChunkId         a;
    detail::ChunkId         b;
    ASSERT_TRUE(ids.next(PagePurpose::kBtreePage, &first, &a).ok());
    ASSERT_TRUE(ids.next(PagePurpose::kPageIndex, &second, &b).ok());
    EXPECT_NE(first, second);
    EXPECT_EQ(first.owner_epoch, 7U);
    EXPECT_EQ(detail::chunk_purpose(a), PagePurpose::kBtreePage);
    EXPECT_EQ(detail::chunk_purpose(b), PagePurpose::kPageIndex);
    EXPECT_NE(a.low, b.low);
}

TEST(ChunkRuntimePageStore, UnsupportedCatalogDoesNotExposeAnIndependentAddressSpace)
{
    test_read::LoadCredits        credits;
    auto                          catalog   = std::make_shared<detail::MemoryRootCatalog>(1);
    auto                          transport = std::make_shared<detail::MemoryChunkTransport>();
    detail::ChunkRuntimePageStore store({.tree = 19, .owner_epoch = 1}, catalog, transport,
                                        credits.credits.reservation());
    EXPECT_FALSE(store.supported());
    std::shared_ptr<const ChunkBackingDomain> domain;
    EXPECT_EQ(store.make_domain(17, credits.credits.reservation(), &domain).code(), Code::kNotSupported);
    EXPECT_FALSE(domain);
    EXPECT_EQ(store.sync().code(), Code::kNotSupported);
    EXPECT_EQ(store.write_at(0, nullptr, 0).code(), Code::kNotSupported);
    EXPECT_EQ(store.read_at(0, nullptr, 0).code(), Code::kNotSupported);
}

TEST(ChunkStageCodec, OwnedEncodingAndDecodingRetainTheCallerAllocator)
{
    test_read::LoadCredits credits;
    using Record = detail::ChunkStageRecord;
    Record record{
        .tree_id             = 19,
        .id                  = {.owner_epoch = 1, .operation_high = 2, .operation_low = 3},
        .revision            = 3,
        .chunk_high          = uint64_t{2}
                           << 56U,
        .chunk_low           = 1,
        .capacity            = 128,
        .manifest_generation = 0,
        .purpose             = 2,
        .state               = detail::ChunkStageState::Reloadable,
        .packs = decltype(Record::packs)(MetadataAllocator<detail::ChunkStagePack>(credits.credits.reservation()))
    };
    record.packs.push_back({.offset = 0, .length = 32, .checksum = 9});
    auto decoded = record;
    decoded.packs.clear();
    decoded.packs.shrink_to_fit();
    const auto before = credits.credits.reservation()->available();
    {
        detail::ChunkStageBytes bytes{MetadataAllocator<uint8_t>(credits.credits.reservation())};
        ASSERT_TRUE(detail::encode_stage_record_owned(record, &bytes).ok());
        ASSERT_TRUE(detail::decode_stage_record(bytes, &decoded).ok());
        EXPECT_EQ(decoded.packs.get_allocator().owner(), credits.credits.reservation());
        EXPECT_EQ(decoded, record);
        EXPECT_EQ(before - credits.credits.reservation()->available(),
                  bytes.capacity() + (decoded.packs.capacity() * sizeof(detail::ChunkStagePack)));
    }
    decoded.packs.clear();
    decoded.packs.shrink_to_fit();
    EXPECT_EQ(credits.credits.reservation()->available(), before);
}
} // namespace crowdb::tree
