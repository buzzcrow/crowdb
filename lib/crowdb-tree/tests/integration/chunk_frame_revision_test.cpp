// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/chunk_catalog_codec.h"
#include "backend/chunk/chunk_frame.h"
#include "backend/chunk/chunk_page_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>

namespace crowdb::tree::detail
{

TEST(ChunkFrameRevision, DefaultPackFitsOnePhysicalFrame)
{
    const ChunkPageStore::Config config;
    EXPECT_EQ(config.pack_bytes, crowdb::protocol::kMaxFrameV2PayloadBytes);
    EXPECT_EQ(chunk_framed_length(config.pack_bytes), crowdb::protocol::kMaxFrameBytes);
}

TEST(ChunkFrameRevision, CompactReferenceTagPreservesBothRevisions)
{
    for (const uint8_t revision : {kLegacyChunkFrameRevision, kChunkFrameRevision}) {
        ChunkReferenceSegmentImage segment{
            .object_id     = 7,
            .first_ordinal = 0,
            .refs          = {{.chunk_id = {3, 5}, .offset = 4096, .length = 9000, .frame_revision = revision}}};
        const auto bytes = encode_reference_segment(segment);
        ASSERT_EQ(bytes.size(), 24 + 28);
        EXPECT_EQ(bytes.back(), revision == 2 ? 0x80 : 0);
        auto decoded = decode_reference_segment(bytes);
        ASSERT_NE(decoded, nullptr);
        ASSERT_EQ(decoded->refs.size(), 1);
        EXPECT_EQ(decoded->refs[0].length, 9000);
        EXPECT_EQ(decoded->refs[0].frame_revision, revision);
        auto damaged = bytes;
        damaged.pop_back();
        EXPECT_EQ(decode_reference_segment(damaged), nullptr);
        const uint32_t checksum        = reference_segment_checksum(segment);
        segment.refs[0].frame_revision = revision == 1 ? 2 : 1;
        EXPECT_NE(reference_segment_checksum(segment), checksum);
    }
}

TEST(ChunkFrameRevision, VersionedManifestRejectsUnknownOrLegacyMislabeling)
{
    ChunkManifest manifest;
    manifest.format_version = 7;
    manifest.packs.push_back({
        .ref = {.chunk_id = {3, 5}, .length = 9000, .frame_revision = 2}
    });
    auto bytes = encode_manifest(manifest);
    ASSERT_FALSE(bytes.empty());
    auto decoded = decode_manifest(bytes);
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->packs[0].ref.frame_revision, 2);
    bytes[0] = 8;
    EXPECT_EQ(decode_manifest(bytes), nullptr);
    manifest.format_version = 6;
    EXPECT_TRUE(encode_manifest(manifest).empty());
    manifest.packs[0].ref.frame_revision = 1;
    bytes                                = encode_manifest(manifest);
    ASSERT_FALSE(bytes.empty());
    decoded = decode_manifest(bytes);
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->packs[0].ref.frame_revision, 1);
    bytes[bytes.size() - 2] |= 0x80U;
    EXPECT_EQ(decode_manifest(bytes), nullptr);
}

TEST(ChunkFrameRevision, LegacyCatalogAndBytesRemainReadableWhileNewPacksUseOrdinaryFrames)
{
    auto    catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto    transport = std::make_shared<MemoryChunkTransport>();
    ChunkId chunk;
    ASSERT_TRUE(transport->allocate_mirror_chunk(16384, 1, &chunk, PagePurpose::kBtreePage).ok());
    const std::vector<uint8_t> payload(9000, 0x57);
    std::vector<uint8_t>       framed;
    ASSERT_EQ(crowdb::protocol::encode_frames(crowdb::protocol::FrameMagic::BtreePageV1,
                                              {.high = chunk.high, .low = chunk.low}, payload, 42, &framed),
              crowdb::protocol::FrameError::Ok);
    for (uint32_t mirror = 0; mirror < 3; ++mirror) {
        ASSERT_TRUE(transport->write_mirror(chunk, mirror, 0, framed.data(), framed.size()).ok());
    }
    ASSERT_TRUE(transport->advance_write(chunk, 0, framed.size()).ok());
    ASSERT_TRUE(transport->seal_chunk(chunk, 1, framed.size()).ok());
    auto segment       = std::make_shared<ChunkReferenceSegmentImage>();
    segment->object_id = catalog->allocate_reference_segment_id(777);
    segment->refs.push_back({.chunk_id = chunk, .length = 9000});
    ASSERT_TRUE(catalog->persist_reference_segment(777, segment).ok());
    auto manifest            = std::make_shared<ChunkManifest>();
    manifest->format_version = 6;
    manifest->tree_id        = 777;
    manifest->owner_epoch    = 1;
    manifest->generation     = 1;
    manifest->logical_size   = payload.size();
    manifest->packs.push_back({.owner_tree_id = 777, .ref = segment->refs[0]});
    manifest->reference_segments.push_back({.owner_tree_id = 777,
                                            .object_id     = segment->object_id,
                                            .ref_count     = 1,
                                            .checksum      = reference_segment_checksum(*segment)});
    manifest->checksum = manifest_checksum(*manifest);
    ASSERT_TRUE(catalog->publish(777, 0, 1, manifest).ok());
    ChunkPageStore store({.tree_id = 777, .owner_epoch = 1, .pack_bytes = 9000, .iu_size = 1}, catalog, transport);
    std::vector<uint8_t> actual(payload.size());
    ASSERT_TRUE(store.read_at(0, actual.data(), actual.size()).ok());
    EXPECT_EQ(actual, payload);
    const uint8_t value = 0x61;
    ASSERT_TRUE(store.write_at(9000, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    ASSERT_TRUE(store.write_at(0, &value, 1).ok());
    ASSERT_TRUE(store.sync().ok());
    auto current = catalog->load(777);
    ASSERT_NE(current, nullptr);
    EXPECT_EQ(current->format_version, 7);
    bool saw_new = false;
    for (const auto &pack : current->packs) {
        if (pack.ref.frame_revision != 2) {
            continue;
        }
        saw_new = true;
        std::vector<uint8_t> bytes(chunk_framed_length(pack.ref.length, 2));
        ASSERT_TRUE(transport->read_mirror(pack.ref.chunk_id, 0, pack.ref.offset, bytes.data(), bytes.size()).ok());
        crowdb::protocol::ParsedFrame frame;
        ASSERT_EQ(crowdb::protocol::parse_frame(bytes, {.high = pack.ref.chunk_id.high, .low = pack.ref.chunk_id.low},
                                                &frame),
                  crowdb::protocol::FrameError::Ok);
        EXPECT_EQ(frame.header.magic, crowdb::protocol::FrameMagic::BtreePageV2);
        EXPECT_EQ(frame.group.role, crowdb::protocol::FrameRole::Ordinary);
    }
    EXPECT_TRUE(saw_new);
    std::vector<uint8_t> retained(framed.size());
    ASSERT_TRUE(transport->read_mirror(chunk, 0, 0, retained.data(), retained.size()).ok());
    EXPECT_EQ(retained, framed);
    EXPECT_EQ(segment->refs[0].frame_revision, 1);
}

} // namespace crowdb::tree::detail
