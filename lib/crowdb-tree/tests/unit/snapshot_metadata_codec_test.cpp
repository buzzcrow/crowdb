// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "snapshot/metadata_codec.h"

#include <gtest/gtest.h>

namespace crowdb::tree::snapshot
{
TEST(SnapshotMetadataCodec, SegmentMatchesExistingFormatsWithoutTouchingPadding)
{
    for (const auto word : {slot_word::pack_unloaded(9, 2), slot_word::pack_page_ref(17, 3)}) {
        const SegmentImageHeader    header{.seg_idx = 4, .generation = 9, .slot_count = 4, .live_count = 1};
        const std::vector<uint64_t> words{slot_word::kEmpty, word, 0, 0};
        std::vector<uint8_t>        expected;
        uint32_t                    expected_crc = 0;
        encode_segment_image(header, words, &expected, &expected_crc);
        std::vector<uint8_t> output(expected.size() + 11, 0xAA);
        uint32_t             crc = 0;
        ASSERT_TRUE(encode_segment_into(header, words, output, &crc).ok());
        EXPECT_TRUE(std::equal(expected.begin(), expected.end(), output.begin()));
        EXPECT_EQ(crc, expected_crc);
        EXPECT_EQ(output.back(), 0xAA);
    }
}

TEST(SnapshotMetadataCodec, RejectsInvalidSegmentAndInsufficientOutput)
{
    SegmentImageHeader    header{.slot_count = 1, .live_count = 1};
    std::vector<uint64_t> words{slot_word::pack_unloaded(1, 1)};
    std::vector<uint8_t>  output(segment_image_encoded_size(1) - 1);
    uint32_t              crc = 0;
    EXPECT_EQ(encode_segment_into(header, words, output, &crc).code(), Code::kInvalidArgument);
    output.resize(output.size() + 1);
    header.live_count = 2;
    EXPECT_EQ(encode_segment_into(header, words, output, &crc).code(), Code::kInvalidArgument);
    header.live_count = 1;
    EXPECT_EQ(encode_segment_into(header, {}, output, &crc).code(), Code::kInvalidArgument);
    EXPECT_EQ(encode_segment_into(header, words, output, nullptr).code(), Code::kInvalidArgument);
}

TEST(SnapshotMetadataCodec, DirectoryMatchesExistingEncodingAcrossEveryPartialPiece)
{
    const std::vector<DirEntry> entries{
        {.seg_idx = 0, .generation = 2, .image_addr = 8192,  .image_len = 8228, .image_crc = 42 },
        {.seg_idx = 7, .generation = 5, .image_addr = 20480, .image_len = 8228, .image_crc = 123}
    };
    for (const auto count : {size_t{0}, entries.size()}) {
        const std::vector<DirEntry> input(entries.begin(), entries.begin() + static_cast<ptrdiff_t>(count));
        std::vector<uint8_t>        expected;
        encode_segment_directory(input, &expected);
        for (size_t window = 1; window <= 85; ++window) {
            DirectoryEncoder     encoder(input);
            std::vector<uint8_t> output;
            std::vector<uint8_t> piece(window, 0xAA);
            while (!encoder.complete()) {
                std::ranges::fill(piece, uint8_t{0xAA});
                size_t written = 0;
                ASSERT_TRUE(encoder.next(piece, &written).ok());
                ASSERT_GT(written, 0U);
                output.insert(output.end(), piece.begin(), piece.begin() + static_cast<ptrdiff_t>(written));
                if (written < window) {
                    EXPECT_EQ(piece[written], 0xAA);
                }
            }
            EXPECT_EQ(output, expected);
            EXPECT_EQ(encoder.crc(), common::crc32c(expected.data(), expected.size()));
            size_t written = 1;
            ASSERT_TRUE(encoder.next(piece, &written).ok());
            EXPECT_EQ(written, 0U);
        }
    }
}

TEST(SnapshotMetadataCodec, AnchorMatchesExistingEncoderAndReaderChecksActualCrcField)
{
    CommitAnchor         anchor{.magic             = kAnchorMagic,
                                .format_version    = kFormatVersion,
                                .snapshot_seq      = 3,
                                .root_page_id      = 17,
                                .last_applied_slot = 50,
                                .next_page_id      = 23,
                                .segment_slots     = 1024,
                                .segdir_addr       = 8192,
                                .segdir_len        = 52,
                                .segdir_crc        = 42,
                                .leaf_count        = 7,
                                .inner_count       = 2};
    std::vector<uint8_t> expected;
    encode_anchor(anchor, 4096, &expected);
    std::vector<uint8_t> output(4096, 0xAA);
    ASSERT_TRUE(encode_anchor_into(anchor, output).ok());
    EXPECT_EQ(output, expected);
    MemPageStore store(4096);
    // Padding is outside the CRC field. Previously the reader mistook offset
    // 80 for the CRC rather than checking the encoded CRC at offset 76.
    output[80] = 0xAA;
    ASSERT_TRUE(store.write_at(0, output.data(), output.size()).ok());
    std::vector<CommitAnchor> anchors;
    ASSERT_TRUE(read_valid_anchors(store, 4096, &anchors).ok());
    ASSERT_EQ(anchors.size(), 1U);
    EXPECT_EQ(anchors[0].snapshot_seq, 3U);
    EXPECT_EQ(anchors[0].last_applied_slot, 50U);
    output[76] ^= 1;
    ASSERT_TRUE(store.write_at(0, output.data(), output.size()).ok());
    ASSERT_TRUE(read_valid_anchors(store, 4096, &anchors).ok());
    EXPECT_TRUE(anchors.empty());
    EXPECT_EQ(encode_anchor_into(anchor, std::span(output).first(4095)).code(), Code::kInvalidArgument);
}
} // namespace crowdb::tree::snapshot
