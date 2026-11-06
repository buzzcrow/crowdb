// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "snapshot/commit_anchor.h"
#include "snapshot/metadata_decode.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace crowdb::tree::snapshot
{
namespace
{
void put_u32(std::span<uint8_t> bytes, size_t offset, uint32_t value)
{
    for (size_t i = 0; i < sizeof(value); ++i) {
        bytes[offset + i] = static_cast<uint8_t>(value >> (8 * i));
    }
}

void header_crc(std::span<uint8_t> bytes, size_t offset)
{
    put_u32(bytes, offset, common::crc32c(bytes.data(), offset));
}

std::vector<uint8_t> segment(std::vector<uint64_t> words, uint32_t live)
{
    std::vector<uint8_t> output;
    encode_segment_image(
        {.seg_idx = 3, .generation = 7, .slot_count = static_cast<uint32_t>(words.size()), .live_count = live}, words,
        &output);
    return output;
}

const std::vector<DirEntry> kEntries{
    {.seg_idx = 1, .generation = 2, .image_addr = 8192,  .image_len = 8228, .image_crc = 42 },
    {.seg_idx = 7, .generation = 5, .image_addr = 20480, .image_len = 8228, .image_crc = 123}
};

std::vector<uint8_t> directory(const std::vector<DirEntry> &entries = kEntries)
{
    std::vector<uint8_t> output;
    encode_segment_directory(entries, &output);
    return output;
}

void expect_entries(std::span<const DirEntry> actual, std::span<const DirEntry> expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        EXPECT_EQ(actual[i].seg_idx, expected[i].seg_idx);
        EXPECT_EQ(actual[i].generation, expected[i].generation);
        EXPECT_EQ(actual[i].image_addr, expected[i].image_addr);
        EXPECT_EQ(actual[i].image_len, expected[i].image_len);
        EXPECT_EQ(actual[i].image_crc, expected[i].image_crc);
    }
}
} // namespace

TEST(SnapshotMetadataDecode, SegmentMatchesExistingFormatsAndIgnoresIuPadding)
{
    for (const auto word : {slot_word::pack_unloaded(0, 2), slot_word::pack_page_ref(17, 3)}) {
        const std::vector<uint64_t> expected{0, word, slot_word::pack_unloaded(9, 1), 0};
        auto                        input = segment(expected, 2);
        input.resize(4096, 0xAA);
        std::array<uint64_t, 5> words{42, 42, 42, 42, 42};
        SegmentImageHeader      header;
        ASSERT_TRUE(decode_segment_into(input, &header, words).ok());
        EXPECT_TRUE(std::equal(expected.begin(), expected.end(), words.begin()));
        EXPECT_EQ(words.back(), 42U);
        EXPECT_EQ(header.seg_idx, 3U);
        EXPECT_EQ(header.generation, 7U);
        EXPECT_EQ(header.slot_count, expected.size());
        EXPECT_EQ(header.live_count, 2U);
        EXPECT_EQ(header.format_version, slot_word::is_page_ref(word) ? 2 : 1);
        EXPECT_EQ(header.body_crc, common::crc32c(input.data() + 32, expected.size() * sizeof(uint64_t)));
    }
    SegmentImageHeader header;
    EXPECT_TRUE(decode_segment_into(segment({}, 0), &header, {}).ok());
}

TEST(SnapshotMetadataDecode, SegmentTruncationAndOutputCapacityAreCheckedWithoutPartialWrites)
{
    const auto              input = segment({0, slot_word::pack_unloaded(1, 1)}, 1);
    std::array<uint64_t, 2> words{42, 42};
    SegmentImageHeader      header{.seg_idx = 99};
    for (size_t length = 0; length < input.size(); ++length) {
        EXPECT_EQ(decode_segment_into(std::span(input).first(length), &header, words).code(), Code::kCorruption);
        EXPECT_EQ(header.seg_idx, 99U);
        EXPECT_EQ(words[0], 42U);
        EXPECT_EQ(words[1], 42U);
    }
    EXPECT_EQ(decode_segment_into(input, &header, std::span(words).first(1)).code(), Code::kInvalidArgument);
    EXPECT_EQ(decode_segment_into(input, nullptr, words).code(), Code::kInvalidArgument);
}

TEST(SnapshotMetadataDecode, SegmentRejectsMagicCrcVersionFlagsCountsAndIllegalWords)
{
    const auto              valid = segment({slot_word::pack_unloaded(1, 1)}, 1);
    std::array<uint64_t, 1> words{42};
    SegmentImageHeader      header{.seg_idx = 99};
    for (const size_t offset : {0U, 28U, 32U, 40U}) {
        auto input = valid;
        input[offset] ^= 1;
        EXPECT_EQ(decode_segment_into(input, &header, words).code(), Code::kCorruption);
    }
    for (const auto [offset, value] : std::array<std::pair<size_t, uint32_t>, 5>{
             {{4, 3}, {4, 0x10001}, {20, UINT32_MAX}, {24, 0}, {24, 2}}
    }) {
        auto input = valid;
        put_u32(input, offset, value);
        header_crc(input, 28);
        EXPECT_EQ(decode_segment_into(input, &header, words).code(), Code::kCorruption);
    }
    for (const uint64_t word :
         {uint64_t{4}, uint64_t{6}, uint64_t{8}, slot_word::pack_unloaded(1, 0), slot_word::pack_page_ref(1, 0)}) {
        EXPECT_EQ(decode_segment_into(segment({word}, 1), &header, words).code(), Code::kCorruption);
    }
    auto legacy_ref = segment({slot_word::pack_page_ref(1, 1)}, 1);
    put_u32(legacy_ref, 4, 1);
    header_crc(legacy_ref, 28);
    EXPECT_EQ(decode_segment_into(legacy_ref, &header, words).code(), Code::kCorruption);
    EXPECT_EQ(header.seg_idx, 99U);
    EXPECT_EQ(words[0], 42U);
}

TEST(SnapshotMetadataDecode, DirectoryRoundTripsEveryWindowSizeIncludingEmptyDirectory)
{
    for (const auto &entries : {std::vector<DirEntry>{}, kEntries}) {
        const auto input = directory(entries);
        const auto bytes = static_cast<uint32_t>(input.size());
        uint32_t   count = UINT32_MAX;
        ASSERT_TRUE(directory_entry_count(std::span(input).first(16), bytes, &count).ok());
        ASSERT_EQ(count, entries.size());
        for (size_t window = 1; window <= input.size() + 1; ++window) {
            std::vector<DirEntry> output(entries.size());
            DirectoryDecoder      decoder(output, bytes);
            for (size_t offset = 0; offset < input.size(); offset += window) {
                ASSERT_TRUE(
                    decoder.next(std::span(input).subspan(offset, std::min(window, input.size() - offset))).ok());
            }
            ASSERT_TRUE(decoder.complete());
            EXPECT_TRUE(decoder.finish().ok());
            EXPECT_TRUE(decoder.next({}).ok());
            EXPECT_EQ(decoder.crc(), common::crc32c(input.data(), input.size()));
            expect_entries(output, entries);
        }
    }
}

TEST(SnapshotMetadataDecode, DirectoryRoundTripsEveryTwoWindowSplit)
{
    const auto input = directory();
    for (size_t split = 0; split <= input.size(); ++split) {
        std::array<DirEntry, 2> output{};
        DirectoryDecoder        decoder(output, static_cast<uint32_t>(input.size()));
        ASSERT_TRUE(decoder.next(std::span(input).first(split)).ok());
        ASSERT_TRUE(decoder.next(std::span(input).subspan(split)).ok());
        ASSERT_TRUE(decoder.finish().ok());
        expect_entries(output, kEntries);
    }
}

TEST(SnapshotMetadataDecode, DirectoryHeaderValidatesFlagsCountsAndExactLength)
{
    const auto valid = directory();
    const auto bytes = static_cast<uint32_t>(valid.size());
    uint32_t   count = 99;
    for (size_t length = 0; length < 16; ++length) {
        EXPECT_EQ(directory_entry_count(std::span(valid).first(length), bytes, &count).code(), Code::kCorruption);
    }
    for (const auto [offset, value] : std::array<std::pair<size_t, uint32_t>, 5>{
             {{0, 0}, {4, 2}, {4, 0x10001}, {8, UINT32_MAX}, {8, 1}}
    }) {
        auto input = valid;
        put_u32(input, offset, value);
        header_crc(input, 12);
        EXPECT_EQ(directory_entry_count(input, bytes, &count).code(), Code::kCorruption);
        std::array<DirEntry, 2> output{};
        DirectoryDecoder        decoder(output, bytes);
        EXPECT_EQ(decoder.next(input).code(), Code::kCorruption);
        EXPECT_FALSE(decoder.complete());
    }
    EXPECT_EQ(directory_entry_count(valid, bytes - 1, &count).code(), Code::kCorruption);
    EXPECT_EQ(directory_entry_count(valid, bytes + 1, &count).code(), Code::kCorruption);
    EXPECT_EQ(directory_entry_count(valid, bytes, nullptr).code(), Code::kInvalidArgument);
    EXPECT_EQ(count, 99U);
}

TEST(SnapshotMetadataDecode, DirectoryRejectsTruncatedOversizedInputAndInsufficientOutput)
{
    const auto input = directory();
    for (size_t length = 0; length < input.size(); ++length) {
        std::array<DirEntry, 2> output{};
        DirectoryDecoder        decoder(output, static_cast<uint32_t>(input.size()));
        EXPECT_TRUE(decoder.next(std::span(input).first(length)).ok());
        EXPECT_FALSE(decoder.complete());
        EXPECT_EQ(decoder.finish().code(), Code::kCorruption);
    }
    std::array<DirEntry, 2> output{};
    DirectoryDecoder        short_output{std::span(output).first(1), static_cast<uint32_t>(input.size())};
    EXPECT_EQ(short_output.next(input).code(), Code::kInvalidArgument);
    EXPECT_EQ(short_output.finish().code(), Code::kInvalidArgument);
    DirectoryDecoder decoder(output, static_cast<uint32_t>(input.size()));
    ASSERT_TRUE(decoder.next(input).ok());
    EXPECT_EQ(decoder.next(std::span(input).first(1)).code(), Code::kCorruption);
    EXPECT_EQ(decoder.next({}).code(), Code::kCorruption);
    EXPECT_FALSE(decoder.complete());
    for (const uint32_t bytes : {0U, 19U, 21U, UINT32_MAX}) {
        EXPECT_EQ(DirectoryDecoder(output, bytes).finish().code(), Code::kCorruption);
    }
}

TEST(SnapshotMetadataDecode, DirectoryRejectsHeaderBodyTrailerCrcAndReservedEntryBits)
{
    const auto valid = directory();
    for (const size_t offset : {12U, 16U, 80U}) {
        auto input = valid;
        input[offset] ^= 1;
        std::array<DirEntry, 2> output{};
        DirectoryDecoder        decoder(output, static_cast<uint32_t>(input.size()));
        EXPECT_EQ(decoder.next(input).code(), Code::kCorruption);
        EXPECT_EQ(decoder.finish().code(), Code::kCorruption);
    }
    auto input = valid;
    input[20]  = 1;
    put_u32(input, input.size() - 4, common::crc32c(input.data() + 16, input.size() - 20));
    std::array<DirEntry, 2> output{};
    DirectoryDecoder        decoder(output, static_cast<uint32_t>(input.size()));
    EXPECT_EQ(decoder.next(input).code(), Code::kCorruption);
}

TEST(SnapshotMetadataDecode, AnchorSpanDecoderChecksLengthMagicFormatAndCrcWithoutChangingFailedOutput)
{
    const CommitAnchor   anchor{.magic             = kAnchorMagic,
                                .format_version    = kFormatVersion,
                                .snapshot_seq      = 7,
                                .root_page_id      = 2,
                                .last_applied_slot = 99,
                                .next_page_id      = 9,
                                .segment_slots     = 1024,
                                .segdir_addr       = 8192,
                                .segdir_len        = 84,
                                .segdir_crc        = 42,
                                .leaf_count        = 4,
                                .inner_count       = 1};
    std::vector<uint8_t> input;
    encode_anchor(anchor, 4096, &input);
    CommitAnchor output;
    ASSERT_TRUE(decode_anchor(std::span(input).first(80), &output));
    EXPECT_EQ(output.snapshot_seq, 7U);
    EXPECT_EQ(output.last_applied_slot, 99U);
    EXPECT_EQ(output.leaf_count, 4U);
    EXPECT_EQ(output.inner_count, 1U);
    input[80] = 0xAA;
    EXPECT_TRUE(decode_anchor(input, &output));
    for (size_t length = 0; length < 80; ++length) {
        EXPECT_FALSE(decode_anchor(std::span(input).first(length), &output));
    }
    EXPECT_FALSE(decode_anchor(input, nullptr));
    for (const size_t offset : {0U, 4U, 8U, 76U}) {
        auto corrupt = input;
        corrupt[offset] ^= 1;
        if (offset < 8) {
            header_crc(corrupt, 76);
        }
        EXPECT_FALSE(decode_anchor(corrupt, &output));
        EXPECT_EQ(output.snapshot_seq, 7U);
    }
}
} // namespace crowdb::tree::snapshot
