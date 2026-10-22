// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-protocol/frame.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace crowdb::protocol
{
namespace
{

constexpr FrameChunkId kChunk{.high = 7, .low = 11};

TEST(FrameRevisionTest, ReadsSharedVectorsAndEmitsCanonicalNewRevision)
{
    const auto    path = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() /
                         "crowdb-protocol/tests/data/frame-vectors.txt";
    std::ifstream input(path);
    ASSERT_TRUE(input.is_open()) << path;
    std::string line;
    size_t      count = 0;
    while (std::getline(input, line)) {
        if (line.empty() || line.front() == '#') {
            continue;
        }
        std::istringstream fields(line);
        std::string        name;
        std::string        hex;
        fields >> name >> hex;
        std::vector<uint8_t> bytes;
        for (size_t offset = 0; offset < hex.size(); offset += 2) {
            bytes.push_back(static_cast<uint8_t>(std::stoul(hex.substr(offset, 2), nullptr, 16)));
        }
        ParsedFrame parsed;
        ASSERT_EQ(parse_frame(bytes, kChunk, &parsed), FrameError::Ok) << name;
        EXPECT_EQ(parsed.header.write_time_ms, 42);
        ASSERT_EQ(parsed.payload.size(), 3);
        EXPECT_EQ(parsed.payload[0], 1);
        EXPECT_EQ(parsed.payload[1], 2);
        EXPECT_EQ(parsed.payload[2], 3);
        std::vector<uint8_t> encoded;
        if (name.starts_with("ordinary")) {
            ASSERT_EQ(encode_frame(parsed.header.magic, kChunk, parsed.payload, 42, &encoded), FrameError::Ok);
            EXPECT_EQ(encoded, bytes) << name;
        }
        else if (name == "head" || name == "follower") {
            ASSERT_EQ(encode_group_frame(kChunk, parsed.payload, 42, parsed.group, &encoded), FrameError::Ok);
            EXPECT_EQ(encoded, bytes) << name;
        }
        ++count;
    }
    EXPECT_EQ(count, 8);
}

TEST(FrameRevisionTest, OrdinaryNewRevisionHasBoundedCanonicalFrames)
{
    for (const auto magic :
         {FrameMagic::RepoSmallV2, FrameMagic::RepoLargeV2, FrameMagic::BtreePageV2, FrameMagic::PageIndexV2}) {
        const std::vector<uint8_t> payload(kMaxFrameV2PayloadBytes + 17, 0x65);
        std::vector<uint8_t>       encoded;
        ASSERT_EQ(encode_frames(magic, kChunk, payload, 42, &encoded), FrameError::Ok);
        EXPECT_EQ(encoded.size(), framed_physical_length(payload.size(), kFrameV2HeaderBytes));
        ParsedFrame first;
        ASSERT_EQ(parse_frame(encoded, kChunk, &first), FrameError::Ok);
        EXPECT_EQ(first.physical_length, kMaxFrameBytes);
        EXPECT_EQ(first.group.role, FrameRole::Ordinary);
        std::vector<uint8_t> decoded;
        ASSERT_EQ(parse_frames(encoded, kChunk, magic, payload.size(), &decoded), FrameError::Ok);
        EXPECT_EQ(decoded, payload);
        EXPECT_EQ(validate_frame_sequence(encoded), FrameError::Ok);
    }
    std::vector<uint8_t> encoded;
    EXPECT_EQ(encode_frame(FrameMagic::StreamV2, kChunk, std::array<uint8_t, 1>{1}, 42, &encoded),
              FrameError::InvalidRole);
}

TEST(FrameRevisionTest, InterruptedAppendNeverBecomesACompleteGroup)
{
    for (const size_t length : {size_t{1}, kMaxFrameGroupPayloadBytes, kMaxFrameGroupPayloadBytes + 1,
                                (3 * kMaxFrameGroupPayloadBytes) + 17}) {
        const std::vector<uint8_t> payload(length, 0x51);
        std::vector<uint8_t>       encoded;
        ASSERT_EQ(encode_append_group(kChunk, 4096, payload, 42, &encoded), FrameError::Ok);
        ParsedAppendGroup group;
        ASSERT_EQ(parse_append_group(encoded, kChunk, 4096, &group), FrameError::Ok);
        EXPECT_EQ(group.logical_length, length);
        EXPECT_EQ(group.physical_length, encoded.size());
        EXPECT_EQ(group.frame_count,
                  (length / kMaxFrameGroupPayloadBytes) + (length % kMaxFrameGroupPayloadBytes != 0 ? 1 : 0));
        const std::span<const uint8_t> bytes(encoded);
        for (size_t boundary = 0; boundary < encoded.size(); boundary += kMaxFrameBytes) {
            for (const size_t end :
                 {boundary == 0 ? 0 : boundary - 1, boundary, std::min(boundary + 1, encoded.size() - 1)}) {
                EXPECT_NE(parse_append_group(bytes.first(end), kChunk, 4096, &group), FrameError::Ok)
                    << length << '/' << end;
            }
        }
        EXPECT_NE(parse_append_group(bytes.first(bytes.size() - 1), kChunk, 4096, &group), FrameError::Ok);
        if (length > kMaxFrameGroupPayloadBytes) {
            EXPECT_EQ(parse_append_group(bytes, kChunk, 4097, &group), FrameError::InvalidGroup);
        }
    }
}

TEST(FrameRevisionTest, ValidFramesCannotBeReorderedOrBorrowedFromAnotherAppend)
{
    const std::vector<uint8_t> payload(3 * kMaxFrameGroupPayloadBytes, 0x51);
    std::vector<uint8_t>       encoded;
    ASSERT_EQ(encode_append_group(kChunk, 4096, payload, 42, &encoded), FrameError::Ok);
    auto reordered = encoded;
    std::swap_ranges(reordered.begin() + kMaxFrameBytes, reordered.begin() + 2 * kMaxFrameBytes,
                     reordered.begin() + 2 * kMaxFrameBytes);
    EXPECT_EQ(parse_append_group(reordered, kChunk, 4096, nullptr), FrameError::InvalidGroup);
    std::vector<uint8_t> other;
    ASSERT_EQ(encode_append_group(kChunk, 8192, payload, 42, &other), FrameError::Ok);
    std::copy(other.begin() + kMaxFrameBytes, other.begin() + 2 * kMaxFrameBytes, encoded.begin() + kMaxFrameBytes);
    EXPECT_EQ(parse_append_group(encoded, kChunk, 4096, nullptr), FrameError::InvalidGroup);
    encoded[kMaxFrameBytes + 15] ^= 1U;
    EXPECT_NE(parse_append_group(encoded, kChunk, 4096, nullptr), FrameError::Ok);
}

TEST(FrameRevisionTest, BoundedValidatorRequiresExactOffsetsAndEveryFollower)
{
    const std::vector<uint8_t> payload(kMaxFrameGroupPayloadBytes + 1, 7);
    std::vector<uint8_t>       encoded;
    ASSERT_EQ(encode_append_group(kChunk, 4096, payload, 42, &encoded), FrameError::Ok);
    ParsedFrame head;
    ASSERT_EQ(parse_frame(encoded, kChunk, &head), FrameError::Ok);
    AppendGroupValidator validator;
    ASSERT_EQ(validator.start(4096, head), FrameError::Ok);
    EXPECT_FALSE(validator.complete());
    EXPECT_EQ(validator.finish(nullptr), FrameError::InvalidGroup);
    ParsedFrame tail;
    ASSERT_EQ(parse_frame(std::span<const uint8_t>(encoded).subspan(head.physical_length), kChunk, &tail),
              FrameError::Ok);
    EXPECT_EQ(validator.push(4097 + head.physical_length, tail), FrameError::InvalidGroup);
    ASSERT_EQ(validator.push(4096 + head.physical_length, tail), FrameError::Ok);
    EXPECT_TRUE(validator.complete());
    EXPECT_EQ(validator.push(4096 + encoded.size(), tail), FrameError::InvalidGroup);
    ParsedAppendGroup group;
    ASSERT_EQ(validator.finish(&group), FrameError::Ok);
    EXPECT_EQ(group.physical_length, encoded.size());
}

TEST(FrameRevisionTest, RejectsMalformedExtensionsAndAmbiguousCrc)
{
    std::vector<uint8_t> encoded;
    ASSERT_EQ(encode_frame(FrameMagic::RepoSmallV2, kChunk, std::array<uint8_t, 3>{1, 2, 3}, 42, &encoded),
              FrameError::Ok);
    const size_t   footer     = encoded.size() - kFrameFooterBytes;
    const uint32_t prefix_crc = crowdb::common::crc32c(encoded.data(), footer);
    const uint32_t legacy_crc = crowdb::common::crc32c_update(prefix_crc, encoded.data() + footer + 4, 16);
    for (size_t index = 0; index < 4; ++index) {
        encoded[footer + index] = static_cast<uint8_t>(legacy_crc >> (index * 8U));
    }
    EXPECT_EQ(parse_frame(encoded, kChunk, nullptr), FrameError::ChecksumMismatch);
    encoded[14] = 1;
    EXPECT_EQ(parse_frame(encoded, kChunk, nullptr), FrameError::InvalidRole);
    encoded[2] = 14;
    EXPECT_EQ(parse_frame(encoded, kChunk, nullptr), FrameError::InvalidPayloadOffset);
}

} // namespace
} // namespace crowdb::protocol
