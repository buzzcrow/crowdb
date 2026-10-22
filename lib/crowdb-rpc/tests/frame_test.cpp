// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-protocol/frame.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace crowdb::protocol
{
namespace
{

TEST(FrameTest, RoundTripAndDetectsCorruption)
{
    const FrameChunkId           chunk{.high = 7, .low = 11};
    const std::array<uint8_t, 3> payload{1, 2, 3};
    std::vector<uint8_t>         encoded;
    ASSERT_EQ(encode_frame(FrameMagic::RepoSmallV2, chunk, payload, 42, &encoded), FrameError::Ok);

    ParsedFrame decoded{};
    ASSERT_EQ(parse_frame(encoded, chunk, &decoded), FrameError::Ok);
    EXPECT_EQ(decoded.header.magic, FrameMagic::RepoSmallV2);
    EXPECT_EQ(decoded.header.write_time_ms, 42);
    ASSERT_EQ(decoded.payload.size(), payload.size());
    EXPECT_TRUE(std::equal(decoded.payload.begin(), decoded.payload.end(), payload.begin()));

    EXPECT_EQ(parse_frame(encoded, {.high = 7, .low = 12}, &decoded), FrameError::ChunkIdMismatch);

    encoded[kFrameV2HeaderBytes] ^= 1U;
    EXPECT_EQ(parse_frame(encoded, chunk, &decoded), FrameError::ChecksumMismatch);
}

TEST(FrameTest, MatchesCanonicalCrossLanguageVector)
{
    constexpr FrameChunkId            chunk{.high = 7, .low = 11};
    constexpr std::array<uint8_t, 3>  payload{1, 2, 3};
    constexpr std::array<uint8_t, 38> expected{0x02, 0x01, 0x0f, 0x00, 0x03, 0x00, 0x2a, 0x00, 0x00, 0x00,
                                               0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0xaf, 0x5a,
                                               0xa0, 0xae, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
                                               0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b};
    std::vector<uint8_t>              encoded;
    ASSERT_EQ(encode_frame(FrameMagic::RepoSmallV2, chunk, payload, 42, &encoded), FrameError::Ok);
    EXPECT_TRUE(std::equal(encoded.begin(), encoded.end(), expected.begin(), expected.end()));

    ParsedFrame decoded{};
    ASSERT_EQ(parse_frame(expected, chunk, &decoded), FrameError::Ok);
    EXPECT_TRUE(std::equal(decoded.payload.begin(), decoded.payload.end(), payload.begin(), payload.end()));
}

TEST(FrameTest, ValidatesConcatenatedPublicFramesWithoutLocationMetadata)
{
    const FrameChunkId           chunk{.high = 17, .low = 19};
    const std::array<uint8_t, 2> first{1, 2};
    const std::array<uint8_t, 3> second{3, 4, 5};
    std::vector<uint8_t>         encoded;
    ASSERT_EQ(encode_frame(FrameMagic::RepoLargeV2, chunk, first, 1, &encoded), FrameError::Ok);
    std::vector<uint8_t> next;
    ASSERT_EQ(encode_frame(FrameMagic::RepoLargeV2, chunk, second, 2, &next), FrameError::Ok);
    encoded.insert(encoded.end(), next.begin(), next.end());
    EXPECT_EQ(validate_frame_sequence(encoded), FrameError::Ok);

    encoded[encoded.size() - kFrameFooterBytes - 1] ^= 1U;
    EXPECT_EQ(validate_frame_sequence(encoded), FrameError::ChecksumMismatch);
}

} // namespace
} // namespace crowdb::protocol
