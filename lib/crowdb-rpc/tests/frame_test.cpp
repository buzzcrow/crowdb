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
    ASSERT_EQ(encode_frame(FrameMagic::RepoSmallV1, chunk, payload, 42, &encoded), FrameError::Ok);

    ParsedFrame decoded{};
    ASSERT_EQ(parse_frame(encoded, chunk, &decoded), FrameError::Ok);
    EXPECT_EQ(decoded.header.magic, FrameMagic::RepoSmallV1);
    EXPECT_EQ(decoded.header.write_time_ms, 42);
    ASSERT_EQ(decoded.payload.size(), payload.size());
    EXPECT_TRUE(std::equal(decoded.payload.begin(), decoded.payload.end(), payload.begin()));

    encoded[kFrameHeaderPrefixBytes] ^= 1U;
    EXPECT_EQ(parse_frame(encoded, chunk, &decoded), FrameError::ChecksumMismatch);
}

TEST(FrameTest, MatchesCrossLanguageVector)
{
    constexpr FrameChunkId            chunk{.high = 7, .low = 11};
    constexpr std::array<uint8_t, 3>  payload{1, 2, 3};
    constexpr std::array<uint8_t, 37> expected{
        0x01, 0x01, 0x0E, 0x00, 0x03, 0x00, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x96, 0x16, 0xD6, 0x1E, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B,
    };
    std::vector<uint8_t> encoded;
    ASSERT_EQ(encode_frame(FrameMagic::RepoSmallV1, chunk, payload, 42, &encoded), FrameError::Ok);
    EXPECT_TRUE(std::equal(encoded.begin(), encoded.end(), expected.begin(), expected.end()));

    ParsedFrame decoded{};
    ASSERT_EQ(parse_frame(expected, chunk, &decoded), FrameError::Ok);
    EXPECT_TRUE(std::equal(decoded.payload.begin(), decoded.payload.end(), payload.begin(), payload.end()));
}

} // namespace
} // namespace crowdb::protocol
