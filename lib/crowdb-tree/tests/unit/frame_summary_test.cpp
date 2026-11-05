// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/frame_page.h"

#include <gtest/gtest.h>

#include <array>

using namespace crowdb::tree;

TEST(FrameSummary, ReadsExistingVersionOneCapacityWithoutExactFlagByte)
{
    std::array<uint8_t, 4096> frame{};
    LeafFrameBuilder          builder(frame.data(), frame.size());
    builder.finish(7, kInvalidPageId);
    // These are the bytes produced by the original version-one writer:
    // the last byte of the capacity word is overwritten with the exact flag.
    frame_put_u64(frame.data(), 64, 3);
    frame_put_u64(frame.data(), 120, 4096);
    frame[127] = 1;
    frame_restamp_crc(frame.data(), frame.size());
    ASSERT_TRUE(frame_validate(frame.data(), frame.size()));
    const auto summary = frame_summary(frame.data());
    EXPECT_TRUE(summary.exact);
    EXPECT_EQ(summary.live_kv, 3U);
    EXPECT_EQ(summary.reachable_page_capacity_bytes, 4096U);
}

TEST(FrameSummary, CapacityRoundTripsThroughEveryRepresentableByte)
{
    std::array<uint8_t, 4096> frame{};
    LeafFrameBuilder          builder(frame.data(), frame.size());
    builder.finish(7, kInvalidPageId);
    for (const uint64_t capacity :
         {uint64_t{0}, uint64_t{4096}, uint64_t{0x00f1e2d3c4b5a697}, uint64_t{0x00ffffffffffffff}}) {
        const PageSummary input{.live_kv                       = 2,
                                .live_key_bytes                = 5,
                                .live_value_bytes              = 8,
                                .live_logical_bytes            = 13,
                                .reachable_leaf_pages          = 1,
                                .reachable_inner_pages         = 2,
                                .reachable_overflow_pages      = 3,
                                .reachable_page_capacity_bytes = capacity,
                                .exact                         = true};
        frame_set_summary(frame.data(), frame.size(), input);
        ASSERT_TRUE(frame_validate(frame.data(), frame.size()));
        const auto output = frame_summary(frame.data());
        EXPECT_TRUE(output.exact);
        EXPECT_EQ(output.live_kv, input.live_kv);
        EXPECT_EQ(output.live_key_bytes, input.live_key_bytes);
        EXPECT_EQ(output.live_value_bytes, input.live_value_bytes);
        EXPECT_EQ(output.live_logical_bytes, input.live_logical_bytes);
        EXPECT_EQ(output.reachable_leaf_pages, input.reachable_leaf_pages);
        EXPECT_EQ(output.reachable_inner_pages, input.reachable_inner_pages);
        EXPECT_EQ(output.reachable_overflow_pages, input.reachable_overflow_pages);
        EXPECT_EQ(output.reachable_page_capacity_bytes, capacity);
    }
}

TEST(FrameSummary, UnrepresentableCapacityAndUnknownFlagsCannotClaimExactCounts)
{
    std::array<uint8_t, 4096> frame{};
    LeafFrameBuilder          builder(frame.data(), frame.size());
    builder.finish(7, kInvalidPageId);
    for (const uint64_t capacity : {uint64_t{1} << 56, UINT64_MAX}) {
        frame_set_summary(frame.data(), frame.size(),
                          {.live_kv = 5, .reachable_page_capacity_bytes = capacity, .exact = true});
        EXPECT_TRUE(frame_validate(frame.data(), frame.size()));
        EXPECT_FALSE(frame_summary(frame.data()).exact);
    }
    frame_set_summary(frame.data(), frame.size(),
                      {.live_kv = 5, .reachable_page_capacity_bytes = 4096, .exact = false});
    EXPECT_FALSE(frame_summary(frame.data()).exact);
    for (const uint8_t flag : {2, 3, 255}) {
        frame[127] = flag;
        frame_restamp_crc(frame.data(), frame.size());
        EXPECT_TRUE(frame_validate(frame.data(), frame.size()));
        EXPECT_FALSE(frame_summary(frame.data()).exact);
    }
}
