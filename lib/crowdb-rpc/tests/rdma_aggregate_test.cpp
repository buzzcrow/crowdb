// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_aggregate.h"
#include "crowdb-rpc/server/message.h"

#include <gtest/gtest.h>

using namespace crowdb::rpc;

TEST(RdmaAggregateTest, ExactWireLimitIncludesEveryHeaderAndControlByte)
{
    SystemBufferPool pool;
    auto            *control = build_ping_request(&pool, 17, 0);
    auto            *data    = pool.alloc(128);
    ASSERT_NE(control, nullptr);
    ASSERT_NE(data, nullptr);
    data->write("data", 4);
    auto          *frame    = build_out_frame(17, 100, control, data);
    const uint32_t expected = HEADER_SIZE + control->len + data->len;
    uint32_t       size     = 0;
    EXPECT_FALSE(rdma_frame_size(*frame, expected - 1, size));
    ASSERT_TRUE(rdma_frame_size(*frame, expected, size));
    EXPECT_EQ(size, expected);
    std::vector<uint8_t> wire(size * 2);
    copy_rdma_frame(wire.data(), *frame);
    copy_rdma_frame(wire.data() + size, *frame);
    RdmaAggregate decoded;
    ASSERT_TRUE(parse_rdma_aggregate(wire, decoded));
    EXPECT_EQ(decoded.count, 2U);
    EXPECT_EQ(decoded.frames[1].offset, expected);
    EXPECT_EQ(std::memcmp(wire.data() + decoded.frames[1].data_offset, "data", 4), 0);
    wire.push_back(0);
    EXPECT_FALSE(parse_rdma_aggregate(wire, decoded));
    wire.resize(size * 2);
    wire[size] ^= 1;
    EXPECT_FALSE(parse_rdma_aggregate(wire, decoded));
    release_rdma_frames(frame);
}

TEST(RdmaAggregateTest, FrameCountAndControlArithmeticAreBounded)
{
    Header               header;
    std::vector<uint8_t> wire(HEADER_SIZE * RDMA_MAX_AGGREGATE_FRAMES);
    for (size_t offset = 0; offset < wire.size(); offset += HEADER_SIZE)
        serialize_header(wire.data() + offset, header);
    RdmaAggregate decoded;
    ASSERT_TRUE(parse_rdma_aggregate(wire, decoded));
    EXPECT_EQ(decoded.count, RDMA_MAX_AGGREGATE_FRAMES);
    wire.resize(wire.size() + HEADER_SIZE);
    serialize_header(wire.data() + wire.size() - HEADER_SIZE, header);
    EXPECT_FALSE(parse_rdma_aggregate(wire, decoded));
    header.msg_size  = UINT32_MAX;
    header.data_size = UINT32_MAX;
    serialize_header(wire.data(), header);
    EXPECT_FALSE(parse_rdma_aggregate(wire, decoded));
}

TEST(RdmaAggregateTest, IndependentSlicesKeepOneParentLeaseUntilLastRelease)
{
    RegisteredBufferPool pool(1024, 1);
    auto                *parent = pool.alloc(1024);
    ASSERT_NE(parent, nullptr);
    std::memcpy(parent->data, "firstmiddlelast", 15);
    auto *first  = parent->slice(0, 5);
    auto *middle = parent->slice(5, 6);
    auto *last   = parent->slice(11, 4);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(middle, nullptr);
    ASSERT_NE(last, nullptr);
    EXPECT_EQ(parent->slice(1023, 2), nullptr);
    auto *held = middle->ref_clone();
    parent->release();
    middle->release();
    first->release();
    last->release();
    EXPECT_EQ(pool.alloc(1024), nullptr);
    EXPECT_EQ(std::memcmp(held->payload_data(), "middle", 6), 0);
    EXPECT_EQ(held->capacity, 6U);
    held->release();
    auto *recycled = pool.alloc(1024);
    ASSERT_NE(recycled, nullptr);
    recycled->release();
}
