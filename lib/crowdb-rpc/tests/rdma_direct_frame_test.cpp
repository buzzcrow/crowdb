// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_aggregate.h"
#include "../src/transport/rdma/rdma_direct_frame.h"
#include "crowdb-rpc/server/message.h"

#include <gtest/gtest.h>

using namespace crowdb::rpc;

TEST(RdmaDirectFrameTest, FullDatapathLengthIncludesFixedControlRegion)
{
    SystemBufferPool pool;
    auto            *data = pool.alloc(RDMA_DATAPATH_CAPACITY);
    ASSERT_NE(data, nullptr);
    data->len        = RDMA_DATAPATH_CAPACITY;
    auto      *frame = build_out_frame(17, 100, build_ping_request(&pool, 17, 0), data);
    uint32_t   size  = 0;
    const auto limit = RDMA_DATAPATH_CAPACITY + RDMA_CONTROL_CAPACITY;
    EXPECT_FALSE(rdma_direct_frame_size(*frame, limit - 1, size));
    ASSERT_TRUE(rdma_direct_frame_size(*frame, limit, size));
    EXPECT_EQ(size, limit);
    std::array<uint8_t, RDMA_CONTROL_CAPACITY> control;
    write_rdma_direct_control(control.data(), *frame);
    Header decoded;
    ASSERT_TRUE(parse_rdma_direct_control(control, RDMA_DATAPATH_CAPACITY, size, decoded));
    EXPECT_EQ(decoded.data_size, RDMA_DATAPATH_CAPACITY);
    EXPECT_EQ(decoded.msg_size, RDMA_CONTROL_CAPACITY - HEADER_SIZE);
    uint64_t id = 0, created = 0, responded = 0;
    extract_control_fields(control.data() + HEADER_SIZE, decoded.msg_size, id, created, responded);
    EXPECT_EQ(id, 17U);
    EXPECT_FALSE(parse_rdma_direct_control(control, RDMA_DATAPATH_CAPACITY - 1, size, decoded));
    EXPECT_FALSE(parse_rdma_direct_control(control, RDMA_DATAPATH_CAPACITY, size - 1, decoded));
    EXPECT_FALSE(parse_rdma_direct_control(control, RDMA_DATAPATH_CAPACITY, size + 1, decoded));
    EXPECT_FALSE(parse_rdma_direct_control(std::span(control).first(1023), RDMA_DATAPATH_CAPACITY, size, decoded));
    control[0] ^= 1;
    EXPECT_FALSE(parse_rdma_direct_control(control, RDMA_DATAPATH_CAPACITY, size, decoded));
    release_rdma_frames(frame);
}

TEST(RdmaDirectFrameTest, ZeroDataAndControlBoundRejectMalformedFrames)
{
    SystemBufferPool pool;
    auto            *frame = build_out_frame(91, 100, build_ping_request(&pool, 91, 0), nullptr);
    uint32_t         size  = 0;
    EXPECT_TRUE(rdma_direct_frame_size(*frame, RDMA_CONTROL_CAPACITY, size));
    EXPECT_EQ(size, RDMA_CONTROL_CAPACITY);
    frame->control->len = RDMA_CONTROL_CAPACITY - HEADER_SIZE + 1;
    EXPECT_FALSE(rdma_direct_frame_size(*frame, UINT32_MAX, size));
    frame->control->len = 0;
    std::array<uint8_t, RDMA_CONTROL_CAPACITY> control;
    write_rdma_direct_control(control.data(), *frame);
    Header header;
    EXPECT_TRUE(parse_rdma_direct_control(control, 1024, 1024, header));
    header.msg_size = 0;
    serialize_header(control.data(), header);
    EXPECT_FALSE(parse_rdma_direct_control(control, 1024, 1024, header));
    release_rdma_frames(frame);
}
