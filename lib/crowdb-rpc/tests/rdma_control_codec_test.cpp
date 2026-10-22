// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_control_codec.h"
#include "../src/transport/rdma/rdma_direct_frame.h"
#include "crowdb-rpc/server/message.h"

#include <gtest/gtest.h>

using namespace crowdb::rpc;

TEST(ControlCodecTest, BoundedFragmentPreservesCommonCorrelationAndAllStreamMetadata)
{
    const ControlFragment                      source{.request_id           = 43,
                                                      .rpc_create_nano      = 11,
                                                      .response_create_nano = 12,
                                                      .stream_id            = 5,
                                                      .generation           = 7,
                                                      .sequence             = 2,
                                                      .offset               = 131072,
                                                      .total                = 131089,
                                                      .capacity             = RDMA_METADATA_CAPACITY,
                                                      .message_type         = 3701,
                                                      .message_flags        = FLAG_ONE_WAY,
                                                      .stream_flags         = CONTROL_STREAM_END};
    std::array<uint8_t, RDMA_CONTROL_CAPACITY> prefix{};
    ASSERT_TRUE(write_control_fragment(prefix, source, 17));
    Header header;
    ASSERT_TRUE(parse_rdma_direct_control(prefix, source.capacity, prefix.size() + 17, header));
    EXPECT_EQ(header.msg_type, RPC_CONTROL_FRAGMENT_MSG_TYPE);
    EXPECT_EQ(header.flags, FLAG_ONE_WAY);
    ControlFragment decoded;
    const auto      control = std::span(prefix).subspan(HEADER_SIZE);
    ASSERT_TRUE(parse_control_fragment(control, decoded));
    EXPECT_EQ(decoded.request_id, source.request_id);
    EXPECT_EQ(decoded.rpc_create_nano, source.rpc_create_nano);
    EXPECT_EQ(decoded.response_create_nano, source.response_create_nano);
    EXPECT_EQ(decoded.stream_id, source.stream_id);
    EXPECT_EQ(decoded.generation, source.generation);
    EXPECT_EQ(decoded.sequence, source.sequence);
    EXPECT_EQ(decoded.offset, source.offset);
    EXPECT_EQ(decoded.total, source.total);
    EXPECT_EQ(decoded.capacity, source.capacity);
    EXPECT_EQ(decoded.message_type, source.message_type);
    EXPECT_EQ(decoded.message_flags, source.message_flags);
    EXPECT_EQ(decoded.stream_flags, source.stream_flags);
    uint64_t id = 0, created = 0, response = 0;
    extract_control_fields(control.data(), control.size(), id, created, response);
    EXPECT_EQ(id, source.request_id);
    EXPECT_EQ(created, source.rpc_create_nano);
    EXPECT_EQ(response, source.response_create_nano);
    EXPECT_FALSE(write_control_fragment(prefix, source, source.capacity + 1));
}

TEST(ControlCodecTest, MalformedMetadataAndMismatchedOriginalControlDoNotPassTheBoundary)
{
    std::array<uint8_t, RDMA_CONTROL_CAPACITY> prefix{};
    ControlFragment                            fragment;
    EXPECT_FALSE(parse_control_fragment(prefix, fragment));
    for (size_t size = 0; size < 8; ++size) {
        EXPECT_FALSE(parse_control_fragment(std::span(prefix).first(size), fragment));
    }
    SystemBufferPool pool;
    auto            *control = build_ping_request(&pool, 43, 11);
    ASSERT_NE(control, nullptr);
    Frame frame;
    frame.control.assign(control->data, control->data + control->len);
    frame.request_id      = 43;
    frame.rpc_create_nano = 11;
    EXPECT_TRUE(control_identity_matches(frame));
    ++frame.request_id;
    EXPECT_FALSE(control_identity_matches(frame));
    --frame.request_id;
    ++frame.response_create_nano;
    EXPECT_FALSE(control_identity_matches(frame));
    frame.control.assign(32, 0xFF);
    EXPECT_FALSE(control_identity_matches(frame));
    control->release();
}
