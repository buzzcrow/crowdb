// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_packed_frame.h"
#include "crowdb-rpc/server/message.h"
#include "msg_type_generated.h"

#include <gtest/gtest.h>

using namespace crowdb::rpc;

TEST(RdmaPackedFrameTest, EnvelopeValidatesEveryFrameAndRejectsMalformedSuffixBeforePublication)
{
    SystemBufferPool     pool;
    std::vector<uint8_t> data;
    for (uint64_t id = 1; id <= 3; ++id) {
        auto *payload = pool.alloc(static_cast<uint32_t>(id * 127));
        ASSERT_NE(payload, nullptr);
        std::memset(payload->data, static_cast<int>(id), id * 127);
        payload->len   = static_cast<uint32_t>(id * 127);
        auto    *frame = build_out_frame(id, 100, build_ping_request(&pool, id, 0), payload);
        uint32_t bytes = 0;
        ASSERT_TRUE(rdma_packable_frame(*frame, 4096, bytes));
        const auto offset = data.size();
        data.resize(offset + bytes);
        copy_rdma_frame(data.data() + offset, *frame);
        release_rdma_frames(frame);
    }
    std::array<uint8_t, RDMA_CONTROL_CAPACITY> control;
    write_rdma_packed_control(control.data(), static_cast<uint32_t>(data.size()), 3);
    RdmaAggregate aggregate;
    ASSERT_TRUE(parse_rdma_packed_frame(control, data, 3, aggregate));
    ASSERT_EQ(aggregate.count, 3U);
    for (uint32_t index = 0; index < 3; ++index) {
        EXPECT_EQ(data[aggregate.frames[index].data_offset], index + 1);
    }
    EXPECT_FALSE(parse_rdma_packed_frame(control, data, 2, aggregate));
    EXPECT_FALSE(parse_rdma_packed_frame(control, std::span(data).first(data.size() - 1), 3, aggregate));
    control.back() = 1;
    EXPECT_FALSE(parse_rdma_packed_frame(control, data, 3, aggregate));
    control.back() = 0;
    data[aggregate.frames[2].offset] ^= 1;
    EXPECT_FALSE(parse_rdma_packed_frame(control, data, 3, aggregate));
}

TEST(RdmaPackedFrameTest, AlignmentSensitiveAndTransportMessagesRemainDirect)
{
    SystemBufferPool pool;
    auto            *frame = build_out_frame(1, 100, build_ping_request(&pool, 1, 0), nullptr);
    uint32_t         size  = 0;
    EXPECT_TRUE(rdma_packable_frame(*frame, 4096, size));
    for (uint16_t type : {static_cast<uint16_t>(proto::FBMsgType_EDiskReadRequest),
                          static_cast<uint16_t>(proto::FBMsgType_EDiskWriteRequest),
                          static_cast<uint16_t>(proto::FBMsgType_ERpcControlFragment), RDMA_PACKED_MSG_TYPE}) {
        frame->header.msg_type = type;
        EXPECT_FALSE(rdma_packable_frame(*frame, 4096, size));
    }
    release_rdma_frames(frame);
}
