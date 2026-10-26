// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_aggregate.h"
#include "common/test_rdma_transport.h"
#include "crowdb-rpc/server/message.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

using namespace crowdb::rpc;

namespace
{
RdmaConfig test_receive_budget_config()
{
    RdmaConfig config;
    config.device   = "test-no-device";
    config.sq_depth = config.rq_depth = config.posted_receive_target = 4;
    config.cq_depth                                                  = 8;
    config.poll_empty_budget                                         = 1;
    return config;
}

class RdmaReceiveBudgetTest : public ::testing::Test
{
  protected:
    RegisteredBufferPool        slots{4U << 20, 8};
    SystemBufferPool            buffers;
    TestCompletionQueue         queue;
    RdmaTransport               transport{test_receive_budget_config()};
    std::shared_ptr<Connection> connection = std::make_shared<Connection>(1, "receiver", &slots);
    std::vector<std::unique_ptr<Buffer, void (*)(Buffer *)>> held;
    uint64_t                                                 received   = 0;
    uint32_t                                                 data_bytes = 1;

    void SetUp() override
    {
        (void)TestRdmaTransport::bind_cq(connection, 0);
        TestRdmaTransport::set_cq(transport, &queue.cq);
        connection->set_on_frame([&](Frame *frame, Connection *) {
            EXPECT_EQ(frame->request_id, ++received);
            EXPECT_EQ(frame->data_buf->len, data_bytes);
            const auto value = static_cast<uint8_t>(frame->request_id);
            EXPECT_TRUE(std::all_of(frame->data_buf->payload_data(), frame->data_buf->payload_data() + data_bytes,
                                    [value](uint8_t byte) { return byte == value; }));
            delete frame;
        });
    }

    void TearDown() override
    {
        connection->close();
        transport.stop();
        TestRdmaTransport::set_cq(transport, nullptr);
        TestRdmaTransport::cancel(transport);
        held.clear();
    }

    void enqueue(uint32_t count, uint64_t first_id)
    {
        auto *slot = slots.alloc(RDMA_METADATA_CAPACITY);
        ASSERT_NE(slot, nullptr);
        held.emplace_back(slot, [](Buffer *buffer) { buffer->release(); });
        slot->len = 0;
        for (uint64_t id = first_id; id < first_id + count; ++id) {
            auto *data = buffers.alloc(data_bytes);
            ASSERT_NE(data, nullptr);
            std::memset(data->data, static_cast<uint8_t>(id), data_bytes);
            data->len                                            = data_bytes;
            auto                                         release = [](OutFrame *frame) { release_rdma_frames(frame); };
            std::unique_ptr<OutFrame, decltype(release)> frame(
                build_out_frame(id, 100, build_ping_request(&buffers, id, 0), data), release);
            uint32_t bytes = 0;
            ASSERT_TRUE(rdma_frame_size(*frame, slot->capacity - slot->len, bytes));
            copy_rdma_frame(slot->data + slot->len, *frame);
            slot->len += bytes;
        }
        ASSERT_TRUE(slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive));
        auto      *work_slot = slot->ref_clone();
        const auto token     = TestRdmaTransport::receive(transport, connection, work_slot);
        if (token == 0) {
            work_slot->release();
            FAIL() << "receive work table exhausted";
        }
        auto completion     = TestCompletionQueue::completion(token);
        completion.byte_len = slot->len;
        queue.ready.push_back(completion);
    }
};

TEST_F(RdmaReceiveBudgetTest, AggregateFrameCountBoundsCallbacksAndContinuesNextRoundInOrder)
{
    enqueue(256, 1);
    enqueue(1, 257);
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(received, 256U);
    EXPECT_EQ(transport.stats().pending_deferred_receives.load(), 1U);
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(received, 257U);
    EXPECT_EQ(transport.stats().pending_deferred_receives.load(), 0U);
    EXPECT_EQ(transport.stats().protocol_errors.load(), 0U);
}

TEST_F(RdmaReceiveBudgetTest, WireBytesBoundCallbacksAndContinueNextRoundWithoutConsumingSlots)
{
    data_bytes = 400;
    TestRdmaTransport::set_receive_budget_for_tests(transport, 512);
    enqueue(1, 1);
    enqueue(1, 2);
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(received, 1U);
    EXPECT_EQ(transport.stats().pending_deferred_receives.load(), 1U);
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(received, 2U);
    EXPECT_EQ(transport.stats().pending_deferred_receives.load(), 0U);
    EXPECT_EQ(transport.stats().protocol_errors.load(), 0U);
}
} // namespace
#endif
