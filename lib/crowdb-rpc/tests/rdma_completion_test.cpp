// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_rdma_transport.h"
#include "crowdb-rpc/transport/epoll/epoll_engine.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)

#    include <gtest/gtest.h>

using namespace crowdb::rpc;

namespace
{
RdmaConfig test_config()
{
    RdmaConfig config;
    config.device                = "test-no-device";
    config.cq_depth              = 8;
    config.rq_depth              = 4;
    config.posted_receive_target = 4;
    config.poll_empty_budget     = 1;
    return config;
}

class RdmaCompletionTest : public ::testing::Test
{
  protected:
    RegisteredBufferPool        slots{16U << 20, 32};
    SystemBufferPool            buffers{32};
    TestCompletionQueue         queue;
    RdmaTransport               transport{test_config()};
    std::shared_ptr<Connection> connection = std::make_shared<Connection>(1, "test", &slots);

    void TearDown() override
    {
        TestRdmaTransport::set_cq(transport, nullptr);
        TestRdmaTransport::cancel(transport);
    }

    uint64_t send(const std::shared_ptr<Connection> &owner = nullptr)
    {
        Buffer *slot = slots.alloc(32);
        if (slot == nullptr)
            return 0;
        (void)slot->transition(BufferOwnership::Available, BufferOwnership::SendInFlight);
        auto *frame       = new OutFrame;
        frame->request_id = 239;
        frame->control    = buffers.alloc(1);
        frame->data       = buffers.alloc(16);
        const auto token  = TestRdmaTransport::send(transport, owner == nullptr ? connection : owner, slot, frame);
        if (token == 0) {
            (void)slot->transition(BufferOwnership::SendInFlight, BufferOwnership::Available);
            slot->release();
            frame->control->release();
            frame->release_data();
            delete frame;
        }
        return token;
    }
};

TEST_F(RdmaCompletionTest, ErrorsResolveOriginalOwnerWithoutTrustingOpcode)
{
    const std::array statuses{IBV_WC_WR_FLUSH_ERR, IBV_WC_LOC_PROT_ERR, IBV_WC_RETRY_EXC_ERR, IBV_WC_REM_ACCESS_ERR};
    for (const auto status : statuses) {
        auto     owner  = std::make_shared<Connection>(2, "error", &slots);
        uint32_t closes = 0;
        owner->set_on_close([&closes](Connection *) { ++closes; });
        const auto token = send(owner);
        ASSERT_NE(token, 0U);
        TestRdmaTransport::complete(transport, token, status);
        TestRdmaTransport::complete(transport, token, status);
        EXPECT_EQ(closes, 1U);
        EXPECT_FALSE(owner->is_open());
        EXPECT_TRUE(connection->is_open());
    }
    EXPECT_EQ(transport.stats().cq_errors.load(), 4U);
    EXPECT_EQ(transport.stats().flush_errors.load(), 1U);
    EXPECT_EQ(transport.stats().local_protection_errors.load(), 1U);
    EXPECT_EQ(transport.stats().retry_exceeded_errors.load(), 1U);
    EXPECT_EQ(transport.stats().remote_access_errors.load(), 1U);
    EXPECT_EQ(slots.quarantined_bytes(), 4U * RDMA_CONTROL_CAPACITY);
}

TEST_F(RdmaCompletionTest, BoundedWorkTableRejectsExhaustionAndStaleCompletions)
{
    std::array<uint64_t, 8> tokens{};
    for (auto &token : tokens) {
        token = send();
        ASSERT_NE(token, 0U);
    }
    EXPECT_EQ(send(), 0U);
    TestRdmaTransport::complete(transport, tokens.front());
    const auto replacement = send();
    ASSERT_NE(replacement, 0U);
    EXPECT_NE(replacement, tokens.front());
    TestRdmaTransport::complete(transport, tokens.front(), IBV_WC_LOC_PROT_ERR);
    EXPECT_TRUE(connection->is_open());
    EXPECT_EQ(transport.stats().cq_errors.load(), 0U);
    TestRdmaTransport::complete(transport, replacement);
    for (size_t index = 1; index < tokens.size(); ++index)
        TestRdmaTransport::complete(transport, tokens[index]);
}

TEST_F(RdmaCompletionTest, WorkRetainsConnectionUntilCompletion)
{
    auto                      owner    = std::make_shared<Connection>(3, "retained", &slots);
    std::weak_ptr<Connection> lifetime = owner;
    const auto                token    = send(owner);
    ASSERT_NE(token, 0U);
    owner.reset();
    EXPECT_FALSE(lifetime.expired());
    TestRdmaTransport::complete(transport, token);
    EXPECT_TRUE(lifetime.expired());
}

TEST_F(RdmaCompletionTest, ReceiveWaitsForFrameReleaseBeforeRecycling)
{
    Frame *received = nullptr;
    connection->set_on_frame([&received](Frame *frame, Connection *) { received = frame; });
    Buffer *slot = slots.alloc(RDMA_WIRE_SLOT_CAPACITY);
    ASSERT_NE(slot, nullptr);
    slot->allocation_data     = slot->data;
    slot->allocation_capacity = slot->capacity;
    Header header;
    header.data_size = 4;
    serialize_header(slot->data, header);
    std::memcpy(slot->data + HEADER_SIZE, "data", 4);
    ASSERT_TRUE(slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive));
    const auto token = TestRdmaTransport::receive(transport, connection, slot);
    TestRdmaTransport::complete(transport, token, IBV_WC_SUCCESS, HEADER_SIZE + 4);
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->data_buf->ownership.load(), BufferOwnership::CallerOwned);
    EXPECT_EQ(std::memcmp(received->data_buf->payload_data(), "data", 4), 0);
    EXPECT_EQ(slot->ref->load(), 1);
    delete received;
    EXPECT_EQ(slot->ref->load(), 0);
    EXPECT_EQ(slot->ownership.load(), BufferOwnership::Available);
}

TEST_F(RdmaCompletionTest, ReceiveErrorAndShutdownQuarantineUncertainBuffers)
{
    Buffer *slot = slots.alloc(RDMA_WIRE_SLOT_CAPACITY);
    ASSERT_NE(slot, nullptr);
    ASSERT_TRUE(slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive));
    const auto token = TestRdmaTransport::receive(transport, connection, slot);
    TestRdmaTransport::complete(transport, token, IBV_WC_LOC_PROT_ERR);
    EXPECT_EQ(slots.quarantined_bytes(), RDMA_WIRE_SLOT_CAPACITY);
    EXPECT_FALSE(connection->is_open());
    auto owner = std::make_shared<Connection>(4, "shutdown", &slots);
    ASSERT_NE(send(owner), 0U);
    TestRdmaTransport::cancel(transport);
    EXPECT_FALSE(owner->is_open());
    EXPECT_EQ(slots.quarantined_bytes(), RDMA_WIRE_SLOT_CAPACITY + RDMA_CONTROL_CAPACITY);
}

TEST_F(RdmaCompletionTest, CallerLeaseKeepsPayloadAfterFrameAndTransportRelease)
{
    auto   source   = std::make_unique<RdmaTransport>(test_config());
    Frame *received = nullptr;
    connection->set_on_frame([&received](Frame *frame, Connection *) { received = frame; });
    Buffer *slot = slots.alloc(RDMA_WIRE_SLOT_CAPACITY);
    ASSERT_NE(slot, nullptr);
    slot->allocation_data     = slot->data;
    slot->allocation_capacity = slot->capacity;
    Header header;
    header.data_size = 4;
    serialize_header(slot->data, header);
    std::memcpy(slot->data + HEADER_SIZE, "held", 4);
    ASSERT_TRUE(slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive));
    const auto token = TestRdmaTransport::receive(*source, connection, slot);
    TestRdmaTransport::complete(*source, token, IBV_WC_SUCCESS, HEADER_SIZE + 4);
    ASSERT_NE(received, nullptr);
    Buffer *lease = received->data_buf->ref_clone();
    ASSERT_NE(lease, nullptr);
    delete received;
    EXPECT_EQ(lease->ownership.load(), BufferOwnership::CallerOwned);
    EXPECT_EQ(lease->view_offset, HEADER_SIZE);
    source.reset();
    EXPECT_EQ(std::memcmp(lease->payload_data(), "held", 4), 0);
    lease->release();
    EXPECT_EQ(slot->ref->load(), 0);
}

TEST_F(RdmaCompletionTest, RejectsTruncatedOverflowedAndExtraWireBytes)
{
    for (int variant = 0; variant < 3; ++variant) {
        auto     owner     = std::make_shared<Connection>(5, "invalid", &slots);
        uint32_t published = 0;
        owner->set_on_frame([&published](Frame *frame, Connection *) {
            ++published;
            delete frame;
        });
        Buffer *slot = slots.alloc(RDMA_WIRE_SLOT_CAPACITY);
        ASSERT_NE(slot, nullptr);
        Header header;
        header.msg_size = variant == 1 ? UINT32_MAX : 0;
        serialize_header(slot->data, header);
        ASSERT_TRUE(slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive));
        const auto     token = TestRdmaTransport::receive(transport, owner, slot);
        const uint32_t bytes = variant == 0 ? HEADER_SIZE - 1 : HEADER_SIZE + 1;
        TestRdmaTransport::complete(transport, token, IBV_WC_SUCCESS, bytes);
        EXPECT_EQ(published, 0U);
        EXPECT_FALSE(owner->is_open());
    }
    EXPECT_EQ(slots.quarantined_bytes(), 3U * RDMA_WIRE_SLOT_CAPACITY);
}

TEST_F(RdmaCompletionTest, PollAfterArmingClosesNotificationRace)
{
    const auto token = send();
    ASSERT_NE(token, 0U);
    queue.on_arm.push_back(TestCompletionQueue::completion(token));
    TestRdmaTransport::set_cq(transport, &queue.cq);
    EXPECT_EQ(transport.drain_cq(8), 1);
    EXPECT_TRUE(queue.ready.empty());
    EXPECT_EQ(queue.arms, 1U);
}

TEST_F(RdmaCompletionTest, CmDisconnectPublishesAlreadyCompletedResponseBeforeFailingRequests)
{
    bool received = false;
    connection->set_on_frame([&received](Frame *frame, Connection *) {
        received = true;
        delete frame;
    });
    connection->set_on_rpc_close([&received](Connection *) { EXPECT_TRUE(received); });
    Buffer *slot = slots.alloc(RDMA_WIRE_SLOT_CAPACITY);
    ASSERT_NE(slot, nullptr);
    slot->allocation_data     = slot->data;
    slot->allocation_capacity = slot->capacity;
    serialize_header(slot->data, Header{});
    ASSERT_TRUE(slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive));
    const auto token      = TestRdmaTransport::receive(transport, connection, slot);
    auto       completion = TestCompletionQueue::completion(token);
    completion.byte_len   = HEADER_SIZE;
    queue.ready.push_back(completion);
    TestRdmaTransport::set_cq(transport, &queue.cq);
    TestRdmaTransport::disconnect(transport, connection.get());
    EXPECT_TRUE(received);
    EXPECT_FALSE(connection->is_open());
}

TEST_F(RdmaCompletionTest, BoundedCqBatchContinuesThroughEpollUntilDrained)
{
    for (int index = 0; index < 8; ++index) {
        const auto token = send();
        ASSERT_NE(token, 0U);
        queue.ready.push_back(TestCompletionQueue::completion(token));
    }
    TestRdmaTransport::set_cq(transport, &queue.cq);
    EpollEngine engine;
    ASSERT_EQ(engine.init(), 0);
    transport.register_events(&engine);
    EXPECT_EQ(transport.drain_cq(2), 2);
    TestRdmaTransport::set_cq_polling(transport, true);
    EXPECT_EQ(transport.handle_external_event(3, 2), 0);
    TestRdmaTransport::set_cq_polling(transport, false);
    int drained = 2;
    for (int iteration = 0; iteration < 4 && !queue.ready.empty(); ++iteration) {
        std::array<EngineEvent, 8> events{};
        const int                  count = engine.wait(events.data(), events.size(), 100);
        ASSERT_GT(count, 0);
        for (int index = 0; index < count; ++index)
            if (events[index].type == SocketEvent::External)
                drained += transport.handle_external_event(events[index].external_tag, 2);
    }
    EXPECT_EQ(drained, 8);
    EXPECT_TRUE(queue.ready.empty());
}

} // namespace

#endif
