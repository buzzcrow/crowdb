// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_aggregate.h"
#include "common/test_rdma_device.h"
#include "common/test_rdma_transport.h"
#include "crowdb-rpc/server/message.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

using namespace crowdb::rpc;

namespace
{
RdmaConfig test_send_config()
{
    RdmaConfig config;
    config.device             = "test-no-device";
    config.wire_slot_capacity = 1024;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 4;
    config.cq_depth                                                  = 8;
    config.send_arena_bytes = config.receive_arena_bytes = 4096;
    config.poll_empty_budget                             = 1;
    config.aggregation_enabled                           = true;
    config.send_queue_capacity                           = 4;
    return config;
}

class RdmaSendTest : public ::testing::Test
{
  protected:
    TestCompletionQueue               queue;
    ibv_context                       context{};
    ibv_qp                            qp{};
    SystemBufferPool                  buffers;
    std::shared_ptr<TestRdmaDevice>   device;
    RdmaTransport                     transport{test_send_config(), &buffers};
    std::shared_ptr<Connection>       connection = std::make_shared<Connection>(1, "sender", &buffers);
    std::vector<std::vector<uint8_t>> sends;
    bool                              fail_send = false;
    uint32_t                          fail_at   = UINT32_MAX;

    void SetUp() override
    {
        device = TestRdmaDevice::open();
        if (device == nullptr) {
            GTEST_SKIP() << "no accessible HCA for registered send slots";
        }
        auto pool = std::make_unique<RdmaBufferPool>(device->pd, 4096, 4, device);
        ASSERT_TRUE(pool->ready());
        qp.context            = &context;
        qp.qp_context         = this;
        context.ops.post_send = [](ibv_qp *qp, ibv_send_wr *wr, ibv_send_wr **bad) {
            auto &self = *static_cast<RdmaSendTest *>(qp->qp_context);
            if (self.fail_send) {
                *bad = wr;
                return EIO;
            }
            uint32_t index = 0;
            for (auto *entry = wr; entry != nullptr; entry = entry->next, ++index) {
                if (index == self.fail_at) {
                    *bad = entry;
                    return EIO;
                }
                const auto *data = reinterpret_cast<const uint8_t *>(entry->sg_list[0].addr);
                self.sends.emplace_back(data, data + entry->sg_list[0].length);
                if ((entry->send_flags & IBV_SEND_SIGNALED) != 0) {
                    self.queue.ready.push_back(TestCompletionQueue::completion(entry->wr_id));
                }
            }
            return 0;
        };
        TestRdmaTransport::bind_sender_for_tests(transport, connection, std::move(pool), &qp, 1024, 3);
        TestRdmaTransport::set_cq(transport, &queue.cq);
    }

    void TearDown() override
    {
        TestRdmaTransport::set_cq(transport, nullptr);
        TestRdmaTransport::cancel(transport);
    }

    OutFrame *frame(uint64_t id, uint32_t bytes = 128)
    {
        auto *data = buffers.alloc(bytes);
        if (data == nullptr)
            return nullptr;
        std::memset(data->data, static_cast<uint8_t>(id), bytes);
        data->len = bytes;
        return build_out_frame(id, 100, build_ping_request(&buffers, id, 0), data);
    }
};

TEST_F(RdmaSendTest, BoundedQueueCoalescesInOrderWithinPeerFrameLimit)
{
    for (uint64_t id = 1; id <= 4; ++id)
        ASSERT_TRUE(transport.submit(connection.get(), frame(id)));
    auto *rejected = frame(5);
    EXPECT_FALSE(transport.submit(connection.get(), rejected));
    release_rdma_frames(rejected);
    EXPECT_TRUE(sends.empty()) << "submit transfers ownership; the CQ owner performs SEND";
    EXPECT_GT(transport.drain_cq(8), 0);
    ASSERT_EQ(sends.size(), 2U);
    RdmaAggregate first, second;
    ASSERT_TRUE(parse_rdma_aggregate(sends[0], first));
    ASSERT_TRUE(parse_rdma_aggregate(sends[1], second));
    EXPECT_EQ(first.count, 3U);
    EXPECT_EQ(second.count, 1U);
    for (uint32_t index = 0; index < first.count; ++index)
        EXPECT_EQ(sends[0][first.frames[index].data_offset], index + 1);
    EXPECT_EQ(sends[1][second.frames[0].data_offset], 4);
    EXPECT_EQ(transport.stats().rpc_frames_sent.load(), 4U);
    EXPECT_EQ(transport.stats().sends.load(), 2U);
    EXPECT_EQ(transport.stats().submit_rejects.load(), 1U);
    EXPECT_EQ(transport.stats().send_queue_high_water.load(), 4U);
    EXPECT_EQ(transport.stats().send_errors.load(), 0U);
}

TEST_F(RdmaSendTest, PeerSlotOversizeRejectsBeforeQueueOrSend)
{
    auto *oversized = frame(1, 1024);
    EXPECT_FALSE(transport.submit(connection.get(), oversized));
    release_rdma_frames(oversized);
    EXPECT_EQ(transport.drain_cq(), 0);
    EXPECT_TRUE(sends.empty());
    EXPECT_EQ(transport.stats().send_queue_high_water.load(), 0U);
}

TEST_F(RdmaSendTest, LinkedBatchSignalsEachActualTailAndRetiresTheCoveredOwners)
{
    TestRdmaTransport::set_aggregation_for_tests(transport, false);
    TestRdmaTransport::set_send_policy_for_tests(transport, 3);
    for (uint64_t id = 1; id <= 4; ++id) {
        ASSERT_TRUE(transport.submit(connection.get(), frame(id)));
    }
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(sends.size(), 4U);
    EXPECT_EQ(transport.stats().send_post_calls.load(), 2U);
    EXPECT_EQ(transport.stats().send_completions.load(), 2U);
    EXPECT_EQ(transport.stats().sends.load(), 4U);
    EXPECT_TRUE(connection->is_open());
    ASSERT_TRUE(transport.submit(connection.get(), frame(5)));
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(sends.size(), 5U) << "a singleton must post without waiting for another request";
}

TEST_F(RdmaSendTest, SoftwareWindowPostsPartialBatchAndRefillsAfterCompletion)
{
    TestRdmaTransport::set_aggregation_for_tests(transport, false);
    TestRdmaTransport::set_send_policy_for_tests(transport, 4, 2, 1024);
    for (uint64_t id = 1; id <= 4; ++id) {
        ASSERT_TRUE(transport.submit(connection.get(), frame(id, 400)));
    }
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(sends.size(), 2U);
    EXPECT_EQ(transport.stats().send_completions.load(), 1U);
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_EQ(sends.size(), 4U);
    EXPECT_EQ(transport.stats().send_completions.load(), 2U);
}

TEST_F(RdmaSendTest, PreparationByteBudgetRotatesBeforeABusyQpConsumesTheNextRound)
{
    TestRdmaTransport::set_aggregation_for_tests(transport, false);
    TestRdmaTransport::set_preparation_budget_for_tests(transport, 512);
    auto second = std::make_shared<Connection>(2, "second", &buffers);
    auto pool   = std::make_unique<RdmaBufferPool>(device->pd, 4096, 4, device);
    ASSERT_TRUE(pool->ready());
    TestRdmaTransport::bind_sender_for_tests(transport, second, std::move(pool), &qp, 1024, 1);
    for (uint64_t id = 1; id <= 4; ++id) {
        ASSERT_TRUE(transport.submit(connection.get(), frame(id, 400)));
    }
    ASSERT_TRUE(transport.submit(second.get(), frame(91, 400)));
    EXPECT_GT(transport.drain_cq(8), 0);
    ASSERT_EQ(sends.size(), 1U);
    EXPECT_GT(transport.drain_cq(8), 0);
    ASSERT_EQ(sends.size(), 2U);
    RdmaAggregate decoded;
    ASSERT_TRUE(parse_rdma_aggregate(sends[1], decoded));
    EXPECT_EQ(sends[1][decoded.frames[0].data_offset], 91);
}

TEST_F(RdmaSendTest, PartiallyAcceptedBatchDoesNotReplayOrPrematurelyReleaseThePrefix)
{
    TestRdmaTransport::set_aggregation_for_tests(transport, false);
    TestRdmaTransport::set_send_policy_for_tests(transport, 4);
    fail_at = 2;
    for (uint64_t id = 1; id <= 4; ++id) {
        ASSERT_TRUE(transport.submit(connection.get(), frame(id)));
    }
    EXPECT_GT(transport.drain_cq(8), 0);
    EXPECT_FALSE(connection->is_open());
    EXPECT_EQ(sends.size(), 2U);
    EXPECT_EQ(transport.stats().sends.load(), 2U);
    EXPECT_EQ(transport.stats().send_errors.load(), 1U);
    EXPECT_EQ(transport.stats().send_completions.load(), 0U);
    TestRdmaTransport::cancel(transport);
    EXPECT_EQ(transport.stats().shutdown_flushes.load(), 2U);
}

TEST_F(RdmaSendTest, SlotByteLimitSplitsWithoutReorderingPendingFrame)
{
    for (uint64_t id = 1; id <= 4; ++id)
        ASSERT_TRUE(transport.submit(connection.get(), frame(id, 400)));
    EXPECT_GT(transport.drain_cq(8), 0);
    ASSERT_EQ(sends.size(), 2U);
    uint32_t expected = 1;
    for (const auto &wire : sends) {
        EXPECT_LE(wire.size(), 1024U);
        RdmaAggregate decoded;
        ASSERT_TRUE(parse_rdma_aggregate(wire, decoded));
        EXPECT_EQ(decoded.count, 2U);
        for (uint32_t index = 0; index < decoded.count; ++index)
            EXPECT_EQ(wire[decoded.frames[index].data_offset], expected++);
    }
}

TEST_F(RdmaSendTest, DisabledAggregationUsesOneSendPerFrame)
{
    TestRdmaTransport::set_aggregation_for_tests(transport, false);
    for (uint64_t id = 1; id <= 4; ++id)
        ASSERT_TRUE(transport.submit(connection.get(), frame(id)));
    EXPECT_GT(transport.drain_cq(8), 0);
    ASSERT_EQ(sends.size(), 4U);
    for (const auto &wire : sends) {
        RdmaAggregate decoded;
        ASSERT_TRUE(parse_rdma_aggregate(wire, decoded));
        EXPECT_EQ(decoded.count, 1U);
    }
}

TEST_F(RdmaSendTest, FailedPostClosesAndRetiresAllAdmittedFrames)
{
    fail_send = true;
    for (uint64_t id = 1; id <= 4; ++id)
        ASSERT_TRUE(transport.submit(connection.get(), frame(id)));
    (void)transport.drain_cq();
    EXPECT_FALSE(connection->is_open());
    EXPECT_EQ(transport.stats().send_errors.load(), 1U);
    EXPECT_EQ(transport.stats().sends.load(), 0U);
    EXPECT_TRUE(sends.empty());
    EXPECT_EQ(transport.drain_cq(), 0);
}
} // namespace
#endif
