// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_rdma_transport.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

using namespace crowdb::rpc;

namespace
{
RdmaConfig test_receive_config()
{
    RdmaConfig config;
    config.device             = "test-no-device";
    config.wire_slot_capacity = 1024;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 4;
    config.cq_depth                                                  = 8;
    config.send_arena_bytes = config.receive_arena_bytes = 4096;
    config.receive_batch_size                            = 4;
    return config;
}

class RdmaReceiveBatchTest : public ::testing::Test
{
  protected:
    RegisteredBufferPool        slots{4096, 4};
    RdmaTransport               transport{test_receive_config()};
    std::shared_ptr<Connection> connection = std::make_shared<Connection>(1, "receiver", &slots);
    std::shared_ptr<void>       pools;
    ibv_context                 context{};
    ibv_qp                      qp{};
    std::vector<uint64_t>       accepted;
    uint32_t                    fail_at = UINT32_MAX;

    void SetUp() override
    {
        pools                 = TestRdmaTransport::bind_cq(connection, 0);
        qp.context            = &context;
        qp.qp_context         = this;
        context.ops.post_recv = [](ibv_qp *qp, ibv_recv_wr *wr, ibv_recv_wr **bad) {
            auto    &self  = *static_cast<RdmaReceiveBatchTest *>(qp->qp_context);
            uint32_t index = 0;
            for (auto *entry = wr; entry != nullptr; entry = entry->next, ++index) {
                if (index == self.fail_at) {
                    *bad = entry;
                    return EIO;
                }
                EXPECT_EQ(entry->num_sge, 1);
                EXPECT_EQ(entry->sg_list[0].length, 1024U);
                self.accepted.push_back(entry->wr_id);
            }
            return 0;
        };
        connection->transport_handle = reinterpret_cast<uint64_t>(&qp);
    }

    void TearDown() override
    {
        connection->close();
        TestRdmaTransport::cancel(transport);
    }

    bool post()
    {
        auto *slot = slots.alloc(1024);
        if (slot == nullptr) {
            ADD_FAILURE() << "receive slot admission exhausted";
            return false;
        }
        const bool result = transport.post_receive(connection.get(), slot);
        slot->release();
        return result;
    }
};

TEST_F(RdmaReceiveBatchTest, ActualPartialListPostsWithoutWaitingForAFullBatch)
{
    ASSERT_TRUE(post());
    ASSERT_TRUE(post());
    EXPECT_TRUE(accepted.empty());
    ASSERT_TRUE(TestRdmaTransport::flush_receive_batch_for_tests(transport, connection.get()));
    EXPECT_EQ(accepted.size(), 2U);
    EXPECT_EQ(transport.stats().receive_post_calls.load(), 1U);
    EXPECT_EQ(transport.stats().posted_receives.load(), 2U);
}

TEST_F(RdmaReceiveBatchTest, PartialProviderFailureKeepsOnlyAcceptedReceiveOwners)
{
    fail_at = 2;
    ASSERT_TRUE(post());
    ASSERT_TRUE(post());
    ASSERT_TRUE(post());
    EXPECT_FALSE(post());
    EXPECT_FALSE(connection->is_open());
    ASSERT_EQ(accepted.size(), 2U);
    for (auto token : accepted) {
        EXPECT_TRUE(TestRdmaTransport::pending(transport, token));
    }
    auto *first  = slots.alloc(1024);
    auto *second = slots.alloc(1024);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(slots.alloc(1024), nullptr);
    first->release();
    second->release();
    TestRdmaTransport::cancel(transport);
    EXPECT_EQ(transport.stats().shutdown_flushes.load(), 2U);
}
} // namespace
#endif
