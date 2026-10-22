// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_rdma_transport.h"
#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)

#    include <gtest/gtest.h>

#    include <chrono>
#    include <cstdlib>
#    include <cstring>
#    include <thread>

using namespace crowdb::rpc;

namespace
{
RdmaConfig test_config(const std::string &address)
{
    RdmaConfig config;
    config.use_environment = false;
    config.source_address  = address;
    if (const char *device = std::getenv("CROWDB_RDMA_DEVICE"); device != nullptr)
        config.device = device;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 8;
    config.wire_slot_capacity                                        = 1024;
    config.send_arena_bytes = config.receive_arena_bytes = 8192;
    config.cq_depth                                      = 16;
    return config;
}

bool wait_for(const std::atomic<uint32_t> &count)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (count.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return count.load(std::memory_order_acquire) != 0;
}

struct TestCompletion
{
    std::atomic<uint32_t> callbacks{0};
    std::atomic<int>      status{CROWDB_RPC_OK};
    std::atomic<bool>     valid{false};
};

void test_complete(uint64_t request_id, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data, crowdb_rpc_status status,
                   void *context)
{
    auto &completion = *static_cast<TestCompletion *>(context);
    completion.status.store(status, std::memory_order_relaxed);
    completion.valid.store(request_id == 91 && status == CROWDB_RPC_OK && data != nullptr &&
                           crowdb_rpc_buffer_len(data) == 16 &&
                           std::memcmp(crowdb_rpc_buffer_data(data), "fallback payload", 16) == 0);
    if (control != nullptr)
        crowdb_rpc_buffer_release(control);
    if (data != nullptr)
        crowdb_rpc_buffer_release(data);
    completion.callbacks.fetch_add(1, std::memory_order_release);
}

bool test_send(RpcClient &client, SocketTransport &transport, Connection *connection, TestCompletion &completion)
{
    auto *control = build_ping_request(transport.pool(), 91, 0);
    auto *data    = transport.pool()->alloc(16);
    if (control == nullptr || data == nullptr) {
        if (control != nullptr)
            control->release();
        if (data != nullptr)
            data->release();
        return false;
    }
    data->write("fallback payload", 16);
    return client.send(&transport, connection, 91, control, data, 100, test_complete, &completion);
}
} // namespace

TEST(RdmaHardwareTest, ExpiredCmEndpointFallsBackOnlyBeforePayload)
{
    const char *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    const auto config = test_config(address);
    RpcServer  server;
    ASSERT_TRUE(server.configure_rdma(config, true, true, false));
    ASSERT_TRUE(server.listen(address, 0));
    auto &native = TestSocketTransport::rdma_for_tests(*server.transport());
    // Publish a stale endpoint before starting workers. There is deliberately
    // no CM listener on the TCP-only service port.
    TestRdmaTransport::advertise_expired_endpoint_for_tests(native, address,
                                                            static_cast<uint16_t>(server.listen_port()));
    std::atomic<uint32_t> received{0};
    server.register_handler(100, [&received](Frame *frame, Connection *connection) {
        received.fetch_add(1, std::memory_order_relaxed);
        return handle_echo(frame, connection);
    });
    server.start();
    for (const bool allow : {false, true}) {
        TestCompletion  completion;
        SocketTransport transport(1, 1);
        ASSERT_TRUE(transport.configure_rdma(config, true, allow, false));
        transport.start();
        RpcClient client;
        client.set_completion_pool_size(128);
        auto        connection = transport.connect(address, server.listen_port());
        const auto *stats      = transport.rdma_stats_for_tests();
        ASSERT_NE(stats, nullptr);
        EXPECT_EQ(stats->preflight_attempts.load(), 1U);
        EXPECT_EQ(stats->cm_attempts.load(), 1U);
        EXPECT_EQ(stats->fallback_count.load(), allow ? 1U : 0U);
        EXPECT_EQ(stats->rpc_frames_sent.load(), 0U);
        if (allow) {
            ASSERT_NE(connection, nullptr);
            EXPECT_EQ(connection->path(), ConnectionPath::Tcp);
            client.attach(connection.get());
            ASSERT_TRUE(test_send(client, transport, connection.get(), completion));
            EXPECT_TRUE(wait_for(completion.callbacks));
            EXPECT_TRUE(completion.valid.load());
        }
        else {
            EXPECT_EQ(connection, nullptr);
        }
        transport.stop();
        EXPECT_EQ(completion.callbacks.load(), allow ? 1U : 0U);
    }
    server.stop();
    EXPECT_EQ(received.load(), 1U);
}

namespace
{
void test_peer_close_after_payload(bool stop_server)
{
    const char *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    const auto            config = test_config(address);
    std::atomic<uint32_t> received{0};
    RpcServer             server;
    ASSERT_TRUE(server.configure_rdma(config, true, true, true));
    ASSERT_TRUE(server.listen(address, 0));
    server.register_handler(100, [&received, stop_server](Frame *frame, Connection *connection) {
        received.fetch_add(1, std::memory_order_relaxed);
        delete frame;
        if (!stop_server)
            connection->close();
        return static_cast<OutFrame *>(nullptr);
    });
    server.start();
    TestCompletion  completion;
    SocketTransport transport(1, 1);
    ASSERT_TRUE(transport.configure_rdma(config, true, true, false));
    transport.start();
    RpcClient client;
    client.set_completion_pool_size(128);
    auto connection = transport.connect(address, server.listen_port());
    ASSERT_NE(connection, nullptr);
    ASSERT_EQ(connection->path(), ConnectionPath::Rdma);
    client.attach(connection.get());
    ASSERT_TRUE(test_send(client, transport, connection.get(), completion));
    if (stop_server) {
        ASSERT_TRUE(wait_for(received));
        server.stop();
    }
    EXPECT_TRUE(wait_for(completion.callbacks));
    EXPECT_EQ(completion.status.load(), CROWDB_RPC_ERR_CONN_CLOSED);
    EXPECT_FALSE(connection->is_open());
    EXPECT_EQ(connection->path(), ConnectionPath::Rdma);
    transport.stop();
    server.stop();
    EXPECT_EQ(completion.callbacks.load(), 1U);
    EXPECT_EQ(received.load(), 1U);
    const auto *stats = transport.rdma_stats_for_tests();
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(stats->cm_attempts.load(), 1U);
    EXPECT_EQ(stats->fallback_count.load(), 0U);
    EXPECT_EQ(stats->rpc_frames_sent.load(), 1U);
    EXPECT_EQ(transport.stats().rpc_frames_sent.load(), 0U);
    EXPECT_EQ(transport.connection_count(), 0U);
    EXPECT_EQ(server.transport()->connection_count(), 0U);
}
} // namespace

TEST(RdmaHardwareTest, PeerCloseAfterPayloadFailsOnceWithoutTcpFallback)
{
    test_peer_close_after_payload(false);
}

TEST(RdmaHardwareTest, ServerShutdownAfterPayloadFailsOnceWithoutTcpFallback)
{
    test_peer_close_after_payload(true);
}

#endif
