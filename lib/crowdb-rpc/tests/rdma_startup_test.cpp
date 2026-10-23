// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/server/handler.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/socket_transport.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

#    include <algorithm>
#    include <atomic>
#    include <chrono>
#    include <cstdlib>
#    include <cstring>
#    include <thread>

using namespace crowdb::rpc;

namespace
{
struct TestFirstResponse
{
    std::atomic<bool> completed{false};
    bool              matches = false;
};

void complete_first_response(uint64_t id, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                             crowdb_rpc_status status, void *context)
{
    auto &result   = *static_cast<TestFirstResponse *>(context);
    result.matches = status == CROWDB_RPC_OK && id == 42 && data != nullptr && crowdb_rpc_buffer_len(data) == 128 &&
                     std::all_of(crowdb_rpc_buffer_data(data), crowdb_rpc_buffer_data(data) + 128,
                                 [](uint8_t byte) { return byte == 0xab; });
    crowdb_rpc_buffer_release(control);
    crowdb_rpc_buffer_release(data);
    result.completed.store(true, std::memory_order_release);
}
} // namespace

static void test_first_response(RdmaWireLayout layout)
{
    const auto *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    RdmaConfig config;
    config.use_environment = false;
    config.wire_layout     = layout;
    config.source_address  = address;
    if (const auto *device = std::getenv("CROWDB_RDMA_DEVICE"))
        config.device = device;
    config.wire_slot_capacity = 1024;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 1024;
    config.cq_depth                                                  = 2048;
    config.send_arena_bytes = config.receive_arena_bytes = 1024 * 1024;
    if (layout == RdmaWireLayout::ControlData) {
        config.wire_slot_capacity = RDMA_DATAPATH_CAPACITY + RDMA_CONTROL_CAPACITY;
        config.sq_depth = config.rq_depth = config.posted_receive_target = 4;
        config.cq_depth                                                  = 8;
        config.send_arena_bytes                                          = 4 * RDMA_DATAPATH_CAPACITY;
        config.receive_arena_bytes                                       = uint64_t{4} * config.wire_slot_capacity;
        config.send_batch_size = config.receive_batch_size = 16;
        config.aggregation_enabled                         = true;
    }
    TestFirstResponse result;
    RpcClient         caller;
    RpcServer         server;
    ASSERT_TRUE(server.configure_rdma(config, true, false, true));
    ASSERT_TRUE(server.listen(address, 0));
    server.register_handler(100, handle_echo);
    server.start();
    SocketTransport client(1, 1);
    ASSERT_TRUE(client.configure_rdma(config, true, false, false));
    client.start();
    auto connection = client.connect(address, server.listen_port());
    ASSERT_NE(connection, nullptr);
    ASSERT_EQ(connection->path(), ConnectionPath::Rdma);
    caller.attach(connection.get());
    bool completed_in_time = true;
    for (uint32_t round = 0; round < 3; ++round) {
        result.completed.store(false, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        auto *data = client.pool()->alloc(128);
        ASSERT_NE(data, nullptr);
        std::memset(data->data, 0xab, 128);
        data->len = 128;
        ASSERT_TRUE(caller.send(&client, connection.get(), 42, build_ping_request(client.pool(), 42, 0), data, 100,
                                complete_first_response, &result));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        while (!result.completed.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        completed_in_time = result.completed.load(std::memory_order_acquire);
        if (!completed_in_time) {
            break;
        }
        EXPECT_TRUE(result.matches);
    }
    EXPECT_TRUE(completed_in_time) << "singleton exceeded the one-second progress bound; client="
                                   << client.status_json() << " server=" << server.transport()->status_json();
    client.stop();
    server.stop();
    caller.fail_all(nullptr, RpcError::ConnectionClosed);
    EXPECT_TRUE(result.matches);
    EXPECT_EQ(client.rdma_stats_for_tests()->fallback_count.load(), 0U);
}

TEST(RdmaHardwareTest, ContiguousFirstResponseCompletesWithinTheWarmupWindow)
{
    test_first_response(RdmaWireLayout::Contiguous);
}

TEST(RdmaHardwareTest, ControlDataSingletonAfterIdleDoesNotWaitForTheNextMessage)
{
    test_first_response(RdmaWireLayout::ControlData);
}
#endif
