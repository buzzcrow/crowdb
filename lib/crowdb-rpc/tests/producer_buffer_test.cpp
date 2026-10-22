// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

namespace
{
using TestBuffer     = std::unique_ptr<crowdb_rpc_buffer_s, decltype(&crowdb_rpc_buffer_release)>;
using TestServer     = std::unique_ptr<crowdb_rpc_server_s, decltype(&crowdb_rpc_server_destroy)>;
using TestConnection = std::unique_ptr<crowdb_rpc_conn_s, decltype(&crowdb_rpc_conn_destroy)>;
} // namespace

TEST(ProducerBufferTest, CommitRequiresUniqueInitializedPrefixAndEndsMutableAccess)
{
    auto pool = crowdb_rpc_pool_create(1);
    ASSERT_NE(pool, nullptr);
    TestBuffer buffer(crowdb_rpc_buffer_alloc(pool, 16), crowdb_rpc_buffer_release);
    ASSERT_NE(buffer, nullptr);
    auto *pointer = crowdb_rpc_buffer_mutable_data(buffer.get());
    ASSERT_NE(pointer, nullptr);
    std::memcpy(pointer, "producer payload", 16);
    TestBuffer clone(crowdb_rpc_buffer_ref(buffer.get()), crowdb_rpc_buffer_release);
    ASSERT_NE(clone, nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_mutable_data(buffer.get()), nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_commit(buffer.get(), 16), 0);
    clone.reset();
    EXPECT_EQ(crowdb_rpc_buffer_commit(buffer.get(), 0), 0);
    EXPECT_EQ(crowdb_rpc_buffer_commit(buffer.get(), 17), 0);
    ASSERT_EQ(crowdb_rpc_buffer_commit(buffer.get(), 16), 1);
    EXPECT_EQ(crowdb_rpc_buffer_commit(buffer.get(), 16), 0);
    EXPECT_EQ(crowdb_rpc_buffer_mutable_data(buffer.get()), nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_data(buffer.get()), pointer);
    crowdb_rpc_pool_destroy(pool);
    EXPECT_EQ(std::memcmp(crowdb_rpc_buffer_data(buffer.get()), "producer payload", 16), 0);
}

TEST(RdmaHardwareTest, ProducerArenaStaysBoundedAndLeaseOutlivesServer)
{
    const auto *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    crowdb_rpc_rdma_config_t config;
    crowdb_rpc_rdma_config_init(&config);
    config.enabled = config.listen_enabled = 1;
    config.allow_tcp_fallback              = 0;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 8;
    config.cq_depth                                                  = 16;
    config.wire_slot_capacity                                        = 1024;
    config.send_arena_bytes = config.receive_arena_bytes = 8192;
    config.source_address                                = address;
    config.device                                        = std::getenv("CROWDB_RDMA_DEVICE");
    TestServer server(crowdb_rpc_server_create(nullptr), crowdb_rpc_server_destroy);
    ASSERT_NE(server, nullptr);
    ASSERT_EQ(crowdb_rpc_server_configure_rdma(server.get(), &config), CROWDB_RPC_OK);
    ASSERT_EQ(crowdb_rpc_server_listen(server.get(), address, 0), CROWDB_RPC_OK);
    crowdb_rpc_server_start(server.get());
    ASSERT_EQ(crowdb_rpc_server_rdma_listening(server.get()), 1);
    EXPECT_EQ(crowdb_rpc_server_alloc_producer_buffer(server.get(), 1024, 1), nullptr);
    // Listening advertises the endpoint; the first established QP adopts
    // its CM device context and publishes the registered producer arena.
    TestServer client(crowdb_rpc_server_create(nullptr), crowdb_rpc_server_destroy);
    config.listen_enabled = 0;
    ASSERT_EQ(crowdb_rpc_server_configure_rdma(client.get(), &config), CROWDB_RPC_OK);
    crowdb_rpc_server_start(client.get());
    TestConnection connection(crowdb_rpc_connect(client.get(), address, crowdb_rpc_server_port(server.get())),
                              crowdb_rpc_conn_destroy);
    ASSERT_NE(connection, nullptr);
    std::vector<TestBuffer> held;
    for (uint32_t i = 0; i < 8; ++i) {
        TestBuffer buffer(crowdb_rpc_server_alloc_producer_buffer(server.get(), 1024, 1), crowdb_rpc_buffer_release);
        ASSERT_NE(buffer, nullptr);
        EXPECT_NE(crowdb_rpc_buffer_registration_id(buffer.get()), 0U);
        auto *pointer = crowdb_rpc_buffer_mutable_data(buffer.get());
        ASSERT_NE(pointer, nullptr);
        std::memcpy(pointer, "registered producer", 19);
        ASSERT_EQ(crowdb_rpc_buffer_commit(buffer.get(), 19), 1);
        held.push_back(std::move(buffer));
    }
    EXPECT_EQ(crowdb_rpc_server_alloc_producer_buffer(server.get(), 1024, 1), nullptr);
    EXPECT_EQ(crowdb_rpc_server_alloc_producer_buffer(server.get(), 1024, 0), nullptr);
    held.pop_back();
    TestBuffer replacement(crowdb_rpc_server_alloc_producer_buffer(server.get(), 1024, 1), crowdb_rpc_buffer_release);
    ASSERT_NE(replacement, nullptr);
    EXPECT_NE(crowdb_rpc_buffer_registration_id(replacement.get()), 0U);
    const auto *pointer = crowdb_rpc_buffer_data(held.front().get());
    TestBuffer  clone(crowdb_rpc_buffer_ref(held.front().get()), crowdb_rpc_buffer_release);
    ASSERT_NE(clone, nullptr);
    server.reset();
    held.clear();
    replacement.reset();
    EXPECT_EQ(crowdb_rpc_buffer_data(clone.get()), pointer);
    EXPECT_EQ(std::memcmp(pointer, "registered producer", 19), 0);
}
