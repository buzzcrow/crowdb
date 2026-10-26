// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/status_json.h"
#include "common/test_environment.h"
#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/client/rpc_client_metrics.h"
#include "crowdb-rpc/rpc_metrics.h"

#include <gtest/gtest.h>

using namespace crowdb::rpc;

TEST(TransportConfigTest, ExplicitTcpOverridesEnvironmentAndRejectsLateChanges)
{
    const TestEnvironment enabled("CROWDB_RDMA_ENABLED", "1");
    const TestEnvironment invalid_device("CROWDB_RDMA_DEVICE", "test-no-device");
    const TestEnvironment strict("CROWDB_RDMA_ALLOW_TCP_FALLBACK", "0");
    RpcServer             server;
    RdmaConfig            config;
    ASSERT_TRUE(server.configure_rdma(config, false, false, false));
    EXPECT_NE(server.transport()->status_json().find("\"allow_tcp_fallback\":false"), std::string::npos);
    EXPECT_NE(server.transport()->status_json().find("\"rdma\":null"), std::string::npos);
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    EXPECT_FALSE(server.configure_rdma(config, false, true, false));
    server.start();
    auto connection = server.transport()->connect("127.0.0.1", server.listen_port());
    ASSERT_NE(connection, nullptr);
    EXPECT_EQ(connection->path(), ConnectionPath::Tcp);
    server.stop();
}

TEST(TransportConfigTest, VersionedCAbiRejectsMalformedBoundsBeforePublication)
{
    const auto server = crowdb_rpc_server_create(nullptr);
    ASSERT_NE(server, nullptr);
    crowdb_rpc_rdma_config_t config;
    crowdb_rpc_rdma_config_init(&config);
    EXPECT_EQ(config.struct_size, sizeof(config));
    config.version = 2;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma(server, &config), CROWDB_RPC_ERR_INVALID_ARG);
    config.version = 1;
    config.port    = 256;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma(server, &config), CROWDB_RPC_ERR_INVALID_ARG);
    config.port     = 1;
    config.cq_count = 0;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma(server, &config), CROWDB_RPC_ERR_INVALID_ARG);
    config.cq_count = 1;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma(server, &config), CROWDB_RPC_OK);
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v2(server, &config, 2), CROWDB_RPC_ERR_INVALID_ARG);
    config.wire_slot_capacity  = RDMA_DATAPATH_CAPACITY + RDMA_CONTROL_CAPACITY;
    config.receive_arena_bytes = uint64_t{config.posted_receive_target} * config.wire_slot_capacity;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma(server, &config), CROWDB_RPC_ERR_INVALID_ARG);
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v2(server, &config, 1), CROWDB_RPC_OK);
    EXPECT_EQ(config.struct_size, sizeof(config));
    EXPECT_EQ(config.version, 1U);
    crowdb_rpc_server_destroy(server);
}

TEST(TransportConfigTest, VersionedLogicalControlBoundsRejectBeforeConfiguration)
{
    const auto server = crowdb_rpc_server_create(nullptr);
    ASSERT_NE(server, nullptr);
    crowdb_rpc_rdma_config_t config;
    crowdb_rpc_rdma_config_init(&config);
    crowdb_rpc_rdma_stream_config_t streams;
    crowdb_rpc_rdma_stream_config_init(&streams);
    EXPECT_EQ(streams.struct_size, sizeof(streams));
    EXPECT_EQ(streams.version, 1U);
    EXPECT_EQ(streams.max_control_message_bytes, 32U << 20);
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v3(server, &config, 0, nullptr), CROWDB_RPC_ERR_INVALID_ARG);
    streams.version = 2;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v3(server, &config, 0, &streams), CROWDB_RPC_ERR_INVALID_ARG);
    streams.version                      = 1;
    streams.control_receive_budget_bytes = streams.max_control_message_bytes - 1;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v3(server, &config, 0, &streams), CROWDB_RPC_ERR_INVALID_ARG);
    streams.control_receive_budget_bytes = streams.max_control_message_bytes;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v3(server, &config, 0, &streams), CROWDB_RPC_OK);
    EXPECT_EQ(config.version, 1U);
    EXPECT_EQ(config.struct_size, sizeof(config));
    crowdb_rpc_server_destroy(server);
}

TEST(TransportConfigTest, SendExtensionPreservesEarlierAbiAndRejectsMalformedBounds)
{
    const auto server = crowdb_rpc_server_create(nullptr);
    ASSERT_NE(server, nullptr);
    crowdb_rpc_rdma_config_t config;
    crowdb_rpc_rdma_config_init(&config);
    crowdb_rpc_rdma_stream_config_t streams;
    crowdb_rpc_rdma_stream_config_init(&streams);
    crowdb_rpc_rdma_post_config_t sends;
    crowdb_rpc_rdma_post_config_init(&sends);
    EXPECT_EQ(sends.struct_size, sizeof(sends));
    EXPECT_EQ(sends.version, 1U);
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v4(server, &config, 0, &streams, nullptr), CROWDB_RPC_ERR_INVALID_ARG);
    sends.version = 2;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v4(server, &config, 0, &streams, &sends), CROWDB_RPC_ERR_INVALID_ARG);
    sends.version         = 1;
    sends.send_batch_size = 0;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v4(server, &config, 0, &streams, &sends), CROWDB_RPC_ERR_INVALID_ARG);
    sends.send_batch_size    = 16;
    sends.receive_batch_size = 0;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v4(server, &config, 0, &streams, &sends), CROWDB_RPC_ERR_INVALID_ARG);
    sends.receive_batch_size = 8;
    sends.send_window_size   = config.sq_depth + 1;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v4(server, &config, 0, &streams, &sends), CROWDB_RPC_ERR_INVALID_ARG);
    sends.send_window_size  = 4;
    sends.send_window_bytes = config.wire_slot_capacity;
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v4(server, &config, 0, &streams, &sends), CROWDB_RPC_OK);
    EXPECT_EQ(crowdb_rpc_server_configure_rdma_v3(server, &config, 0, &streams), CROWDB_RPC_OK);
    crowdb_rpc_server_destroy(server);
}

TEST(TransportConfigTest, ClientErrorCountersSurviveMetricWindowFlush)
{
    crowdb_rpc_client_counters_t before{}, pending{}, flushed{};
    crowdb_rpc_client_get_counters(nullptr, &before);
    rpc_submit_retry().inc_by(2);
    rpc_resp_missed().inc();
    crowdb_rpc_client_get_counters(nullptr, &pending);
    rpc_submit_retry().flush();
    rpc_resp_missed().flush();
    crowdb_rpc_client_get_counters(nullptr, &flushed);
    EXPECT_EQ(pending.submit_retry, before.submit_retry + 2);
    EXPECT_EQ(pending.resp_missed, before.resp_missed + 1);
    EXPECT_EQ(flushed.submit_retry, pending.submit_retry);
    EXPECT_EQ(flushed.resp_missed, pending.resp_missed);
}

TEST(TransportConfigTest, StatusEscapesConfiguredStrings)
{
    StatusJson status;
    status.string("key", "a\"b\\c\n");
    EXPECT_EQ(status.finish(), "{\"key\":\"a\\\"b\\\\c\\u000a\"}");
}
