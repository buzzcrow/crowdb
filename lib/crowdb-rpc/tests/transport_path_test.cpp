// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_environment.h"
#include "crowdb-rpc/server/server.h"

#include <gtest/gtest.h>

using namespace crowdb::rpc;

namespace
{
class TransportPathTest : public ::testing::Test
{
  protected:
    TestEnvironment disabled{"CROWDB_RDMA_ENABLED", "0"};
    TestEnvironment fallback{"CROWDB_RDMA_ALLOW_TCP_FALLBACK", nullptr};
    TestEnvironment device{"CROWDB_RDMA_DEVICE", "test-no-device"};
    RpcServer       server;

    void SetUp() override
    {
        ASSERT_TRUE(server.listen("127.0.0.1", 0));
        server.start();
    }

    void check(const char *enabled, const char *policy, bool expect_tcp)
    {
        TestEnvironment requested{"CROWDB_RDMA_ENABLED", enabled};
        TestEnvironment selected{"CROWDB_RDMA_ALLOW_TCP_FALLBACK", policy};
        SocketTransport client(1, 1);
        client.start();
        auto connection = client.connect("127.0.0.1", server.listen_port());
        if (expect_tcp) {
            ASSERT_NE(connection, nullptr);
            EXPECT_EQ(connection->path(), ConnectionPath::Tcp);
        }
        else {
            EXPECT_EQ(connection, nullptr);
        }
#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
        if (const auto *stats = client.rdma_stats_for_tests(); stats != nullptr) {
            EXPECT_EQ(stats->fallback_count.load(), expect_tcp ? 1U : 0U);
        }
#endif
    }
};

TEST_F(TransportPathTest, DefaultPolicyKeepsExplicitTcpUsable)
{
    check("0", nullptr, true);
}

TEST_F(TransportPathTest, UnavailableRdmaFallsBackWhenAllowed)
{
    check("1", "1", true);
}

TEST_F(TransportPathTest, StrictRdmaRejectsUnavailableDeviceWithoutTcpFallback)
{
    check("1", "0", false);
}

TEST_F(TransportPathTest, StrictRdmaRejectsDisabledOrUncompiledTransport)
{
    check("0", "0", false);
}

TEST_F(TransportPathTest, InvalidFallbackPolicyDoesNotSilentlyEnableTcp)
{
    check("0", "invalid", false);
}
} // namespace
