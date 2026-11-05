// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#include <gtest/gtest.h>

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <cstdlib>

using namespace crowdb::rpc;

TEST(RdmaHardwareTest, EphemeralListenersAdvertiseDistinctBoundPorts)
{
    const auto *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr) {
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    }
    RdmaConfig config;
    config.use_environment = false;
    if (const auto *device = std::getenv("CROWDB_RDMA_DEVICE")) {
        config.device = device;
    }
    RdmaTransport first(config);
    RdmaTransport second(config);
    ASSERT_FALSE(first.listen(address, -1));
    ASSERT_FALSE(first.listen(address, 65536));
    ASSERT_TRUE(first.listen(address, 0));
    ASSERT_TRUE(second.listen(address, 0));
    const auto first_endpoint  = first.local_capability();
    const auto second_endpoint = second.local_capability();
    EXPECT_EQ(first_endpoint.cm_address, address);
    EXPECT_EQ(second_endpoint.cm_address, address);
    EXPECT_GT(first_endpoint.port, 0);
    EXPECT_GT(second_endpoint.port, 0);
    EXPECT_NE(first_endpoint.port, second_endpoint.port);
    EXPECT_FALSE(first.listen(address, 0));
}
#endif
