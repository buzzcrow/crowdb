// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "test_rdma_transport.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
namespace crowdb::rpc
{
RdmaConfig test_control_config(uint32_t capacity, uint32_t max_control_bytes);
OutFrame  *test_control_frame(BufferPool &pool, uint64_t id, uint32_t size);
OutFrame  *test_control_reply(Frame *frame);

struct TestControlPoller
{
    explicit TestControlPoller(RdmaTransport &owner) : transport(&owner)
    {
        transport->start();
    }

    ~TestControlPoller()
    {
        stop();
    }

    void stop()
    {
        if (transport != nullptr) {
            transport->stop();
            transport = nullptr;
        }
    }

    RdmaTransport *transport;
};

struct TestControlPeers
{
    SystemBufferPool               application;
    std::unique_ptr<RdmaTransport> server;
    std::unique_ptr<RdmaTransport> client;
    std::shared_ptr<Connection>    incoming;
    std::shared_ptr<Connection>    outgoing;

    static std::unique_ptr<TestControlPeers> create(const RdmaConfig &client_config, const RdmaConfig &server_config);

    template <typename Ready> bool drain_until(Ready ready)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!ready() && std::chrono::steady_clock::now() < deadline) {
            client->drain_cq();
            server->drain_cq();
        }
        return ready();
    }
};
} // namespace crowdb::rpc
#endif
