// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <limits>

namespace
{
using namespace crowdb::rpc;

enum class TestSubmit : uint8_t { Call, Chain, Slab };

struct TestCompletion
{
    crowdb_rpc_client_t              client     = nullptr;
    crowdb_rpc_server_t              server     = nullptr;
    crowdb_rpc_conn_t                connection = nullptr;
    std::shared_ptr<SocketTransport> transport;
    uint32_t                         calls    = 0;
    bool                             returned = false;

    ~TestCompletion()
    {
        crowdb_rpc_client_destroy(client);
        crowdb_rpc_server_destroy(server);
        crowdb_rpc_conn_destroy(connection);
    }
};

void complete(uint64_t /*request*/, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data, crowdb_rpc_status status,
              void *context)
{
    auto &state = *static_cast<TestCompletion *>(context);
    ++state.calls;
    EXPECT_FALSE(state.returned);
    EXPECT_EQ(status, CROWDB_RPC_ERR_CONN_CLOSED);
    EXPECT_EQ(control, nullptr);
    EXPECT_EQ(data, nullptr);
    crowdb_rpc_client_destroy(state.client);
    state.client = nullptr;
    crowdb_rpc_server_destroy(state.server);
    state.server = nullptr;
    crowdb_rpc_conn_destroy(state.connection);
    state.connection = nullptr;
    // The fixture retains one native owner. The active ABI call must retain
    // another, even after a synchronous callback destroys every C wrapper.
    EXPECT_GE(state.transport.use_count(), 2);
}

void check_submit(TestSubmit method, bool pooled)
{
    SystemBufferPool pool;
    TestCompletion   state;
    state.server    = new crowdb_rpc_server_s{new RpcServer(&pool)};
    state.transport = state.server->server->shared_transport();
    std::array<int, 2> sockets{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()), 0);
    auto native_connection = state.transport->create_connection(sockets[0], "synchronous-close");
    state.connection       = new crowdb_rpc_conn_s{native_connection};
    state.client           = crowdb_rpc_client_create();
    ASSERT_NE(state.client, nullptr);
    const auto native_client = state.client->client;
    if (pooled) {
        crowdb_rpc_client_set_completion_pool_size(state.client, 8);
    }
    crowdb_rpc_client_attach(state.client, state.connection);
    const int original_write_fd = native_connection->write_fd;
    // This descriptor is outside the process descriptor limit. The real
    // direct-write path gets EBADF and synchronously completes on close.
    native_connection->write_fd = std::numeric_limits<int>::max();
    constexpr std::array<uint8_t, 7> payload{1, 2, 3, 4, 5, 6, 7};
    auto *const                      control = crowdb_rpc_buffer_create(payload.data(), payload.size());
    crowdb_rpc_status                status  = CROWDB_RPC_ERR_INVALID_ARG;
    if (method == TestSubmit::Chain) {
        const std::array<crowdb_rpc_buffer_t, 2> data{crowdb_rpc_buffer_create(payload.data(), 3),
                                                      crowdb_rpc_buffer_create(payload.data() + 3, 3)};
        status = crowdb_rpc_client_send_chain(state.client, state.server, state.connection, 1, control, data.data(),
                                              data.size(), 42, complete, &state);
    }
    else if (method == TestSubmit::Slab) {
        status = crowdb_rpc_client_send_slab(state.client, state.server, state.connection, 1, control, nullptr, 42,
                                             complete, &state);
    }
    else {
        status = crowdb_rpc_client_send(state.client, state.server, state.connection, 1, control, nullptr, 42, complete,
                                        &state);
    }
    state.returned              = true;
    native_connection->write_fd = original_write_fd;
    EXPECT_EQ(status, CROWDB_RPC_OK);
    EXPECT_EQ(state.calls, 1U);
    EXPECT_EQ(native_client->pending_count(), 0U);
    EXPECT_FALSE(native_connection->is_open());
    state.transport->stop();
    ::close(sockets[1]);
}
} // namespace

TEST(CApiSubmitLifetimeTest, MapCompletionMayDestroyWrappersBeforeSendReturns)
{
    check_submit(TestSubmit::Call, false);
}

TEST(CApiSubmitLifetimeTest, SlabCompletionMayDestroyWrappersBeforeSendReturns)
{
    check_submit(TestSubmit::Call, true);
}

TEST(CApiSubmitLifetimeTest, ChainCompletionMayDestroyWrappersBeforeSendReturns)
{
    check_submit(TestSubmit::Chain, true);
}

TEST(CApiSubmitLifetimeTest, SlabOnlyCompletionMayDestroyWrappersBeforeSendReturns)
{
    check_submit(TestSubmit::Slab, true);
}
