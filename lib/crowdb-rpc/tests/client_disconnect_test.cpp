// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/pool.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace
{
int socketpair_nonblocking(int sockets[2])
{
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
        return -1;
    }
    for (int socket : {sockets[0], sockets[1]}) {
        const int flags = ::fcntl(socket, F_GETFL, 0);
        if (flags < 0 || ::fcntl(socket, F_SETFL, flags | O_NONBLOCK) != 0) {
            ::close(sockets[0]);
            ::close(sockets[1]);
            return -1;
        }
    }
    return 0;
}

struct Completion
{
    std::atomic<int> calls{0};
    std::atomic<int> status{CROWDB_RPC_OK};
};

void complete(uint64_t /*request_id*/, crowdb_rpc_buffer_t /*control*/, crowdb_rpc_buffer_t /*data*/,
              crowdb_rpc_status status, void *context)
{
    auto *result = static_cast<Completion *>(context);
    result->status.store(status, std::memory_order_relaxed);
    result->calls.fetch_add(1, std::memory_order_release);
}
} // namespace

TEST(ClientDisconnectTest, PeerCloseFailsSlabAndMapWithoutReaperAndPreservesOtherConnection)
{
    using namespace crowdb::rpc;
    SystemBufferPool pool;
    SocketTransport  transport(1, 1);
    RpcClient        caller;
    caller.set_completion_pool_size(4);
    transport.start();
    int first[2]{};
    int second[2]{};
    ASSERT_EQ(socketpair_nonblocking(first), 0);
    ASSERT_EQ(socketpair_nonblocking(second), 0);
    std::atomic<int> cleanups{0};
    auto             closed = transport.create_connection(first[0], "closed", {}, [&](Connection *) { ++cleanups; });
    auto             live   = transport.create_connection(second[0], "live");
    caller.attach(closed.get());
    caller.attach(live.get());
    Completion slab;
    Completion fallback;
    Completion unrelated;
    auto       submit = [&](Connection *connection, uint64_t request, Completion *result) {
        Buffer *control = pool.alloc(1);
        control->write("x", 1);
        return caller.send(&transport, connection, request, control, nullptr, 42, complete, result);
    };
    ASSERT_TRUE(submit(closed.get(), 1, &slab));
    ASSERT_TRUE(submit(closed.get(), 5, &fallback));
    ASSERT_TRUE(submit(live.get(), 2, &unrelated));
    ASSERT_EQ(caller.pending_count(), 3U);
    ::close(first[1]);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while ((slab.calls.load(std::memory_order_acquire) == 0 || fallback.calls.load(std::memory_order_acquire) == 0) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(slab.calls.load(), 1);
    EXPECT_EQ(fallback.calls.load(), 1);
    EXPECT_EQ(slab.status.load(), CROWDB_RPC_ERR_CONN_CLOSED);
    EXPECT_EQ(fallback.status.load(), CROWDB_RPC_ERR_CONN_CLOSED);
    EXPECT_EQ(unrelated.calls.load(), 0);
    EXPECT_EQ(caller.pending_count(), 1U);
    auto *response       = new Frame;
    response->request_id = 2;
    EXPECT_TRUE(caller.on_response(2, response));
    EXPECT_EQ(unrelated.calls.load(), 1);
    EXPECT_EQ(unrelated.status.load(), CROWDB_RPC_OK);
    EXPECT_EQ(caller.pending_count(), 0U);
    transport.stop();
    EXPECT_EQ(cleanups.load(), 1);
    ::close(second[1]);
}

TEST(ClientDisconnectTest, AttachedConnectionOutlivesFfiClientHandle)
{
    auto *server = crowdb_rpc_server_create(nullptr);
    ASSERT_NE(server, nullptr);
    ASSERT_EQ(crowdb_rpc_server_listen(server, "127.0.0.1", 0), CROWDB_RPC_OK);
    crowdb_rpc_server_start(server);
    auto *connection = crowdb_rpc_connect(server, "127.0.0.1", crowdb_rpc_server_port(server));
    ASSERT_NE(connection, nullptr);
    auto *client = crowdb_rpc_client_create();
    ASSERT_NE(client, nullptr);
    crowdb_rpc_client_set_completion_pool_size(client, 4);
    crowdb_rpc_client_attach(client, connection);
    crowdb_rpc_client_destroy(client);
    crowdb_rpc_conn_close(connection);
    EXPECT_EQ(crowdb_rpc_conn_is_open(connection), 0);
    crowdb_rpc_conn_destroy(connection);
    crowdb_rpc_server_stop(server);
    crowdb_rpc_server_destroy(server);
}
