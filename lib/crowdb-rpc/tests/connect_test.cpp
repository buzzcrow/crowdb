// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/socket_transport.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <vector>

namespace
{
class TestSockets
{
  public:
    ~TestSockets()
    {
        for (const int fd : descriptors_) {
            ::close(fd);
        }
    }

    int add()
    {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd >= 0) {
            descriptors_.push_back(fd);
        }
        return fd;
    }

  private:
    std::vector<int> descriptors_;
};
} // namespace

TEST(TransportConnectTest, PendingHandshakeDoesNotBlockCaller)
{
    TestSockets sockets;
    const int   listener = sockets.add();
    ASSERT_GE(listener, 0);
    sockaddr_in address{};
    address.sin_family      = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)), 0);
    ASSERT_EQ(::listen(listener, 1), 0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr *>(&address), &length), 0);

    // Keep accepted connections queued until a new handshake cannot complete.
    // This provides a local pending SYN without a route or firewall dependency.
    bool pending = false;
    for (int index = 0; index < 32 && !pending; ++index) {
        const int fd = sockets.add();
        ASSERT_GE(fd, 0);
        ASSERT_EQ(::fcntl(fd, F_SETFL, O_NONBLOCK), 0);
        const int result = ::connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address));
        ASSERT_TRUE(result == 0 || errno == EINPROGRESS);
        pollfd    state{.fd = fd, .events = POLLOUT, .revents = 0};
        const int ready = ::poll(&state, 1, 100);
        ASSERT_GE(ready, 0);
        pending = ready == 0;
    }
    ASSERT_TRUE(pending) << "listener backlog must hold a pending handshake";

    crowdb::rpc::SocketTransport transport(1, 1);
    transport.start();
    const auto started    = std::chrono::steady_clock::now();
    auto       connection = transport.connect("127.0.0.1", ntohs(address.sin_port));
    const auto elapsed    = std::chrono::steady_clock::now() - started;
    ASSERT_NE(connection, nullptr);
    EXPECT_LT(elapsed, std::chrono::milliseconds(100));
    EXPECT_NE(::fcntl(static_cast<int>(connection->transport_handle), F_GETFL) & O_NONBLOCK, 0);
    transport.stop();
    EXPECT_FALSE(connection->is_open());
}
