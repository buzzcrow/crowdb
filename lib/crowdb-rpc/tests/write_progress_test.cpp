// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <thread>
#include <vector>

namespace
{
constexpr uint32_t PAYLOAD_BYTES = 64 * 1024;

// Inject a concurrent producer after the worker observes an empty send queue.
// Real sockets retain partial writes; this engine controls only event delivery.
class TestWriteEngine final : public crowdb::rpc::SocketEngine
{
  public:
    explicit TestWriteEngine(bool oneshot) : oneshot_(oneshot)
    {
    }

    int init() override
    {
        return 0;
    }

    void set_oneshot(bool on) override
    {
        oneshot_ = on;
    }

    bool oneshot() const override
    {
        inject();
        return oneshot_;
    }

    void add_listen_fd(int /*fd*/) override
    {
    }

    void add_connection(int /*read_fd*/, int write_fd, crowdb::rpc::Connection *connection) override
    {
        write_fd_   = write_fd;
        connection_ = connection;
    }

    void remove_connection(int /*read_fd*/, int /*write_fd*/) override
    {
    }

    void arm_read(int /*read_fd*/, crowdb::rpc::Connection * /*conn*/) override
    {
    }

    void arm_write(int /*write_fd*/, crowdb::rpc::Connection * /*conn*/) override
    {
        armed_.store(true);
    }

    void disarm_write(int /*write_fd*/, crowdb::rpc::Connection * /*conn*/) override
    {
        inject();
        armed_.store(false);
    }

    void notify_worker() override
    {
    }

    void notify_stop() override
    {
        stopped_.store(true);
    }

    void set_timer(int /*timeout_ms*/) override
    {
    }

    void shutdown() override
    {
    }

    int wait(crowdb::rpc::EngineEvent *events, int /*max_events*/, int /*timeout_ms*/) override
    {
        while (!stopped_.load()) {
            if (armed_.exchange(false)) {
                events[0] = {.type          = crowdb::rpc::SocketEvent::Writable,
                             .fd            = write_fd_,
                             .conn          = nullptr,
                             .connection_id = connection_->id(),
                             .write_side    = true};
                return 1;
            }
            std::this_thread::yield();
        }
        return 0;
    }

    std::function<void()> producer;

  private:
    void inject() const
    {
        if (!injected_.exchange(true)) {
            producer();
        }
    }

    bool                      oneshot_;
    int                       write_fd_   = -1;
    crowdb::rpc::Connection  *connection_ = nullptr;
    mutable std::atomic<bool> injected_{false};
    std::atomic<bool>         armed_{true};
    std::atomic<bool>         stopped_{false};
};

class SocketWriteProgressTest : public ::testing::TestWithParam<bool>
{
};

TEST_P(SocketWriteProgressTest, ConcurrentRearmSurvivesAnEarlierEmptyWritableEvent)
{
    int sockets[2]{};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    for (int fd : sockets) {
        ASSERT_NE(::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK), -1);
    }
    const int send_bytes = 4096;
    ASSERT_EQ(::setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &send_bytes, sizeof(send_bytes)), 0);
    crowdb::rpc::SocketTransport transport(1, 1);
    crowdb::rpc::TransportStats  stats;
    TestWriteEngine              engine(GetParam());
    crowdb::rpc::Worker          worker(0, &engine, &stats, &transport);
    auto connection              = std::make_shared<crowdb::rpc::Connection>(1, "write-progress", transport.pool());
    connection->transport_handle = static_cast<uint64_t>(sockets[0]);
    connection->write_fd         = ::dup(sockets[0]);
    ASSERT_GE(connection->write_fd, 0);
    transport.register_conn(connection);
    worker.add_connection(sockets[0], connection->write_fd, connection);
    std::atomic<bool> submitted{false};
    engine.producer = [&] {
        std::vector<uint8_t> bytes(PAYLOAD_BYTES, 0x5a);
        auto                *data = transport.pool()->alloc(static_cast<uint32_t>(bytes.size()));
        data->write(bytes.data(), static_cast<uint32_t>(bytes.size()));
        auto *control = crowdb::rpc::build_ping_request(transport.pool(), 42, 1);
        EXPECT_TRUE(connection->enqueue_send(crowdb::rpc::build_out_frame(42, 42, control, data)));
        EXPECT_FALSE(connection->try_send(connection->write_fd, &stats));
        engine.arm_write(connection->write_fd, connection.get());
        submitted.store(true, std::memory_order_release);
    };
    worker.start();
    crowdb::rpc::FrameParser parser;
    parser.set_pool(transport.pool());
    bool       received = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!submitted.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    while (!received && std::chrono::steady_clock::now() < deadline) {
        auto target = parser.next_read_target();
        auto size   = ::read(sockets[1], target.ptr, target.len);
        if (size <= 0) {
            std::this_thread::yield();
        }
        else if (auto *frame = parser.advance(static_cast<uint32_t>(size)); frame != nullptr) {
            EXPECT_EQ(frame->request_id, 42U);
            EXPECT_EQ(frame->header.data_size, PAYLOAD_BYTES);
            EXPECT_EQ(frame->data_buf->data[PAYLOAD_BYTES - 1], 0x5a);
            received = true;
            delete frame;
        }
    }
    EXPECT_TRUE(received);
    worker.stop();
    ::close(sockets[1]);
}

INSTANTIATE_TEST_SUITE_P(LevelAndOneshot, SocketWriteProgressTest, ::testing::Bool());
} // namespace
