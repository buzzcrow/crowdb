// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "crowdb-rpc/transport/socket_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <execinfo.h>
#    include <gtest/gtest.h>
#    include <unistd.h>

#    include <algorithm>
#    include <array>
#    include <chrono>
#    include <csignal>
#    include <cstdint>
#    include <cstdlib>
#    include <cstring>
#    include <mutex>
#    include <thread>
#    include <unordered_set>
#    include <vector>

namespace
{
void rdma_test_crash_handler(int signal_number)
{
    void      *frames[64];
    const int  count    = ::backtrace(frames, 64);
    const char prefix[] = "R239 RDMA crash backtrace:\n";
    (void)::write(STDERR_FILENO, prefix, sizeof(prefix) - 1);
    ::backtrace_symbols_fd(frames, count, STDERR_FILENO);
    std::_Exit(128 + signal_number);
}

void install_rdma_test_crash_handler()
{
    static std::once_flag installed;
    std::call_once(installed, [] {
        std::signal(SIGSEGV, rdma_test_crash_handler);
        std::signal(SIGABRT, rdma_test_crash_handler);
    });
}
} // namespace

using crowdb::rpc::Buffer;
using crowdb::rpc::BufferOwnership;
using crowdb::rpc::Connection;
using crowdb::rpc::Frame;
using crowdb::rpc::Header;
using crowdb::rpc::HEADER_SIZE;
using crowdb::rpc::OutFrame;
using crowdb::rpc::RdmaBufferPool;
using crowdb::rpc::RdmaConfig;
using crowdb::rpc::RdmaTransport;
using crowdb::rpc::RpcClient;
using crowdb::rpc::RpcServer;
using crowdb::rpc::serialize_header;
using crowdb::rpc::SocketTransport;
using crowdb::rpc::SystemBufferPool;

TEST(RdmaBufferPoolTest, SlicesOneRegisteredRegion)
{
    int          device_count = 0;
    ibv_device **devices      = ibv_get_device_list(&device_count);
    ASSERT_NE(devices, nullptr);
    if (device_count == 0) {
        ibv_free_device_list(devices);
        GTEST_SKIP() << "no RDMA device is available";
    }

    ibv_context *context = ibv_open_device(devices[0]);
    ASSERT_NE(context, nullptr);
    ibv_pd *pd = ibv_alloc_pd(context);
    ASSERT_NE(pd, nullptr);

    {
        RdmaBufferPool pool(pd, 128U << 10, 4);
        Buffer        *control = pool.alloc(1);
        ASSERT_NE(control, nullptr);
        EXPECT_EQ(control->capacity, 1U << 10);
        EXPECT_NE(control->registration_handle, 0U);
        EXPECT_NE(control->registration_key, 0U);
        EXPECT_TRUE(control->transition(BufferOwnership::Available, BufferOwnership::SendInFlight));
        EXPECT_TRUE(control->quarantine());
        control->release();
        Buffer *next = pool.alloc(1);
        ASSERT_NE(next, nullptr);
        EXPECT_NE(next, control);
        next->release();
    }

    ibv_dealloc_pd(pd);
    ibv_close_device(context);
    ibv_free_device_list(devices);
}

TEST(RdmaBufferPoolTest, TransportCopiesSystemBufferIntoRegisteredPool)
{
    RdmaTransport    transport;
    SystemBufferPool system_pool(1);
    Buffer          *source = system_pool.alloc(32);
    ASSERT_NE(source, nullptr);
    const uint8_t payload[] = {1, 2, 3, 4};
    source->write(payload, sizeof(payload));
    Buffer *registered = transport.register_buffer(source);
    if (registered == nullptr) {
        source->release();
        GTEST_SKIP() << "RDMA device registration is unavailable";
    }
    EXPECT_EQ(registered->type, crowdb::rpc::BufferType::Registered);
    EXPECT_EQ(registered->len, sizeof(payload));
    EXPECT_EQ(std::memcmp(registered->data, payload, sizeof(payload)), 0);
    registered->release();
    source->release();
}

TEST(RdmaBufferPoolTest, RegistrationFailureLeavesSystemPoolUsable)
{
    RdmaConfig config;
    config.device = "crowdb-device-that-does-not-exist";
    RdmaTransport    transport(config);
    SystemBufferPool system_pool(1);
    Buffer          *source = system_pool.alloc(32);
    ASSERT_NE(source, nullptr);
    source->write("tcp", 3);
    EXPECT_EQ(transport.register_buffer(source), nullptr);
    EXPECT_EQ(source->len, 3U);
    EXPECT_EQ(std::memcmp(source->data, "tcp", 3), 0);
    source->release();
    Buffer *reused = system_pool.alloc(32);
    ASSERT_NE(reused, nullptr);
    reused->release();
}

TEST(RdmaHardwareTest, CompleteCmQpSendReceiveFlow)
{
    constexpr uint64_t kFrameCount        = 8;
    constexpr int      kTimeoutIterations = 500;
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E=1 to run against a live HCA";
    install_rdma_test_crash_handler();
    const char *address         = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    const char *connect_address = std::getenv("CROWDB_RDMA_E2E_CONNECT_ADDR");
    const int   port =
        std::getenv("CROWDB_RDMA_E2E_PORT") == nullptr ? 19100 : std::atoi(std::getenv("CROWDB_RDMA_E2E_PORT"));
    ASSERT_NE(address, nullptr);
    if (connect_address == nullptr)
        connect_address = address;

    const char *role = std::getenv("CROWDB_RDMA_E2E_ROLE");
    if (role != nullptr && std::strcmp(role, "server") == 0) {
        RdmaTransport server;
        ASSERT_TRUE(server.listen(address, port));
        server.start();
        std::shared_ptr<crowdb::rpc::Connection> accepted;
        for (int i = 0; i < kTimeoutIterations && accepted == nullptr; ++i) {
            accepted = server.accept();
            (void)server.drain_cq(32);
            if (accepted == nullptr)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_NE(accepted, nullptr);
        accepted->set_on_frame([](Frame *frame, Connection *) { delete frame; });
        for (int i = 0; i < kTimeoutIterations && server.stats().receives.load() < kFrameCount; ++i) {
            (void)server.drain_cq(32);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::fprintf(stderr, "R239 server stats: receives=%llu posted=%llu cq_errors=%llu\n",
                     static_cast<unsigned long long>(server.stats().receives.load()),
                     static_cast<unsigned long long>(server.stats().posted_receives.load()),
                     static_cast<unsigned long long>(server.stats().cq_errors.load()));
        EXPECT_GE(server.stats().receives.load(), kFrameCount);
        EXPECT_GE(server.stats().posted_receives.load(), kFrameCount);
        server.stop();
        return;
    }
    if (role != nullptr && std::strcmp(role, "client") == 0) {
        std::fprintf(stderr, "R239 hardware client: device=%s source=%s target=%s port=%d\\n",
                     std::getenv("CROWDB_RDMA_DEVICE") == nullptr ? "" : std::getenv("CROWDB_RDMA_DEVICE"),
                     std::getenv("CROWDB_RDMA_SOURCE_ADDR") == nullptr ? "" : std::getenv("CROWDB_RDMA_SOURCE_ADDR"),
                     connect_address, port);
        RdmaTransport client;
        std::fprintf(stderr, "R239 client transport ready=%d\\n", client.ready() ? 1 : 0);
        client.start();
        auto outgoing = client.connect(connect_address, port);
        ASSERT_NE(outgoing, nullptr);
        EXPECT_GE(client.stats().posted_receives.load(), 1U);
        SystemBufferPool system_pool(2);
        for (uint64_t sequence = 0; sequence < kFrameCount; ++sequence) {
            Buffer *raw = system_pool.alloc(HEADER_SIZE);
            ASSERT_NE(raw, nullptr);
            Header header;
            header.msg_type = static_cast<uint16_t>(1 + sequence);
            serialize_header(raw->data, header);
            raw->len        = HEADER_SIZE;
            Buffer *control = client.register_buffer(raw);
            ASSERT_NE(control, nullptr);
            auto *frame    = new OutFrame;
            frame->header  = header;
            frame->control = control;
            ASSERT_TRUE(client.submit(outgoing.get(), frame));
            raw->release();
        }
        for (int i = 0; i < 300; ++i) {
            (void)client.drain_cq(32);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        client.stop();
        return;
    }

    RdmaTransport server;
    RdmaTransport client;
    ASSERT_TRUE(server.listen(address, port));
    server.start();
    client.start();

    std::shared_ptr<crowdb::rpc::Connection> accepted;
    std::thread                              accept_thread([&] {
        for (int i = 0; i < 200 && accepted == nullptr; ++i) {
            accepted = server.accept();
            if (accepted == nullptr)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    auto                                     outgoing = client.connect(connect_address, port);
    accept_thread.join();
    ASSERT_NE(outgoing, nullptr);
    ASSERT_NE(accepted, nullptr);
    accepted->set_on_frame([](Frame *frame, Connection *) { delete frame; });
    EXPECT_GE(server.stats().posted_receives.load(), 1U);
    EXPECT_GE(client.stats().posted_receives.load(), 1U);

    SystemBufferPool system_pool(2);
    Buffer          *raw = system_pool.alloc(HEADER_SIZE);
    ASSERT_NE(raw, nullptr);
    Header header;
    header.msg_type = 1;
    serialize_header(raw->data, header);
    raw->len        = HEADER_SIZE;
    Buffer *control = client.register_buffer(raw);
    ASSERT_NE(control, nullptr);
    auto *frame    = new OutFrame;
    frame->header  = header;
    frame->control = control;
    ASSERT_TRUE(client.submit(outgoing.get(), frame));
    raw->release();

    for (int i = 0; i < 200 && server.stats().receives.load() == 0; ++i) {
        (void)client.drain_cq(32);
        (void)server.drain_cq(32);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_GE(server.stats().receives.load(), 1U);
    EXPECT_GE(server.stats().posted_receives.load(), 1U);
    client.stop();
    server.stop();
}

#endif
