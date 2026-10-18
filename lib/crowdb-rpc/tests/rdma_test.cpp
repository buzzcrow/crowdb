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
#    include <chrono>
#    include <csignal>
#    include <cstdint>
#    include <cstdlib>
#    include <cstring>
#    include <mutex>
#    include <thread>

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

struct WorkerCompletion
{
    std::atomic<uint32_t> completed{0};
    std::atomic<uint32_t> failed{0};
};

extern "C" void rdma_worker_completion(uint64_t, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                       crowdb_rpc_status status, void *opaque)
{
    auto *state = static_cast<WorkerCompletion *>(opaque);
    (status == CROWDB_RPC_OK ? state->completed : state->failed).fetch_add(1, std::memory_order_relaxed);
    if (control != nullptr)
        crowdb_rpc_buffer_release(control);
    if (data != nullptr)
        crowdb_rpc_buffer_release(data);
}

using crowdb::rpc::Buffer;
using crowdb::rpc::BufferOwnership;
using crowdb::rpc::Connection;
using crowdb::rpc::Frame;
using crowdb::rpc::Header;
using crowdb::rpc::HEADER_SIZE;
using crowdb::rpc::OutFrame;
using crowdb::rpc::RdmaBufferPool;
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

TEST(RdmaHardwareTest, RpcServerClientWorkerRoundTrip)
{
    if (std::getenv("CROWDB_RDMA_RPC_E2E") == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_RPC_E2E=1 to run the worker flow";
    install_rdma_test_crash_handler();
    const char *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    const char *role    = std::getenv("CROWDB_RDMA_E2E_ROLE");
    ASSERT_NE(address, nullptr);
    const int port =
        std::getenv("CROWDB_RDMA_E2E_PORT") == nullptr ? 19101 : std::atoi(std::getenv("CROWDB_RDMA_E2E_PORT"));
    constexpr uint32_t kRequests    = 8;
    constexpr uint16_t kMessageType = 239;

    if (role != nullptr && std::strcmp(role, "server") == 0) {
        RpcServer server;
        ASSERT_TRUE(server.listen(address, port));
        std::atomic<uint32_t> received{0};
        server.register_handler(kMessageType, [&received, &server](Frame *frame, Connection *) {
            received.fetch_add(1, std::memory_order_relaxed);
            OutFrame *response        = new OutFrame;
            response->request_id      = frame->request_id;
            response->header.msg_type = static_cast<uint16_t>(kMessageType);
            response->control =
                crowdb::rpc::build_ping_response(server.pool(), frame->request_id, frame->rpc_create_nano, 0);
            response->header.msg_size = response->control == nullptr ? 0 : response->control->len;
            delete frame;
            return response;
        });
        server.start();
        for (int i = 0; i < 100 && received.load(std::memory_order_relaxed) < kRequests; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        EXPECT_EQ(received.load(std::memory_order_relaxed), kRequests);
        server.stop();
        return;
    }

    ASSERT_NE(role, nullptr);
    ASSERT_STREQ(role, "client");
    SocketTransport transport(1, 1);
    transport.start();
    auto connection = transport.connect(address, port);
    ASSERT_NE(connection, nullptr);
    RpcClient client;
    client.set_completion_pool_size(kRequests);
    client.attach(connection.get());
    WorkerCompletion completion;
    for (uint32_t i = 0; i < kRequests; ++i) {
        Buffer *control = crowdb::rpc::build_ping_request(transport.pool(), i + 1, 0);
        ASSERT_NE(control, nullptr);
        ASSERT_TRUE(client.send(&transport, connection.get(), i + 1, control, nullptr, kMessageType,
                                rdma_worker_completion, &completion));
    }
    for (int i = 0; i < 200 && completion.completed.load(std::memory_order_relaxed) < kRequests; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(completion.completed.load(std::memory_order_relaxed), kRequests);
    EXPECT_EQ(completion.failed.load(std::memory_order_relaxed), 0U);
    transport.stop();
}

#endif
