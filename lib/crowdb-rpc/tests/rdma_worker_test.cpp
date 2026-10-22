// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/socket_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <gtest/gtest.h>

#    include <algorithm>
#    include <array>
#    include <chrono>
#    include <cstdlib>
#    include <cstring>
#    include <thread>
#    include <unordered_set>
#    include <vector>

using namespace crowdb::rpc;

namespace
{
uint64_t monotonic_ns()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

uint32_t test_setting(const char *name, uint32_t fallback)
{
    const char *value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::strtoul(value, nullptr, 10));
}
} // namespace

struct TestTransportStop
{
    SocketTransport &transport;

    ~TestTransportStop()
    {
        transport.stop();
    }
};

struct WorkerCompletion
{
    std::atomic<uint32_t>                   completed{0};
    std::atomic<uint32_t>                   failed{0};
    std::atomic<uint32_t>                   bad_payloads{0};
    std::vector<uint64_t>                   submitted;
    std::vector<uint64_t>                   latencies;
    std::unique_ptr<std::atomic<uint8_t>[]> seen;

    explicit WorkerCompletion(uint32_t requests)
        : submitted(requests + 1),
          latencies(requests + 1),
          seen(std::make_unique<std::atomic<uint8_t>[]>(requests + 1))
    {
    }
};

extern "C" void rdma_worker_completion(uint64_t request_id, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                       crowdb_rpc_status status, void *opaque)
{
    auto *state = static_cast<WorkerCompletion *>(opaque);
    if (request_id == 0 || request_id >= state->submitted.size() ||
        state->seen[request_id].exchange(1, std::memory_order_acq_rel) != 0) {
        state->bad_payloads.fetch_add(1, std::memory_order_release);
    }
    else {
        state->latencies[request_id] = monotonic_ns() - state->submitted[request_id];
    }
    if (status != CROWDB_RPC_OK)
        std::fprintf(stderr, "R239 worker completion failed status=%d request=%llu\n", status,
                     static_cast<unsigned long long>(request_id));
    if (status == CROWDB_RPC_OK) {
        const uint32_t length = data == nullptr ? 0 : crowdb_rpc_buffer_len(data);
        const auto    *bytes  = data == nullptr ? nullptr : crowdb_rpc_buffer_data(data);
        if (length != 128 || bytes == nullptr || !std::all_of(bytes, bytes + length, [request_id](uint8_t value) {
                return value == static_cast<uint8_t>(request_id);
            }))
            state->bad_payloads.fetch_add(1, std::memory_order_release);
    }
    (status == CROWDB_RPC_OK ? state->completed : state->failed).fetch_add(1, std::memory_order_release);
    if (control != nullptr)
        crowdb_rpc_buffer_release(control);
    if (data != nullptr)
        crowdb_rpc_buffer_release(data);
}

TEST(RdmaHardwareTest, RpcServerClientWorkerRoundTrip)
{
    if (std::getenv("CROWDB_RDMA_RPC_E2E") == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_RPC_E2E=1 to run the worker flow";
    const char *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    const char *role    = std::getenv("CROWDB_RDMA_E2E_ROLE");
    ASSERT_NE(address, nullptr);
    const int port =
        std::getenv("CROWDB_RDMA_E2E_PORT") == nullptr ? 19101 : std::atoi(std::getenv("CROWDB_RDMA_E2E_PORT"));
    const uint32_t kRequests    = test_setting("CROWDB_RDMA_TEST_REQUESTS", 1000);
    const uint32_t kConnections = test_setting("CROWDB_RDMA_TEST_CONNECTIONS", 1);
    const uint32_t kWorkers     = test_setting("CROWDB_RDMA_TEST_WORKERS", 1);
    const uint32_t kWindow      = test_setting("CROWDB_RDMA_TEST_WINDOW", 1);
    ASSERT_GT(kRequests, 0U);
    ASSERT_LE(kRequests, 1000000U);
    ASSERT_GT(kConnections, 0U);
    ASSERT_LE(kConnections, 32U);
    ASSERT_GT(kWorkers, 0U);
    ASSERT_LE(kWorkers, 32U);
    ASSERT_GT(kWindow, 0U);
    ASSERT_LE(kWindow, 1024U);
    constexpr uint16_t kMessageType  = 239;
    constexpr uint16_t kFinishedType = 240;

    if (role != nullptr && std::strcmp(role, "server") == 0) {
        RpcServer server(nullptr, 1, kWorkers);
        ASSERT_TRUE(server.listen(address, port));
        std::atomic<uint32_t> received{0};
        std::atomic<bool>     client_finished{false};
        server.register_handler(kFinishedType, [&client_finished](Frame *frame, Connection *) {
            client_finished.store(true, std::memory_order_release);
            delete frame;
            return static_cast<OutFrame *>(nullptr);
        });
        server.register_handler(kMessageType, [&received, &server](Frame *frame, Connection *connection) {
            EXPECT_EQ(connection->pool(), server.pool());
            received.fetch_add(1, std::memory_order_relaxed);
            return handle_echo(frame, connection);
        });
        server.start();
        for (int i = 0; i < 100 && !client_finished.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        EXPECT_EQ(received.load(std::memory_order_relaxed), kRequests);
        EXPECT_TRUE(client_finished.load(std::memory_order_acquire));
#    ifdef CROWDB_RPC_TEST_UTIL
        if (const auto *stats = server.transport()->rdma_stats_for_tests(); stats != nullptr)
            std::fprintf(stderr, "server RDMA: send=%llu recv=%llu cq_errors=%llu polls=%llu posted=%llu\n",
                         static_cast<unsigned long long>(stats->sends.load()),
                         static_cast<unsigned long long>(stats->receives.load()),
                         static_cast<unsigned long long>(stats->cq_errors.load()),
                         static_cast<unsigned long long>(stats->cq_polls.load()),
                         static_cast<unsigned long long>(stats->posted_receives.load()));
#    endif
        server.stop();
        return;
    }

    ASSERT_NE(role, nullptr);
    ASSERT_STREQ(role, "client");
    SocketTransport transport(1, kWorkers);
    transport.start();
    std::vector<std::shared_ptr<Connection>> connections;
    std::unordered_set<uint32_t>             qp_numbers;
    WorkerCompletion                         completion(kRequests);
    RpcClient                                client;
    TestTransportStop                        stop{transport};
    client.set_completion_pool_size(kWindow * 2);
    for (uint32_t index = 0; index < kConnections; ++index) {
        auto connection = transport.connect(address, port);
        ASSERT_NE(connection, nullptr);
        ASSERT_EQ(connection->path(), crowdb::rpc::ConnectionPath::Rdma);
        auto *qp = reinterpret_cast<ibv_qp *>(connection->transport_handle);
        ASSERT_NE(qp, nullptr);
        qp_numbers.insert(qp->qp_num);
        client.attach(connection.get());
        connections.push_back(std::move(connection));
    }
    ASSERT_EQ(qp_numbers.size(), kConnections);
    std::fprintf(stderr, "RDMA topology: physical_hosts=1 shared_hca=%s independent_client_qps=%zu\n",
                 std::getenv("CROWDB_RDMA_DEVICE"), qp_numbers.size());
    const auto started  = std::chrono::steady_clock::now();
    const auto deadline = started + std::chrono::seconds(10);
    for (uint32_t i = 0; i < kRequests; ++i) {
        while (i - completion.completed.load(std::memory_order_acquire) >= kWindow &&
               completion.failed.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
        ASSERT_EQ(completion.failed.load(std::memory_order_acquire), 0U);
        ASSERT_LT(std::chrono::steady_clock::now(), deadline);
        completion.submitted[i + 1] = monotonic_ns();
        Buffer *control             = crowdb::rpc::build_ping_request(transport.pool(), i + 1, 0);
        ASSERT_NE(control, nullptr);
        Buffer *data = transport.pool()->alloc(128);
        ASSERT_NE(data, nullptr);
        std::array<uint8_t, 128> payload{};
        payload.fill(static_cast<uint8_t>(i + 1));
        data->write(payload.data(), static_cast<uint32_t>(payload.size()));
        ASSERT_TRUE(client.send(&transport, connections[i % kConnections].get(), i + 1, control, data, kMessageType,
                                rdma_worker_completion, &completion));
    }
    while (completion.completed.load(std::memory_order_acquire) < kRequests &&
           completion.failed.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    const auto finished_at = std::chrono::steady_clock::now();
    EXPECT_EQ(completion.completed.load(std::memory_order_relaxed), kRequests);
    EXPECT_EQ(completion.failed.load(std::memory_order_relaxed), 0U);
    EXPECT_EQ(completion.bad_payloads.load(std::memory_order_relaxed), 0U);
    if (completion.completed.load(std::memory_order_acquire) == kRequests) {
        Buffer *finished = crowdb::rpc::build_ping_request(transport.pool(), kRequests + 1, 0);
        ASSERT_NE(finished, nullptr);
        ASSERT_TRUE(client.send_one_way(&transport, connections.front().get(), kRequests + 1, finished, nullptr,
                                        kFinishedType));
    }
#    ifdef CROWDB_RPC_TEST_UTIL
    if (const auto *stats = transport.rdma_stats_for_tests(); stats != nullptr)
        std::fprintf(stderr, "client RDMA: send=%llu recv=%llu cq_errors=%llu polls=%llu posted=%llu\n",
                     static_cast<unsigned long long>(stats->sends.load()),
                     static_cast<unsigned long long>(stats->receives.load()),
                     static_cast<unsigned long long>(stats->cq_errors.load()),
                     static_cast<unsigned long long>(stats->cq_polls.load()),
                     static_cast<unsigned long long>(stats->posted_receives.load()));
#    endif
    transport.stop();
    const double seconds = std::chrono::duration<double>(finished_at - started).count();
    auto         latency = completion.latencies;
    latency.erase(latency.begin());
    std::sort(latency.begin(), latency.end());
    auto percentile = [&](size_t percent) {
        return static_cast<double>(latency[(latency.size() - 1) * percent / 100]) / 1000.0;
    };
    std::fprintf(stderr,
                 "RDMA load: requests=%u workers=%u connections=%u window=%u TPS=%.0f p50_us=%.2f p95_us=%.2f "
                 "p99_us=%.2f errors=%u\n",
                 kRequests, kWorkers, kConnections, kWindow, kRequests / seconds, percentile(50), percentile(95),
                 percentile(99), completion.failed.load());
}

#endif
