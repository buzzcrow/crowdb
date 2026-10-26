// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_rdma_load.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include "crowdb-rpc/server/message.h"
#    include "crowdb-rpc/server/server.h"

#    include <gtest/gtest.h>

#    include <algorithm>
#    include <cstdlib>
#    include <cstring>
#    include <unordered_set>

using namespace crowdb::rpc;

namespace
{
uint32_t test_setting(const char *name, uint32_t fallback)
{
    const char *value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::strtoul(value, nullptr, 10));
}

struct TestLoadSettings
{
    uint32_t producer_count         = test_setting("CROWDB_RDMA_TEST_PRODUCERS", 1);
    uint32_t io_workers             = test_setting("CROWDB_RDMA_TEST_WORKERS", 1);
    uint32_t connection_count       = test_setting("CROWDB_RDMA_TEST_CONNECTIONS", 1);
    uint32_t in_flight_per_producer = test_setting("CROWDB_RDMA_TEST_WINDOW", 32);
    uint32_t duration_seconds       = test_setting("CROWDB_RDMA_TEST_SECONDS", 10);
    uint32_t warmup_seconds         = test_setting("CROWDB_RDMA_TEST_WARMUP_SECONDS", 1);

    bool valid() const
    {
        return producer_count > 0 && producer_count <= 32 && io_workers > 0 && io_workers <= 32 &&
               connection_count > 0 && connection_count <= 32 && in_flight_per_producer > 0 &&
               in_flight_per_producer <= 1024 && duration_seconds > 0 && duration_seconds <= 300 &&
               warmup_seconds > 0 && warmup_seconds <= 30;
    }
};

struct TestLoadStop
{
    SocketTransport &transport;

    ~TestLoadStop()
    {
        transport.stop();
    }
};

OutFrame *test_echo(RpcServer &server, Frame *frame)
{
    auto *response            = new OutFrame;
    response->request_id      = frame->request_id;
    response->header.msg_type = TestRdmaLoad::MESSAGE_TYPE;
    response->control         = build_ping_response(server.pool(), frame->request_id, frame->rpc_create_nano, 0);
    response->header.msg_size = response->control == nullptr ? 0 : response->control->len;
    if (frame->data_buf != nullptr) {
        response->data = server.pool()->alloc(frame->data_buf->len);
        if (response->data != nullptr)
            response->data->write(frame->data_buf->payload_data(), frame->data_buf->len);
    }
    delete frame;
    return response;
}

void test_timed_server(const char *address, int port, const TestLoadSettings &settings)
{
    RpcServer server(nullptr, 1, settings.io_workers);
    ASSERT_TRUE(server.listen(address, port));
    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> expected{0};
    std::atomic<bool>     finished{false};
    server.register_handler(TestRdmaLoad::MESSAGE_TYPE, [&](Frame *frame, Connection *) {
        received.fetch_add(1, std::memory_order_relaxed);
        return test_echo(server, frame);
    });
    server.register_handler(TestRdmaLoad::FINISHED_TYPE, [&](Frame *frame, Connection *) {
        expected.store(frame->request_id, std::memory_order_relaxed);
        finished.store(true, std::memory_order_release);
        delete frame;
        return static_cast<OutFrame *>(nullptr);
    });
    server.start();
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(settings.warmup_seconds + settings.duration_seconds + 30);
    while (!finished.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    server.stop();
    EXPECT_TRUE(finished.load(std::memory_order_acquire));
    EXPECT_GT(received.load(), 0U);
    EXPECT_EQ(received.load(), expected.load());
}

void test_report_load(const TestLoadSettings &settings, TestRdmaLoad &load)
{
    crowdb::common::metrics::LatencyHistogram::Data histogram;
    uint64_t                                        completed = 0, submitted = 0, drained = 0;
    for (const auto &producer : load.producers) {
        completed += producer->window_completed.load();
        submitted += producer->measured_submitted;
        drained += producer->drain_completed.load();
        histogram.merge(producer->latency.capture_total());
    }
    const auto latency = histogram.snapshot();
    EXPECT_EQ(latency.count, completed);
    EXPECT_EQ(load.errors.load(), 0U);
    EXPECT_GT(completed, 0U);
    EXPECT_EQ(completed + drained, submitted);
    std::fprintf(stderr,
                 "RDMA timed load: duration_s=%u warmup_s=%u producer_count=%u io_workers=%u connection_count=%u "
                 "in_flight_per_producer=%u payload_bytes=%u submitted=%llu completed=%llu drain_completed=%llu "
                 "TPS=%.0f p50_us=%.2f p95_us=%.2f p99_us=%.2f mean_us=%.2f max_us=%.2f errors=%llu "
                 "p90_us=%.2f p999_us=%.2f histogram_overflow=%llu\n",
                 settings.duration_seconds, settings.warmup_seconds, settings.producer_count, settings.io_workers,
                 settings.connection_count, settings.in_flight_per_producer, TestRdmaLoad::PAYLOAD_BYTES,
                 static_cast<unsigned long long>(submitted), static_cast<unsigned long long>(completed),
                 static_cast<unsigned long long>(drained), static_cast<double>(completed) / settings.duration_seconds,
                 static_cast<double>(latency.p50) / 1000.0, static_cast<double>(latency.p95) / 1000.0,
                 static_cast<double>(latency.p99) / 1000.0, latency.avg / 1000.0,
                 static_cast<double>(latency.max) / 1000.0, static_cast<unsigned long long>(load.errors.load()),
                 static_cast<double>(latency.p90) / 1000.0, static_cast<double>(latency.p999) / 1000.0,
                 static_cast<unsigned long long>(latency.overflow_count));
}

void test_timed_client(const char *address, int port, const TestLoadSettings &settings)
{
    SocketTransport transport(1, settings.io_workers);
    TestRdmaLoad    load(settings.producer_count, settings.in_flight_per_producer, settings.duration_seconds);
    RpcClient       client;
    TestLoadStop    stop{transport};
    client.set_completion_pool_size(load.id_stride * 4);
    transport.start();
    std::vector<std::shared_ptr<Connection>> connections;
    std::unordered_set<uint32_t>             qp_numbers;
    std::unordered_set<ibv_cq *>             cq_pointers;
    for (uint32_t index = 0; index < settings.connection_count; ++index) {
        auto connection = transport.connect(address, port);
        ASSERT_NE(connection, nullptr);
        ASSERT_EQ(connection->path(), ConnectionPath::Rdma);
        auto *qp = reinterpret_cast<ibv_qp *>(connection->transport_handle);
        ASSERT_NE(qp, nullptr);
        ASSERT_EQ(qp->send_cq, qp->recv_cq);
        cq_pointers.insert(qp->send_cq);
        qp_numbers.insert(qp->qp_num);
        client.attach(connection.get());
        connections.push_back(std::move(connection));
    }
    ASSERT_EQ(qp_numbers.size(), settings.connection_count);
    ASSERT_EQ(cq_pointers.size(), std::min(settings.connection_count, test_setting("CROWDB_RDMA_CQ_COUNT", 1)));
#    ifdef CROWDB_RPC_TEST_UTIL
    const auto *stats = transport.rdma_stats_for_tests();
    ASSERT_NE(stats, nullptr);
    ASSERT_EQ(stats->poller_count.load(), test_setting("CROWDB_RDMA_CQ_COUNT", 1));
#    endif
    std::fprintf(stderr, "RDMA topology: physical_hosts=1 shared_hca=%s independent_client_qps=%zu active_cqs=%zu\n",
                 std::getenv("CROWDB_RDMA_DEVICE"), qp_numbers.size(), cq_pointers.size());
    std::vector<std::thread> threads;
    for (uint32_t index = 0; index < settings.producer_count; ++index)
        threads.emplace_back([&, index] {
            load.producers[index]->run(client, transport, connections[index % connections.size()].get());
        });
    while (load.ready_producers.load(std::memory_order_acquire) != settings.producer_count)
        std::this_thread::yield();
    load.warmup_end_ns.store(test_rdma_clock_ns() + settings.warmup_seconds * 1'000'000'000ULL,
                             std::memory_order_release);
    while (load.warmed_producers.load(std::memory_order_acquire) != settings.producer_count)
        std::this_thread::yield();
    load.measure_end_ns.store(test_rdma_clock_ns() + settings.duration_seconds * 1'000'000'000ULL,
                              std::memory_order_release);
    for (auto &thread : threads)
        thread.join();
    uint64_t total_submitted = 0;
    for (const auto &producer : load.producers)
        total_submitted += producer->total_submitted;
    if (load.errors.load() == 0) {
        auto *finished = build_ping_request(transport.pool(), total_submitted, 0);
        ASSERT_NE(finished, nullptr);
        EXPECT_TRUE(client.send_one_way(&transport, connections.front().get(), total_submitted, finished, nullptr,
                                        TestRdmaLoad::FINISHED_TYPE));
    }
    transport.stop();
    test_report_load(settings, load);
}
} // namespace

TEST(RdmaHardwareTest, RpcServerClientTimedLoad)
{
    if (std::getenv("CROWDB_RDMA_RPC_E2E") == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_RPC_E2E=1 to run the timed worker load";
    const char            *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    const char            *role    = std::getenv("CROWDB_RDMA_E2E_ROLE");
    const TestLoadSettings settings;
    ASSERT_NE(address, nullptr);
    ASSERT_NE(role, nullptr);
    ASSERT_TRUE(settings.valid());
    const int port = static_cast<int>(test_setting("CROWDB_RDMA_E2E_PORT", 19101));
    if (std::strcmp(role, "server") == 0)
        test_timed_server(address, port, settings);
    else {
        ASSERT_STREQ(role, "client");
        test_timed_client(address, port, settings);
    }
}

#endif
