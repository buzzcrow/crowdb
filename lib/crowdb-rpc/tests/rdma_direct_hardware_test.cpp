// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_aggregate.h"
#include "common/test_rdma_transport.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

#    include <chrono>
#    include <thread>

using namespace crowdb::rpc;

namespace
{
RdmaConfig direct_config()
{
    RdmaConfig config;
    config.use_environment    = false;
    config.wire_layout        = RdmaWireLayout::ControlData;
    config.wire_slot_capacity = RDMA_DATAPATH_CAPACITY + RDMA_CONTROL_CAPACITY;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 4;
    config.cq_depth                                                  = 8;
    config.send_arena_bytes                                          = 4U * RDMA_DATAPATH_CAPACITY;
    config.receive_arena_bytes                                       = uint64_t{4} * config.wire_slot_capacity;
    config.source_address                                            = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (const auto *device = std::getenv("CROWDB_RDMA_DEVICE"))
        config.device = device;
    return config;
}

template <typename Ready> bool drain_until(RdmaTransport &client, RdmaTransport &server, Ready ready)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!ready() && std::chrono::steady_clock::now() < deadline) {
        client.drain_cq();
        server.drain_cq();
    }
    return ready();
}
} // namespace

TEST(RdmaHardwareTest, DirectDataPreservesBytesAlignmentAndLeaseAdmission)
{
    const auto *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    auto config            = direct_config();
    config.send_batch_size = 4;
    SystemBufferPool application;
    auto             server = std::make_unique<RdmaTransport>(config, &application);
    auto             client = std::make_unique<RdmaTransport>(config, &application);
    ASSERT_TRUE(server->listen(address, 19391));
    std::shared_ptr<Connection> incoming;
    std::thread                 acceptor([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (incoming == nullptr && std::chrono::steady_clock::now() < deadline) {
            incoming = server->accept();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    auto                        outgoing = client->connect(address, 19391);
    acceptor.join();
    ASSERT_NE(incoming, nullptr);
    ASSERT_NE(outgoing, nullptr);
    std::vector<Buffer *>         server_held, client_held;
    const std::array<uint32_t, 4> lengths{0, 13, RDMA_DATAPATH_CAPACITY, 65536};
    incoming->set_on_frame([&](Frame *frame, Connection *connection) {
        EXPECT_GE(frame->request_id, 1U);
        EXPECT_LE(frame->request_id, lengths.size());
        const auto expected = lengths[frame->request_id - 1];
        EXPECT_EQ(frame->data_buf->len, expected);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(frame->data_buf->payload_data()) % 4096, 0U);
        EXPECT_TRUE(std::all_of(frame->data_buf->payload_data(), frame->data_buf->payload_data() + expected,
                                [id = frame->request_id](uint8_t byte) { return byte == id; }));
        server_held.push_back(frame->data_buf->ref_clone());
        auto *reply =
            build_out_frame(frame->request_id, 100, build_ping_response(&application, frame->request_id, 0, 0),
                            frame->data_buf->ref_clone());
        delete frame;
        const bool accepted = server->submit(connection, reply);
        EXPECT_TRUE(accepted);
        if (!accepted)
            release_rdma_frames(reply);
    });
    outgoing->set_on_frame([&](Frame *frame, Connection *) {
        const auto expected = lengths[frame->request_id - 1];
        EXPECT_EQ(frame->data_buf->len, expected);
        EXPECT_TRUE(std::all_of(frame->data_buf->payload_data(), frame->data_buf->payload_data() + expected,
                                [id = frame->request_id](uint8_t byte) { return byte == id; }));
        client_held.push_back(frame->data_buf->ref_clone());
        delete frame;
    });
    Buffer  *source_held = nullptr;
    uint64_t total       = 0;
    for (size_t index = 0; index < lengths.size(); ++index) {
        Buffer *data = nullptr;
        if (lengths[index] != 0) {
            data = index == 3 ? application.alloc(lengths[index]) : client->allocate_producer_buffer(lengths[index]);
            ASSERT_NE(data, nullptr);
            std::memset(data->data, static_cast<int>(index + 1), lengths[index]);
            data->len = lengths[index];
            if (index == 2)
                source_held = data->ref_clone();
            total += lengths[index];
        }
        auto *frame = build_out_frame(index + 1, 100, build_ping_request(&application, index + 1, 0), data);
        ASSERT_TRUE(client->submit(outgoing.get(), frame));
        ASSERT_TRUE(drain_until(*client, *server, [&] { return client_held.size() == index + 1; }));
    }
    ASSERT_EQ(server_held.size(), lengths.size());
    EXPECT_EQ(server->stats().posted_receives.load(), 4U);
    EXPECT_EQ(client->stats().posted_receives.load(), 4U);
    EXPECT_EQ(server->stats().copied_payload_bytes.load(), 0U);
    EXPECT_EQ(client->stats().copied_payload_bytes.load(), lengths.back());
    EXPECT_EQ(server->stats().direct_payload_bytes.load(), total);
    EXPECT_EQ(client->stats().direct_payload_bytes.load(), total - lengths.back());
    server_held.front()->release();
    server_held.erase(server_held.begin());
    EXPECT_EQ(server->stats().posted_receives.load(), 5U);
    EXPECT_EQ(client->stats().posted_receives.load(), 4U);
    // Exhaust receive admission with caller leases, then destroy both peers.
    // The held receive and registered source must still retain their own MR/PD.
    const auto server_device = TestRdmaTransport::device_owner(*server);
    const auto client_device = TestRdmaTransport::device_owner(*client);
    EXPECT_EQ(server->stats().receive_post_errors.load(), 0U);
    EXPECT_EQ(client->stats().receive_post_errors.load(), 0U);
    EXPECT_EQ(server->stats().cq_errors.load(), 0U);
    EXPECT_EQ(client->stats().cq_errors.load(), 0U);
    server.reset();
    client.reset();
    incoming.reset();
    outgoing.reset();
    EXPECT_FALSE(client_device.expired());
    EXPECT_FALSE(server_device.expired());
    EXPECT_EQ(source_held->payload_data()[RDMA_DATAPATH_CAPACITY - 1], 3U);
    for (auto *buffer : server_held)
        buffer->release();
    for (auto *buffer : client_held)
        buffer->release();
    source_held->release();
    EXPECT_TRUE(client_device.expired());
    EXPECT_TRUE(server_device.expired());
}

TEST(RdmaHardwareTest, LayoutMismatchFailsBeforeCmOrUsesExplicitTcpFallback)
{
    const auto *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    auto config                   = direct_config();
    auto contiguous               = config;
    contiguous.wire_layout        = RdmaWireLayout::Contiguous;
    contiguous.wire_slot_capacity = RDMA_DATAPATH_CAPACITY;
    RpcServer server;
    ASSERT_TRUE(server.configure_rdma(contiguous, true, false, true));
    ASSERT_TRUE(server.listen(address, 0));
    server.start();
    for (bool fallback : {false, true}) {
        SocketTransport client(1, 1);
        ASSERT_TRUE(client.configure_rdma(config, true, fallback, false));
        client.start();
        auto connection = client.connect(address, server.listen_port());
        if (fallback) {
            ASSERT_NE(connection, nullptr);
            EXPECT_EQ(connection->path(), ConnectionPath::Tcp);
        }
        else {
            EXPECT_EQ(connection, nullptr);
        }
        const auto *stats = client.rdma_stats_for_tests();
        ASSERT_NE(stats, nullptr);
        EXPECT_EQ(stats->cm_attempts.load(), 0U);
        EXPECT_EQ(stats->fallback_count.load(), static_cast<uint64_t>(fallback));
    }
}

static void test_mixed_staging(bool packed, uint32_t receive_batch = 1)
{
    const auto *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    auto config                = direct_config();
    config.send_batch_size     = 4;
    config.aggregation_enabled = packed;
    config.receive_batch_size  = receive_batch;
    SystemBufferPool application;
    RdmaTransport    server(config, &application), client(config, &application);
    ASSERT_TRUE(server.listen(address, 19392));
    std::shared_ptr<Connection> incoming;
    std::thread                 acceptor([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (incoming == nullptr && std::chrono::steady_clock::now() < deadline) {
            incoming = server.accept();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    auto                        outgoing = client.connect(address, 19392);
    acceptor.join();
    ASSERT_NE(outgoing, nullptr);
    ASSERT_NE(incoming, nullptr);
    uint32_t replies = 0;
    incoming->set_on_frame([&](Frame *frame, Connection *connection) {
        auto *reply =
            build_out_frame(frame->request_id, 100, build_ping_response(&application, frame->request_id, 0, 0),
                            frame->data_buf->ref_clone());
        delete frame;
        const bool accepted = server.submit(connection, reply);
        EXPECT_TRUE(accepted);
        if (!accepted)
            release_rdma_frames(reply);
    });
    std::vector<Buffer *> held;
    outgoing->set_on_frame([&](Frame *frame, Connection *) {
        EXPECT_EQ(frame->request_id, replies + 1);
        const uint32_t expected = frame->request_id <= 4 ? 128 : 65536;
        EXPECT_EQ(frame->data_buf->len, expected);
        EXPECT_TRUE(std::all_of(frame->data_buf->payload_data(), frame->data_buf->payload_data() + expected,
                                [id = frame->request_id](uint8_t byte) { return byte == id; }));
        if (packed && frame->request_id <= 4) {
            held.push_back(frame->data_buf->ref_clone());
        }
        if (expected == 65536) {
            EXPECT_EQ(reinterpret_cast<uintptr_t>(frame->data_buf->payload_data()) % 4096, 0U);
        }
        ++replies;
        delete frame;
    });
    auto submit = [&](uint64_t id, uint32_t size) {
        auto *data = application.alloc(size);
        EXPECT_NE(data, nullptr);
        if (data == nullptr)
            return false;
        std::memset(data->data, static_cast<int>(id), size);
        data->len   = size;
        auto *frame = build_out_frame(id, 100, build_ping_request(&application, id, 0), data);
        if (client.submit(outgoing.get(), frame))
            return true;
        release_rdma_frames(frame);
        return false;
    };
    for (uint64_t id = 1; id <= 4; ++id)
        ASSERT_TRUE(submit(id, 128));
    if (!packed) {
        ASSERT_EQ(TestRdmaTransport::flush_for_tests(client, outgoing.get(), 4), 4U);
        // Compatibility staging cannot consume the registered producer's
        // independent admission while completed SENDs await CQ retirement.
        auto *producer = client.allocate_producer_buffer(128);
        ASSERT_NE(producer, nullptr);
        producer->release();
    }
    ASSERT_TRUE(drain_until(client, server, [&] { return replies == 4; }));
    EXPECT_EQ(client.stats().sends.load(), packed ? 1U : 4U);
    EXPECT_EQ(client.stats().send_post_calls.load(), 1U);
    EXPECT_EQ(client.stats().send_completions.load(), 1U);
    ASSERT_TRUE(submit(5, 65536));
    ASSERT_TRUE(drain_until(client, server, [&] { return replies == 5; }));
    EXPECT_EQ(client.stats().send_post_calls.load(), 2U);
    EXPECT_EQ(client.stats().copied_payload_bytes.load(), 4U * 128 + 65536);
    EXPECT_EQ(client.stats().copied_aggregate_payload_bytes.load(), packed ? 4U * 128 : 0U);
    EXPECT_EQ(client.stats().copied_staging_payload_bytes.load(), packed ? 65536U : 4U * 128 + 65536);
    if (packed) {
        const auto posted = client.stats().posted_receives.load();
        ASSERT_EQ(held.size(), 4U);
        for (size_t index = 0; index < held.size(); ++index) {
            EXPECT_EQ(held[index]->payload_data()[127], index + 1);
            if (index == 3 && receive_batch > 1) {
                std::thread release([buffer = held[index]] { buffer->release(); });
                release.join();
            }
            else {
                held[index]->release();
            }
            EXPECT_EQ(client.stats().posted_receives.load(),
                      posted + static_cast<uint64_t>(index == 3 && receive_batch == 1));
        }
        client.drain_cq();
        EXPECT_EQ(client.stats().posted_receives.load(), posted + 1);
    }
    EXPECT_EQ(client.stats().send_errors.load(), 0U);
    EXPECT_EQ(client.stats().cq_errors.load(), 0U);
}

TEST(RdmaHardwareTest, DirectStagingReusesSmallBurstSlotsForLaterLargePayload)
{
    test_mixed_staging(false);
}

TEST(RdmaHardwareTest, PackedSmallBurstKeepsOrderAlignmentAndSharedReceiveLease)
{
    test_mixed_staging(true);
}

TEST(RdmaHardwareTest, BatchedReceiveRepostsFlushSingletonAndCrossThreadFinalLease)
{
    test_mixed_staging(true, 4);
    test_mixed_staging(false, 4);
}
#endif
