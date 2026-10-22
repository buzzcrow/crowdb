// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_rdma_device.h"
#include "common/test_rdma_transport.h"
#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/server/message.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <gtest/gtest.h>

#    include <algorithm>
#    include <array>
#    include <chrono>
#    include <cstring>
#    include <thread>

using namespace crowdb::rpc;

TEST(RdmaBufferPoolTest, HeldLeaseRetainsMrAndPdAfterPoolDestruction)
{
    auto device = TestRdmaDevice::open();
    if (device == nullptr)
        GTEST_SKIP() << "no accessible RDMA device";
    auto                          result      = device->release_result;
    std::weak_ptr<TestRdmaDevice> weak_device = device;
    auto                          pool        = std::make_unique<RdmaBufferPool>(device->pd, 4096, 4, device);
    auto                         *buffer      = pool->alloc(32);
    ASSERT_NE(buffer, nullptr);
    buffer->write("held registered payload", 23);
    auto original = new crowdb_rpc_buffer_s{buffer};
    auto clone    = crowdb_rpc_buffer_ref(original);
    ASSERT_NE(original, clone);
    pool.reset();
    device.reset();
    EXPECT_FALSE(weak_device.expired());
    EXPECT_EQ(result->load(), -1);
    EXPECT_EQ(std::memcmp(crowdb_rpc_buffer_data(clone), "held registered payload", 23), 0);
    crowdb_rpc_buffer_release(original);
    EXPECT_FALSE(weak_device.expired());
    crowdb_rpc_buffer_release(clone);
    EXPECT_TRUE(weak_device.expired());
    EXPECT_EQ(result->load(), 0) << "MR must deregister before PD release";
}

TEST(RdmaBufferPoolTest, FailedLargeAllocationPreservesSmallBufferAdmission)
{
    auto device = TestRdmaDevice::open();
    if (device == nullptr)
        GTEST_SKIP() << "no accessible RDMA device";
    RdmaBufferPool pool(device->pd, 2048, 2, device);
    for (int attempt = 0; attempt < 8; ++attempt)
        EXPECT_EQ(pool.alloc(65536), nullptr);
    auto *first  = pool.alloc(1);
    auto *second = pool.alloc(1);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(pool.alloc(1), nullptr);
    first->release();
    second->release();
    EXPECT_TRUE(pool.quiescent());
}

TEST(RdmaBufferPoolTest, ConcurrentRecyclingNeverPublishesOneSlotTwice)
{
    auto device = TestRdmaDevice::open();
    if (device == nullptr)
        GTEST_SKIP() << "no accessible RDMA device";
    RdmaBufferPool                    pool(device->pd, 32 * 1024, 32, device);
    std::array<std::atomic<bool>, 32> occupied{};
    std::atomic<uint64_t>             errors{0};
    std::vector<std::thread>          threads;
    for (uint8_t producer = 1; producer <= 8; ++producer) {
        threads.emplace_back([&, producer] {
            for (uint32_t iteration = 0; iteration < 20000; ++iteration) {
                Buffer *buffer;
                while ((buffer = pool.alloc(32)) == nullptr)
                    std::this_thread::yield();
                if (occupied[buffer->pool_index].exchange(true))
                    errors.fetch_add(1);
                std::memset(buffer->data, producer, 32);
                auto *clone = buffer->ref_clone();
                if (!std::all_of(buffer->data, buffer->data + 32,
                                 [producer](uint8_t value) { return value == producer; }))
                    errors.fetch_add(1);
                buffer->release();
                occupied[clone->pool_index].store(false);
                clone->release();
            }
        });
    }
    for (auto &thread : threads)
        thread.join();
    EXPECT_EQ(errors.load(), 0U);
    EXPECT_TRUE(pool.quiescent());
}

#    ifdef CROWDB_RPC_TEST_UTIL
TEST(RdmaHardwareTest, HeldReceiveOutlivesTransportAndCmIds)
{
    const char *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    RdmaConfig config;
    config.wire_slot_capacity = 1024;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 8;
    config.send_arena_bytes = config.receive_arena_bytes = 8192;
    config.cq_depth                                      = 16;
    SystemBufferPool application_pool;
    auto             server = std::make_unique<RdmaTransport>(config, &application_pool);
    auto             client = std::make_unique<RdmaTransport>(config, &application_pool);
    ASSERT_TRUE(server->listen(address, 19388));
    std::shared_ptr<Connection> incoming;
    std::thread                 acceptor([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (incoming == nullptr && std::chrono::steady_clock::now() < deadline) {
            incoming = server->accept();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    auto                        outgoing = client->connect(address, 19388);
    acceptor.join();
    ASSERT_NE(incoming, nullptr);
    ASSERT_NE(outgoing, nullptr);
    Buffer *held = nullptr;
    incoming->set_on_frame([&](Frame *frame, Connection *) {
        held = frame->data_buf->ref_clone();
        delete frame;
    });
    auto  device        = TestRdmaTransport::device_owner(*server);
    auto  client_device = TestRdmaTransport::device_owner(*client);
    auto *control       = build_ping_request(&application_pool, 71, 0);
    auto *data          = application_pool.alloc(32);
    ASSERT_NE(control, nullptr);
    ASSERT_NE(data, nullptr);
    data->write("receive lease", 13);
    auto *registered = client->register_buffer(data);
    ASSERT_NE(registered, nullptr);
    auto *frame = build_out_frame(71, 239, control, data);
    ASSERT_TRUE(client->submit(outgoing.get(), frame));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (held == nullptr && std::chrono::steady_clock::now() < deadline) {
        client->drain_cq();
        server->drain_cq();
    }
    ASSERT_NE(held, nullptr);
    server.reset();
    client.reset();
    incoming.reset();
    outgoing.reset();
    EXPECT_FALSE(device.expired());
    EXPECT_NE(held->registration_handle, 0U);
    EXPECT_EQ(std::memcmp(held->payload_data(), "receive lease", 13), 0);
    EXPECT_FALSE(client_device.expired());
    EXPECT_EQ(std::memcmp(registered->payload_data(), "receive lease", 13), 0);
    registered->release();
    EXPECT_TRUE(client_device.expired());
    held->release();
    EXPECT_TRUE(device.expired());
}
#    endif
#endif
