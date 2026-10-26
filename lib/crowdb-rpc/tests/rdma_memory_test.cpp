// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_rdma_device.h"
#include "common/test_rdma_transport.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

#    include <chrono>
#    include <thread>
#    include <vector>

using namespace crowdb::rpc;

TEST(RdmaMemoryTest, ConcurrentReservationsStayWithinBudget)
{
    auto device    = std::make_shared<RdmaDevice>();
    device->memory = std::make_shared<RdmaMemoryStats>(4096);
    std::atomic<uint32_t>    errors{0};
    std::vector<std::thread> threads;
    for (uint32_t producer = 0; producer < 8; ++producer) {
        threads.emplace_back([&] {
            for (uint32_t iteration = 0; iteration < 20000; ++iteration) {
                auto lease = device->reserve_bytes(1024);
                if (device->memory->current.load() > 4096)
                    errors.fetch_add(1);
                if (lease != nullptr)
                    std::this_thread::yield();
            }
        });
    }
    for (auto &thread : threads)
        thread.join();
    EXPECT_EQ(errors.load(), 0U);
    EXPECT_EQ(device->memory->current.load(), 0U);
    EXPECT_GT(device->memory->high_water.load(), 0U);
    EXPECT_LE(device->memory->high_water.load(), 4096U);
}

TEST(RdmaMemoryTest, MrFailureRollsBackBytesAndReportsFailure)
{
    RdmaConfig config;
    config.device = "test-no-device";
    RdmaTransport transport(config);
    auto          device = std::make_shared<RdmaDevice>();
    device->memory       = std::make_shared<RdmaMemoryStats>(4096);
    TestRdmaTransport::install_device_for_tests(transport, device);
    EXPECT_EQ(TestRdmaTransport::make_pool_for_tests(transport, 1024, 1), nullptr);
    EXPECT_EQ(transport.stats().registered_bytes.load(), 0U);
    EXPECT_EQ(transport.stats().registered_bytes_high_water.load(), 1024U);
    EXPECT_EQ(transport.stats().registration_failures.load(), 1U);
    EXPECT_EQ(transport.stats().registration_rejects.load(), 0U);
    EXPECT_EQ(TestRdmaTransport::make_pool_for_tests(transport, 8192, 1), nullptr);
    EXPECT_EQ(transport.stats().registered_bytes.load(), 0U);
    EXPECT_EQ(transport.stats().registration_rejects.load(), 1U);
}

TEST(RdmaMemoryTest, RetiredRealMrBlocksReplacementUntilFinalLeaseRelease)
{
    auto real = TestRdmaDevice::open();
    if (real == nullptr)
        GTEST_SKIP() << "no accessible RDMA device";
    auto release_result = real->release_result;
    auto device         = std::make_shared<RdmaDevice>();
    device->memory      = std::make_shared<RdmaMemoryStats>(2048);
    auto reservation    = device->reserve_bytes(2048);
    ASSERT_NE(reservation, nullptr);
    auto owner = std::make_shared<std::pair<std::shared_ptr<TestRdmaDevice>, std::shared_ptr<void>>>(
        real, std::move(reservation));
    auto pool = std::make_unique<RdmaBufferPool>(real->pd, 2048, 2, owner);
    ASSERT_TRUE(pool->ready());
    auto *buffer = pool->alloc(1);
    ASSERT_NE(buffer, nullptr);
    buffer->write("held", 4);
    owner.reset();
    real.reset();
    pool.reset();
    EXPECT_EQ(device->memory->current.load(), 2048U);
    EXPECT_EQ(device->reserve_bytes(1024), nullptr);
    EXPECT_EQ(device->memory->rejects.load(), 1U);
    EXPECT_EQ(release_result->load(), -1);
    EXPECT_EQ(buffer->payload_data()[0], 'h');
    buffer->release();
    EXPECT_EQ(device->memory->current.load(), 0U);
    EXPECT_EQ(release_result->load(), 0);
    EXPECT_NE(device->reserve_bytes(2048), nullptr);
}

TEST(RdmaHardwareTest, QpSetupRejectsRemainingRegistrationBudgetExhaustion)
{
    const char *address = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    RdmaConfig config;
    config.sq_depth = config.rq_depth = config.posted_receive_target = 8;
    config.wire_slot_capacity                                        = 1024;
    config.send_arena_bytes = config.receive_arena_bytes = 8192;
    config.cq_depth                                      = 32;
    config.registered_memory_budget_bytes                = 32768;
    RdmaTransport server(config);
    RdmaTransport client(config);
    ASSERT_TRUE(server.listen(address, 19389));
    std::shared_ptr<Connection> incoming;
    std::thread                 acceptor([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (incoming == nullptr && std::chrono::steady_clock::now() < deadline) {
            incoming = server.accept();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    auto                        outgoing = client.connect(address, 19389);
    acceptor.join();
    ASSERT_NE(outgoing, nullptr);
    ASSERT_NE(incoming, nullptr);
    EXPECT_EQ(client.stats().registered_bytes.load(), 32768U);
    EXPECT_EQ(client.connect(address, 19389), nullptr);
    EXPECT_TRUE(outgoing->is_open());
    EXPECT_TRUE(incoming->is_open());
    EXPECT_EQ(client.stats().registered_bytes.load(), 32768U);
    EXPECT_EQ(client.stats().registered_bytes_high_water.load(), 32768U);
    EXPECT_GT(client.stats().registration_rejects.load(), 0U);
    EXPECT_EQ(client.stats().registration_failures.load(), 0U);
}
#endif
