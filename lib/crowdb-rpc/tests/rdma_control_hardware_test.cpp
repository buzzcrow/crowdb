// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_aggregate.h"
#include "common/test_rdma_control.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

#    include <thread>

using namespace crowdb::rpc;
using namespace std::chrono_literals;

class RdmaControlHardwareTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        if (std::getenv("CROWDB_RDMA_E2E") == nullptr || std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR") == nullptr) {
            GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
        }
    }
};

TEST_F(RdmaControlHardwareTest, AsymmetricClassesRoundTripBeyondTheWholeReceiveWindow)
{
    for (const auto capacity : {RDMA_METADATA_CAPACITY, RDMA_AGGREGATE_CAPACITY, RDMA_DATAPATH_CAPACITY}) {
        const auto size          = (2 * capacity) + 13;
        auto       client_config = test_control_config(capacity, size);
        auto       server_config = test_control_config(RDMA_DATAPATH_CAPACITY, size);
        auto       pair          = TestControlPeers::create(client_config, server_config);
        ASSERT_NE(pair, nullptr);
        for (auto *transport : {pair->client.get(), pair->server.get()}) {
            const auto owner      = transport == pair->client.get() ? pair->outgoing.get() : pair->incoming.get();
            const auto capacities = TestRdmaTransport::receive_capacities_for_tests(*transport, owner);
            ASSERT_EQ(capacities.size(), 2U);
            for (const auto actual : capacities) {
                EXPECT_EQ(actual, capacity);
            }
        }
        auto *request = test_control_frame(pair->application, 43, size);
        ASSERT_NE(request, nullptr);
        const std::vector<uint8_t> expected(request->control->data, request->control->data + size);
        pair->incoming->set_on_frame([&](Frame *frame, Connection *connection) {
            EXPECT_EQ(frame->control, expected);
            EXPECT_EQ(frame->header.data_size, 0U);
            EXPECT_EQ(frame->data_buf, nullptr);
            auto *reply = test_control_reply(frame);
            if (!pair->server->submit(connection, reply)) {
                ADD_FAILURE() << "complete logical response was not admitted";
                release_rdma_frames(reply);
            }
        });
        std::unique_ptr<Frame> received;
        pair->outgoing->set_on_frame([&](Frame *frame, Connection *) { received.reset(frame); });
        ASSERT_TRUE(pair->client->submit(pair->outgoing.get(), request));
        ASSERT_TRUE(pair->drain_until([&] { return received != nullptr; }));
        EXPECT_EQ(received->control, expected);
        EXPECT_EQ(received->request_id, 43U);
        EXPECT_EQ(pair->client->stats().control_fragments_sent.load(), 3U);
        EXPECT_EQ(pair->server->stats().control_fragments_sent.load(), 3U);
        EXPECT_EQ(pair->client->stats().rpc_frames_received.load(), 1U);
        EXPECT_EQ(pair->server->stats().rpc_frames_received.load(), 1U);
        EXPECT_EQ(pair->client->stats().control_receive_bytes.load(), size);
        EXPECT_EQ(pair->client->stats().copied_control_receive_bytes.load(), size);
        received.reset();
        EXPECT_EQ(pair->client->stats().control_receive_bytes.load(), 0U);
        ASSERT_TRUE(pair->drain_until([&] { return pair->server->stats().control_send_bytes.load() == 0; }));
        EXPECT_EQ(pair->server->stats().control_receive_bytes.load(), 0U);
        for (auto *transport : {pair->client.get(), pair->server.get()}) {
            EXPECT_EQ(transport->stats().protocol_errors.load(), 0U);
            EXPECT_EQ(transport->stats().cq_errors.load(), 0U);
            EXPECT_EQ(transport->stats().receive_post_errors.load(), 0U);
            EXPECT_EQ(transport->stats().fallback_count.load(), 0U);
        }
    }
}

TEST_F(RdmaControlHardwareTest, HeldFinalOwnerBoundsTheRqAndResumesMessagesInFifoOrder)
{
    const auto size                    = (2 * RDMA_METADATA_CAPACITY) + 13;
    auto       config                  = test_control_config(RDMA_METADATA_CAPACITY, size);
    config.control_assembly_timeout_ms = 25;
    auto pair                          = TestControlPeers::create(config, config);
    ASSERT_NE(pair, nullptr);
    std::array<std::atomic<Frame *>, 3> held{};
    pair->incoming->set_on_frame([&](Frame *frame, Connection *) {
        if (frame->request_id >= 1 && frame->request_id <= held.size()) {
            held[frame->request_id - 1].store(frame, std::memory_order_release);
        }
        else {
            delete frame;
        }
    });
    TestControlPoller poller(*pair->server);
    auto              wait_client = [&](auto ready) {
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (!ready() && std::chrono::steady_clock::now() < deadline) {
            pair->client->drain_cq();
            std::this_thread::yield();
        }
        return ready();
    };
    ASSERT_TRUE(pair->client->submit(pair->outgoing.get(), test_control_frame(pair->application, 1, size)));
    ASSERT_TRUE(wait_client([&] { return held[0].load(std::memory_order_acquire) != nullptr; }));
    ASSERT_TRUE(wait_client([&] { return pair->client->stats().control_send_bytes.load() == 0; }));
    for (uint64_t id : {2, 3}) {
        ASSERT_TRUE(pair->client->submit(pair->outgoing.get(), test_control_frame(pair->application, id, size)));
    }
    ASSERT_TRUE(wait_client([&] { return pair->server->stats().pending_deferred_receives.load() == 2; }));
    const auto registered = pair->server->stats().registered_bytes.load();
    const auto posted     = pair->server->stats().posted_receives.load();
    std::this_thread::sleep_for(50ms);
    EXPECT_TRUE(pair->incoming->is_open());
    EXPECT_EQ(held[1].load(std::memory_order_acquire), nullptr);
    EXPECT_EQ(pair->server->stats().control_stream_timeouts.load(), 0U);
    EXPECT_EQ(pair->server->stats().control_receive_bytes.load(), size);
    EXPECT_EQ(pair->server->stats().registered_bytes.load(), registered);
    EXPECT_EQ(pair->server->stats().posted_receives.load(), posted);
    EXPECT_GT(pair->server->stats().control_receive_stalls.load(), 0U);
    const auto wakeups = pair->server->stats().cq_wakeups.load();
    delete held[0].exchange(nullptr, std::memory_order_acq_rel);
    ASSERT_TRUE(wait_client([&] { return held[1].load(std::memory_order_acquire) != nullptr; }));
    EXPECT_EQ(held[1].load()->request_id, 2U);
    EXPECT_GT(pair->server->stats().cq_wakeups.load(), wakeups);
    delete held[1].exchange(nullptr, std::memory_order_acq_rel);
    ASSERT_TRUE(wait_client([&] { return held[2].load(std::memory_order_acquire) != nullptr; }));
    EXPECT_EQ(held[2].load()->request_id, 3U);
    poller.stop();
    std::unique_ptr<Frame> final_owner(held[2].exchange(nullptr));
    EXPECT_EQ(pair->server->stats().control_receive_bytes_high_water.load(), size);
    EXPECT_EQ(pair->server->stats().control_receive_bytes.load(), size);
    EXPECT_EQ(pair->server->stats().protocol_errors.load(), 0U);
    EXPECT_EQ(pair->server->stats().cq_errors.load(), 0U);
    EXPECT_EQ(pair->server->stats().poller_count.load(), 1U);
    pair.reset();
    EXPECT_EQ(final_owner->control.size(), size);
    EXPECT_EQ(final_owner->request_id, 3U);
    final_owner.reset();
}

TEST_F(RdmaControlHardwareTest, PartialAssemblyExpiresAndNeverPublishesAnApplicationFrame)
{
    const auto size                    = (2 * RDMA_METADATA_CAPACITY) + 13;
    auto       config                  = test_control_config(RDMA_METADATA_CAPACITY, size);
    config.control_assembly_timeout_ms = 10;
    auto pair                          = TestControlPeers::create(config, config);
    ASSERT_NE(pair, nullptr);
    uint32_t published = 0;
    pair->incoming->set_on_frame([&](Frame *frame, Connection *) {
        ++published;
        delete frame;
    });
    ASSERT_TRUE(pair->client->submit(pair->outgoing.get(), test_control_frame(pair->application, 1, size)));
    ASSERT_EQ(TestRdmaTransport::flush_for_tests(*pair->client, pair->outgoing.get(), 1), 1U);
    TestControlPoller poller(*pair->server);
    const auto        deadline = std::chrono::steady_clock::now() + 1s;
    while (pair->server->stats().control_fragments_received.load() == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    ASSERT_EQ(pair->server->stats().control_fragments_received.load(), 1U);
    EXPECT_EQ(pair->server->stats().control_receive_bytes.load(), size);
    while (pair->incoming->is_open() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    poller.stop();
    EXPECT_FALSE(pair->incoming->is_open());
    EXPECT_EQ(published, 0U);
    EXPECT_EQ(pair->server->stats().control_receive_bytes.load(), 0U);
    EXPECT_EQ(pair->server->stats().control_stream_timeouts.load(), 1U);
    EXPECT_EQ(pair->server->stats().fallback_count.load(), 0U);
    pair->client->shutdown();
    EXPECT_EQ(pair->client->stats().control_send_bytes.load(), 0U);
}

TEST_F(RdmaControlHardwareTest, QueueRejectionRestoresCallerAndInFlightSourceRetainsItsByteLease)
{
    const auto size            = RDMA_METADATA_CAPACITY + 13;
    auto       config          = test_control_config(RDMA_METADATA_CAPACITY, size);
    config.send_queue_capacity = 2;
    auto pair                  = TestControlPeers::create(config, config);
    ASSERT_NE(pair, nullptr);
    pair->incoming->set_on_frame([](Frame *frame, Connection *) { delete frame; });
    for (uint64_t id : {1, 2}) {
        ASSERT_TRUE(pair->client->submit(pair->outgoing.get(), test_control_frame(pair->application, id, size)));
    }
    auto *rejected = test_control_frame(pair->application, 3, size);
    auto *original = rejected->control;
    EXPECT_FALSE(pair->client->submit(pair->outgoing.get(), rejected));
    EXPECT_EQ(rejected->control, original);
    EXPECT_EQ(original->ref->load(), 1);
    release_rdma_frames(rejected);
    EXPECT_EQ(pair->client->stats().control_send_bytes.load(), uint64_t{2} * size);
    ASSERT_EQ(TestRdmaTransport::flush_for_tests(*pair->client, pair->outgoing.get(), 2), 2U);
    EXPECT_EQ(pair->client->stats().control_send_bytes.load(), uint64_t{2} * size);
    EXPECT_EQ(pair->client->stats().rpc_frames_sent.load(), 1U);
    ASSERT_TRUE(pair->drain_until([&] { return pair->server->stats().rpc_frames_received.load() == 2; }));
    ASSERT_TRUE(pair->drain_until([&] { return pair->client->stats().control_send_bytes.load() == 0; }));
    EXPECT_EQ(pair->client->stats().send_errors.load(), 0U);
}

TEST_F(RdmaControlHardwareTest, PeerLogicalLimitRejectsBeforePublishingOrChargingBytes)
{
    const auto size          = RDMA_METADATA_CAPACITY + 13;
    const auto server_config = test_control_config(RDMA_METADATA_CAPACITY, size);
    const auto client_config = test_control_config(RDMA_METADATA_CAPACITY, 2 * size);
    auto       pair          = TestControlPeers::create(client_config, server_config);
    ASSERT_NE(pair, nullptr);
    auto *request  = test_control_frame(pair->application, 43, size + 1);
    auto *original = request->control;
    EXPECT_FALSE(pair->client->submit(pair->outgoing.get(), request));
    EXPECT_EQ(request->control, original);
    EXPECT_EQ(pair->client->stats().control_send_bytes.load(), 0U);
    EXPECT_EQ(pair->client->stats().send_queue_high_water.load(), 0U);
    EXPECT_EQ(pair->client->stats().sends.load(), 0U);
    EXPECT_EQ(pair->client->stats().submit_rejects.load(), 1U);
    release_rdma_frames(request);
}

TEST_F(RdmaControlHardwareTest, MalformedWireIdentityFailsTheWholeLogicalControl)
{
    const auto size   = RDMA_METADATA_CAPACITY + 13;
    const auto config = test_control_config(RDMA_METADATA_CAPACITY, size);
    for (const bool wrong_generation : {true, false}) {
        auto pair = TestControlPeers::create(config, config);
        ASSERT_NE(pair, nullptr);
        uint32_t published = 0;
        pair->incoming->set_on_frame([&](Frame *frame, Connection *) {
            ++published;
            delete frame;
        });
        auto           *original = test_control_frame(pair->application, 44, size);
        ControlFragment f{.request_id      = 43,
                          .rpc_create_nano = 11,
                          .stream_id       = 1,
                          .generation      = pair->client->local_capability().generation + (wrong_generation ? 1 : 0),
                          .total           = size,
                          .capacity        = RDMA_METADATA_CAPACITY,
                          .message_type    = 100,
                          .stream_flags    = CONTROL_STREAM_START};
        const auto      bytes = std::span(original->control->data, size);
        ASSERT_TRUE(TestRdmaTransport::post_fragment_for_tests(*pair->client, pair->outgoing.get(), f,
                                                               bytes.first(RDMA_METADATA_CAPACITY)));
        ASSERT_TRUE(pair->drain_until([&] {
            return pair->server->stats().control_fragments_received.load() != 0 || !pair->incoming->is_open();
        }));
        if (!wrong_generation) {
            f.sequence     = 1;
            f.offset       = RDMA_METADATA_CAPACITY;
            f.stream_flags = CONTROL_STREAM_END;
            ASSERT_TRUE(TestRdmaTransport::post_fragment_for_tests(*pair->client, pair->outgoing.get(), f,
                                                                   bytes.subspan(RDMA_METADATA_CAPACITY)));
            ASSERT_TRUE(pair->drain_until([&] { return !pair->incoming->is_open(); }));
        }
        pair->server->drain_cq();
        EXPECT_EQ(published, 0U);
        EXPECT_FALSE(pair->incoming->is_open());
        EXPECT_EQ(pair->server->stats().protocol_errors.load(), 1U);
        EXPECT_EQ(pair->server->stats().control_receive_bytes.load(), 0U);
        EXPECT_EQ(pair->server->stats().fallback_count.load(), 0U);
        release_rdma_frames(original);
    }
}
#endif

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
TEST_F(RdmaControlHardwareTest, MixedControlSizesDoNotStrandTheFixedSendWindow)
{
    constexpr auto capacity = RDMA_AGGREGATE_CAPACITY;
    auto           config   = test_control_config(capacity, capacity + 13);
    auto           pair     = TestControlPeers::create(config, config);
    ASSERT_NE(pair, nullptr);
    uint32_t received = 0;
    pair->incoming->set_on_frame([&](Frame *frame, Connection *connection) {
        auto *reply = test_control_reply(frame);
        if (!pair->server->submit(connection, reply)) {
            ADD_FAILURE() << "logical response admission failed";
            release_rdma_frames(reply);
        }
    });
    pair->outgoing->set_on_frame([&](Frame *frame, Connection *) {
        ++received;
        delete frame;
    });
    for (uint64_t id : {1, 2}) {
        ASSERT_TRUE(pair->client->submit(pair->outgoing.get(), test_control_frame(pair->application, id, 4096)));
    }
    ASSERT_TRUE(pair->drain_until([&] { return received == 2; }));
    ASSERT_TRUE(pair->client->submit(pair->outgoing.get(), test_control_frame(pair->application, 3, capacity + 13)));
    EXPECT_TRUE(pair->drain_until([&] { return received == 3; }))
        << "cached smaller allocations must not strand the configured SQ data slots";
    EXPECT_EQ(pair->client->stats().fallback_count.load(), 0U);
    EXPECT_EQ(pair->server->stats().fallback_count.load(), 0U);
}
#endif
