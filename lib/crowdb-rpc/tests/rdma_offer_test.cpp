// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_offer.h"
#include "../src/transport/rdma/rdma_preflight.h"
#include "common/test_environment.h"
#include "crowdb-rpc/server/message.h"
#include "crowdb-rpc/server/server.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <thread>

using namespace crowdb::rpc;

namespace
{
RdmaEndpointCapability test_capability(uint64_t generation)
{
    return {
        .rdma_available        = true,
        .protocol_version      = RDMA_PROTOCOL_VERSION,
        .generation            = generation,
        .port                  = 19101,
        .device                = "test-device",
        .gid                   = "fe80::1",
        .data_classes          = {1024, 65536, 262144, 1048576},
        .receive_slot_capacity = 1024,
        .receive_frame_limit   = 1,
        .cm_address            = "10.200.1.1"
    };
}

void test_echo_round_trip(SocketTransport &transport, const std::shared_ptr<Connection> &connection)
{
    struct TestEchoResult
    {
        std::atomic<bool> complete{false};
        std::atomic<bool> valid{false};
    };

    auto result = std::make_shared<TestEchoResult>();
    connection->set_on_frame([result](Frame *frame, Connection *) {
        result->valid.store(frame->request_id == 91 && frame->header.msg_type == 100 && frame->data_buf != nullptr &&
                            frame->data_buf->len == 16 &&
                            std::memcmp(frame->data_buf->payload_data(), "fallback payload", 16) == 0);
        delete frame;
        result->complete.store(true, std::memory_order_release);
    });
    auto *control = build_ping_request(transport.pool(), 91, 0);
    auto *data    = transport.pool()->alloc(16);
    ASSERT_NE(control, nullptr);
    ASSERT_NE(data, nullptr);
    data->write("fallback payload", 16);
    auto      *frame    = build_out_frame(91, 100, control, data);
    const bool accepted = transport.submit(connection.get(), frame);
    if (!accepted) {
        control->release();
        frame->release_data();
        delete frame;
    }
    ASSERT_TRUE(accepted);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!result->complete.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EXPECT_TRUE(result->complete.load());
    EXPECT_TRUE(result->valid.load());
}
} // namespace

TEST(RdmaOfferTest, PrivateDataBindsPreflightIdentityAndReceiveAdmission)
{
    const auto client            = test_capability(11);
    auto       server            = test_capability(17);
    server.receive_slot_capacity = 65536;
    RdmaCmOffer decoded;
    const auto  wire = serialize_rdma_offer(client, &server);
    ASSERT_TRUE(parse_rdma_offer(wire, &decoded));
    EXPECT_EQ(wire.size(), 48U);
    EXPECT_EQ(decoded.receive_slot_capacity, 1024U);
    EXPECT_TRUE(validate_rdma_offer(decoded, server, &client, true));
    auto stale = server;
    ++stale.generation;
    EXPECT_FALSE(validate_rdma_offer(decoded, stale, &client, true));
    stale     = server;
    stale.gid = "fe80::2";
    EXPECT_FALSE(validate_rdma_offer(decoded, stale, &client, true));
    RdmaCmOffer reply;
    ASSERT_TRUE(parse_rdma_offer(serialize_rdma_reply(server, decoded), &reply));
    EXPECT_TRUE(validate_rdma_offer(reply, client, &server, true));
    server.receive_slot_capacity = 1024;
    EXPECT_FALSE(validate_rdma_offer(reply, client, &server, true));
}

TEST(RdmaOfferTest, MissingPreflightAndMalformedAdmissionFailClosed)
{
    const auto  client = test_capability(1);
    const auto  server = test_capability(2);
    auto        wire   = serialize_rdma_offer(client);
    RdmaCmOffer decoded;
    ASSERT_TRUE(parse_rdma_offer(wire, &decoded));
    EXPECT_TRUE(validate_rdma_offer(decoded, server, nullptr, false));
    EXPECT_FALSE(validate_rdma_offer(decoded, server, nullptr, true));
    for (size_t size = 0; size < RDMA_CM_OFFER_SIZE; ++size)
        EXPECT_FALSE(parse_rdma_offer(std::span(wire).first(size), &decoded));
    wire[8] = 1;
    EXPECT_FALSE(parse_rdma_offer(wire, &decoded));
    wire     = serialize_rdma_offer(client);
    wire[12] = 1;
    EXPECT_FALSE(parse_rdma_offer(wire, &decoded));
}

TEST(RdmaOfferTest, TcpCapabilityCarriesCmAddressAndRetainsLegacyFallbackParsing)
{
    const auto             source = test_capability(27);
    auto                   wire   = serialize_rdma_handshake(source);
    RdmaEndpointCapability decoded;
    ASSERT_TRUE(parse_rdma_handshake(wire, &decoded));
    EXPECT_EQ(decoded.cm_address, source.cm_address);
    EXPECT_EQ(decoded.receive_slot_capacity, source.receive_slot_capacity);
    EXPECT_EQ(decoded.receive_frame_limit, source.receive_frame_limit);
    EXPECT_TRUE(validate_rdma_capability(decoded, RDMA_PROTOCOL_VERSION, 27, "test-device"));
    EXPECT_FALSE(parse_rdma_handshake(std::span(wire).first(RDMA_LEGACY_HANDSHAKE_SIZE), &decoded));
    wire[2] = 1;
    ASSERT_TRUE(parse_rdma_handshake(std::span(wire).first(RDMA_LEGACY_HANDSHAKE_SIZE), &decoded));
    EXPECT_EQ(decoded.protocol_version, 1U);
    EXPECT_EQ(decoded.receive_slot_capacity, 0U);
}

TEST(RdmaOfferTest, RawTcpPreflightConsumesOnlyCapabilityAndLeavesFallbackUsable)
{
    const TestEnvironment disabled("CROWDB_RDMA_ENABLED", "0");
    RpcServer             server;
    ASSERT_TRUE(server.listen("127.0.0.1", 0));
    server.register_handler(100, handle_echo);
    server.start();
    const auto client = test_capability(91);
    auto       probe  = RdmaPreflight::connect("127.0.0.1", server.listen_port(), client, {});
    ASSERT_NE(probe, nullptr);
    EXPECT_FALSE(probe->peer.rdma_available);
    EXPECT_EQ(probe->peer.protocol_version, RDMA_PROTOCOL_VERSION);
    SocketTransport transport(1, 1);
    transport.start();
    auto connection = transport.create_connection(probe->release_fd(), "fallback");
    ASSERT_NE(connection, nullptr);
    connection->set_peer_capability(probe->peer);
    EXPECT_TRUE(connection->is_open());
    EXPECT_EQ(connection->path(), ConnectionPath::Tcp);
    test_echo_round_trip(transport, connection);
    transport.stop();
    server.stop();
}

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
TEST(RdmaHardwareTest, TcpOnlyPeerSkipsCmAndHonorsFallbackPolicy)
{
    const char *address_env = std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR");
    if (std::getenv("CROWDB_RDMA_E2E") == nullptr || address_env == nullptr)
        GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
    const std::string     address(address_env);
    const TestEnvironment disabled("CROWDB_RDMA_ENABLED", "0");
    RpcServer             server;
    ASSERT_TRUE(server.listen(address, 0));
    server.register_handler(100, handle_echo);
    server.start();
    const TestEnvironment enabled("CROWDB_RDMA_ENABLED", "1");
    {
        const TestEnvironment forbidden("CROWDB_RDMA_ALLOW_TCP_FALLBACK", "0");
        SocketTransport       transport(1, 1);
        ASSERT_TRUE(transport.local_rdma_capability().rdma_available);
        transport.start();
        EXPECT_EQ(transport.connect(address, server.listen_port()), nullptr);
        const auto *stats = transport.rdma_stats_for_tests();
        ASSERT_NE(stats, nullptr);
        EXPECT_EQ(stats->preflight_attempts.load(), 1U);
        EXPECT_EQ(stats->cm_attempts.load(), 0U);
        EXPECT_EQ(stats->fallback_count.load(), 0U);
    }
    {
        const TestEnvironment allowed("CROWDB_RDMA_ALLOW_TCP_FALLBACK", "1");
        SocketTransport       transport(1, 1);
        transport.start();
        auto connection = transport.connect(address, server.listen_port());
        ASSERT_NE(connection, nullptr);
        EXPECT_EQ(connection->path(), ConnectionPath::Tcp);
        const auto *stats = transport.rdma_stats_for_tests();
        ASSERT_NE(stats, nullptr);
        EXPECT_EQ(stats->preflight_attempts.load(), 1U);
        EXPECT_EQ(stats->cm_attempts.load(), 0U);
        EXPECT_EQ(stats->fallback_count.load(), 1U);
        test_echo_round_trip(transport, connection);
        transport.stop();
    }
    server.stop();
}
#endif

TEST(RdmaOfferTest, PublishedPeerCapabilityRejectsIdentityReplacement)
{
    SystemBufferPool pool;
    Connection       connection(1, "capability", &pool);
    auto             peer = test_capability(19);
    connection.set_peer_capability(peer);
    connection.set_peer_capability(peer);
    EXPECT_TRUE(connection.is_open());
    peer.generation++;
    connection.set_peer_capability(peer);
    EXPECT_FALSE(connection.is_open());
    EXPECT_EQ(connection.peer_capability().generation, 19U);
}

TEST(RdmaOfferTest, TruncatedAndStalledControlRepliesFailWithinOneDeadline)
{
    for (bool truncated : {true, false}) {
        const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(listener, 0);
        sockaddr_in address{};
        address.sin_family      = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)), 0);
        ASSERT_EQ(::listen(listener, 1), 0);
        socklen_t length = sizeof(address);
        ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr *>(&address), &length), 0);
        std::atomic<bool> done{false};
        std::thread       peer([&] {
            pollfd descriptor{listener, POLLIN, 0};
            if (::poll(&descriptor, 1, 1000) <= 0)
                return;
            const int fd = ::accept(listener, nullptr, nullptr);
            if (fd < 0)
                return;
            if (truncated) {
                const uint8_t partial[] = {0x01, 0x02};
                (void)::send(fd, partial, sizeof(partial), 0);
                (void)::shutdown(fd, SHUT_WR);
            }
            while (!done.load(std::memory_order_acquire))
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            ::close(fd);
        });
        const auto        begin = std::chrono::steady_clock::now();
        auto       probe   = RdmaPreflight::connect("127.0.0.1", ntohs(address.sin_port), test_capability(1), {}, 30);
        const auto elapsed = std::chrono::steady_clock::now() - begin;
        done.store(true, std::memory_order_release);
        peer.join();
        ::close(listener);
        EXPECT_EQ(probe, nullptr);
        EXPECT_LT(elapsed, std::chrono::seconds(1));
        if (!truncated) {
            EXPECT_GE(elapsed, std::chrono::milliseconds(20));
        }
    }
}
