// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/transport/rdma/rdma_capability.h"

#include <gtest/gtest.h>

using crowdb::rpc::RdmaConnectionState;
using crowdb::rpc::RdmaEndpointCapability;
using crowdb::rpc::RdmaPathState;
using crowdb::rpc::SystemBufferPool;

TEST(RdmaCapabilityTest, ValidatesGenerationDeviceAndClasses)
{
    RdmaEndpointCapability capability{
        .rdma_available   = true,
        .protocol_version = 1,
        .generation       = 9,
        .port             = 1,
        .device           = "rocep35s0f0",
        .gid              = "fe80::1",
        .data_classes     = {1U << 10, 64U << 10, 256U << 10, 1U << 20}
    };
    EXPECT_TRUE(crowdb::rpc::validate_rdma_capability(capability, 1, 9, "rocep35s0f0"));
    EXPECT_FALSE(crowdb::rpc::validate_rdma_capability(capability, 1, 8, "rocep35s0f0"));
    EXPECT_FALSE(crowdb::rpc::validate_rdma_capability(capability, 1, 9, "rocep35s0f1"));
}

TEST(RdmaCapabilityTest, HandshakeRoundTripsEndpointIdentity)
{
    RdmaEndpointCapability source{
        .rdma_available   = true,
        .protocol_version = 2,
        .generation       = 17,
        .port             = 1,
        .device           = "rocep35s0f0",
        .gid              = "fe80::abcd",
        .data_classes     = {1024, 64U << 10, 256U << 10, 1U << 20}
    };
    const auto             wire = crowdb::rpc::serialize_rdma_handshake(source);
    RdmaEndpointCapability decoded;
    ASSERT_TRUE(crowdb::rpc::parse_rdma_handshake(wire, &decoded));
    EXPECT_EQ(decoded.protocol_version, source.protocol_version);
    EXPECT_EQ(decoded.rdma_available, source.rdma_available);
    EXPECT_EQ(decoded.generation, source.generation);
    EXPECT_EQ(decoded.device, source.device);
    EXPECT_EQ(decoded.gid, source.gid);
    EXPECT_EQ(decoded.data_classes, source.data_classes);
}

TEST(RdmaCapabilityTest, FallbackStopsAfterPayloadSubmission)
{
    EXPECT_TRUE(crowdb::rpc::can_fallback_to_tcp(RdmaPathState::Failed, false));
    EXPECT_TRUE(crowdb::rpc::can_fallback_to_tcp(RdmaPathState::Connecting, false));
    EXPECT_FALSE(crowdb::rpc::can_fallback_to_tcp(RdmaPathState::Ready, false));
    EXPECT_FALSE(crowdb::rpc::can_fallback_to_tcp(RdmaPathState::Failed, true));
}

TEST(RdmaCapabilityTest, ConnectionConsumesCapabilityBeforeApplicationHandlerPublication)
{
    SystemBufferPool        pool;
    crowdb::rpc::Connection connection(1, "handshake", &pool);
    RdmaEndpointCapability  source;
    source.rdma_available   = true;
    source.protocol_version = 1;
    source.generation       = 17;
    const auto wire         = crowdb::rpc::serialize_rdma_handshake(source);
    auto      *frame        = new crowdb::rpc::Frame;
    frame->header.msg_type  = crowdb::rpc::RDMA_HANDSHAKE_MSG_TYPE;
    frame->control.assign(wire.begin(), wire.end());
    connection.on_frame(frame);
    EXPECT_TRUE(connection.peer_handshake_received());
    EXPECT_EQ(connection.peer_capability().generation, source.generation);
    EXPECT_FALSE(connection.has_frame_handler());
}

TEST(RdmaCapabilityTest, ConnectionStateRejectsIllegalUpgradeAndFallback)
{
    RdmaConnectionState state;
    EXPECT_EQ(state.state(), RdmaPathState::Unavailable);
    EXPECT_TRUE(state.begin_probe());
    EXPECT_TRUE(state.begin_connect());
    EXPECT_TRUE(state.mark_ready());
    EXPECT_TRUE(state.mark_payload_submitted());
    EXPECT_FALSE(state.fallback_to_tcp());
    EXPECT_TRUE(state.fail());
    EXPECT_TRUE(state.begin_drain());
    EXPECT_FALSE(state.mark_ready());
}

TEST(RdmaCapabilityTest, SetupFailureCanFallbackBeforePayload)
{
    RdmaConnectionState state;
    ASSERT_TRUE(state.begin_probe());
    ASSERT_TRUE(state.begin_connect());
    ASSERT_TRUE(state.fail());
    EXPECT_TRUE(state.fallback_to_tcp());
    EXPECT_EQ(state.state(), RdmaPathState::TcpFallback);
}
