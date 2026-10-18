// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

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

TEST(RdmaCapabilityTest, BuildsControlAndDataSgesWithoutConcatenation)
{
    SystemBufferPool pool(2);
    auto            *control = pool.alloc(1024);
    auto            *data    = pool.alloc(64U << 10);
    ASSERT_NE(control, nullptr);
    ASSERT_NE(data, nullptr);
    const uint8_t control_bytes[] = {1, 2};
    const uint8_t data_bytes[]    = {3, 4, 5};
    control->write(control_bytes, sizeof(control_bytes));
    data->write(data_bytes, sizeof(data_bytes));

    crowdb::rpc::RdmaFrameSges sges;
    ASSERT_TRUE(crowdb::rpc::build_rdma_frame_sges(control, data, &sges));
    EXPECT_EQ(sges.count, 2);
    EXPECT_EQ(sges.entries[0].address, reinterpret_cast<uintptr_t>(control->data));
    EXPECT_EQ(sges.entries[0].length, sizeof(control_bytes));
    EXPECT_EQ(sges.entries[1].address, reinterpret_cast<uintptr_t>(data->data));
    EXPECT_EQ(sges.entries[1].length, sizeof(data_bytes));
    data->release();
    control->release();
}
