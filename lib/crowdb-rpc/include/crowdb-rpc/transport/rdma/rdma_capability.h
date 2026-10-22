// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"

#include <array>
#include <cstdint>
#include <span>
#include <string>

namespace crowdb::rpc
{

enum class RdmaPathState : uint8_t {
    Unavailable,
    Probing,
    Connecting,
    Ready,
    Draining,
    Failed,
    TcpFallback,
};

enum class RdmaWireLayout : uint32_t { Contiguous = 0, ControlData = 1 };

[[nodiscard]] uint32_t rdma_data_slot_capacity(uint32_t wire_capacity, RdmaWireLayout layout);
[[nodiscard]] bool     valid_rdma_wire_capacity(uint32_t wire_capacity, RdmaWireLayout layout);

struct RdmaEndpointCapability
{
    bool                    rdma_available   = false;
    uint16_t                protocol_version = 0;
    uint64_t                generation       = 0;
    uint16_t                port             = 0;
    std::string             device;
    std::string             gid;
    std::array<uint32_t, 4> data_classes{};
    uint32_t                receive_slot_capacity = 0;
    uint32_t                receive_frame_limit   = 1;
    std::string             cm_address{};
    RdmaWireLayout          wire_layout = RdmaWireLayout::Contiguous;
};

constexpr uint16_t RDMA_HANDSHAKE_MAGIC       = 0x5244;
constexpr uint16_t RDMA_HANDSHAKE_MSG_TYPE    = 0xFFFE;
constexpr uint16_t RDMA_PROTOCOL_VERSION      = 3;
constexpr size_t   RDMA_LEGACY_HANDSHAKE_SIZE = 128;
constexpr size_t   RDMA_HANDSHAKE_SIZE        = 160;

[[nodiscard]] std::array<uint8_t, RDMA_HANDSHAKE_SIZE>
serialize_rdma_handshake(const RdmaEndpointCapability &capability);

[[nodiscard]] bool parse_rdma_handshake(std::span<const uint8_t> bytes, RdmaEndpointCapability *out);

[[nodiscard]] bool validate_rdma_capability(const RdmaEndpointCapability &capability, uint16_t expected_protocol,
                                            uint64_t expected_generation, const std::string &expected_device);

[[nodiscard]] uint64_t rdma_capability_identity(const RdmaEndpointCapability &capability);

[[nodiscard]] bool can_fallback_to_tcp(RdmaPathState state, bool payload_submitted);

class RdmaConnectionState
{
  public:
    [[nodiscard]] RdmaPathState state() const
    {
        return state_;
    }

    [[nodiscard]] bool payload_submitted() const
    {
        return payload_submitted_;
    }

    bool begin_probe();
    bool begin_connect();
    bool mark_ready();
    bool mark_payload_submitted();
    bool fail();
    bool begin_drain();
    bool fallback_to_tcp();

  private:
    RdmaPathState state_             = RdmaPathState::Unavailable;
    bool          payload_submitted_ = false;
};

} // namespace crowdb::rpc
