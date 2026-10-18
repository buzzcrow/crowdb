// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_capability.h"

#include <algorithm>
#include <cstring>

namespace crowdb::rpc
{

std::array<uint8_t, RDMA_HANDSHAKE_SIZE> serialize_rdma_handshake(const RdmaEndpointCapability &capability)
{
    std::array<uint8_t, RDMA_HANDSHAKE_SIZE> bytes{};
    auto                                     put16 = [&](size_t offset, uint16_t value) {
        bytes[offset]     = static_cast<uint8_t>(value);
        bytes[offset + 1] = static_cast<uint8_t>(value >> 8);
    };
    auto put64 = [&](size_t offset, uint64_t value) {
        for (size_t i = 0; i < 8; ++i)
            bytes[offset + i] = static_cast<uint8_t>(value >> (i * 8));
    };
    put16(0, RDMA_HANDSHAKE_MAGIC);
    put16(2, capability.protocol_version);
    put64(4, capability.generation);
    put16(12, capability.port);
    bytes[14] = capability.rdma_available ? 1 : 0;
    std::memcpy(bytes.data() + 16, capability.device.data(), std::min<size_t>(32, capability.device.size()));
    std::memcpy(bytes.data() + 48, capability.gid.data(), std::min<size_t>(64, capability.gid.size()));
    for (size_t i = 0; i < capability.data_classes.size(); ++i) {
        const uint32_t value  = capability.data_classes[i];
        const size_t   offset = 112 + i * 4;
        bytes[offset]         = static_cast<uint8_t>(value);
        bytes[offset + 1]     = static_cast<uint8_t>(value >> 8);
        bytes[offset + 2]     = static_cast<uint8_t>(value >> 16);
        bytes[offset + 3]     = static_cast<uint8_t>(value >> 24);
    }
    return bytes;
}

bool parse_rdma_handshake(std::span<const uint8_t> bytes, RdmaEndpointCapability *out)
{
    if (out == nullptr || bytes.size() < RDMA_HANDSHAKE_SIZE)
        return false;
    auto get16 = [&](size_t offset) {
        return static_cast<uint16_t>(bytes[offset]) | static_cast<uint16_t>(bytes[offset + 1] << 8);
    };
    auto get64 = [&](size_t offset) {
        uint64_t value = 0;
        for (size_t i = 0; i < 8; ++i)
            value |= static_cast<uint64_t>(bytes[offset + i]) << (i * 8);
        return value;
    };
    if (get16(0) != RDMA_HANDSHAKE_MAGIC)
        return false;
    out->protocol_version = get16(2);
    out->generation       = get64(4);
    out->port             = get16(12);
    out->rdma_available   = bytes[14] != 0;
    out->device.assign(reinterpret_cast<const char *>(bytes.data() + 16), 32);
    out->gid.assign(reinterpret_cast<const char *>(bytes.data() + 48), 64);
    out->device.resize(out->device.find('\0') == std::string::npos ? out->device.size() : out->device.find('\0'));
    out->gid.resize(out->gid.find('\0') == std::string::npos ? out->gid.size() : out->gid.find('\0'));
    for (size_t i = 0; i < out->data_classes.size(); ++i) {
        const size_t offset  = 112 + i * 4;
        out->data_classes[i] = static_cast<uint32_t>(bytes[offset]) | (static_cast<uint32_t>(bytes[offset + 1]) << 8) |
                               (static_cast<uint32_t>(bytes[offset + 2]) << 16) |
                               (static_cast<uint32_t>(bytes[offset + 3]) << 24);
    }
    return true;
}

bool validate_rdma_capability(const RdmaEndpointCapability &capability, uint16_t expected_protocol,
                              uint64_t expected_generation, const std::string &expected_device)
{
    if (!capability.rdma_available || capability.protocol_version != expected_protocol ||
        capability.generation != expected_generation || capability.port == 0 || capability.device.empty() ||
        capability.gid.empty() || (!expected_device.empty() && capability.device != expected_device)) {
        return false;
    }
    return capability.data_classes[0] == RDMA_CONTROL_CAPACITY &&
           capability.data_classes[1] == RDMA_METADATA_CAPACITY &&
           capability.data_classes[2] == RDMA_AGGREGATE_CAPACITY &&
           capability.data_classes[3] == RDMA_DATAPATH_CAPACITY;
}

bool can_fallback_to_tcp(RdmaPathState state, bool payload_submitted)
{
    if (payload_submitted)
        return false;
    return state == RdmaPathState::Unavailable || state == RdmaPathState::Failed || state == RdmaPathState::Connecting;
}

bool RdmaConnectionState::begin_probe()
{
    if (state_ != RdmaPathState::Unavailable)
        return false;
    state_ = RdmaPathState::Probing;
    return true;
}

bool RdmaConnectionState::begin_connect()
{
    if (state_ != RdmaPathState::Probing)
        return false;
    state_ = RdmaPathState::Connecting;
    return true;
}

bool RdmaConnectionState::mark_ready()
{
    if (state_ != RdmaPathState::Connecting)
        return false;
    state_ = RdmaPathState::Ready;
    return true;
}

bool RdmaConnectionState::mark_payload_submitted()
{
    if (state_ != RdmaPathState::Ready || payload_submitted_)
        return false;
    payload_submitted_ = true;
    return true;
}

bool RdmaConnectionState::fail()
{
    if (state_ != RdmaPathState::Connecting && state_ != RdmaPathState::Ready)
        return false;
    state_ = RdmaPathState::Failed;
    return true;
}

bool RdmaConnectionState::begin_drain()
{
    if (state_ != RdmaPathState::Ready && state_ != RdmaPathState::Failed)
        return false;
    state_ = RdmaPathState::Draining;
    return true;
}

bool RdmaConnectionState::fallback_to_tcp()
{
    if (!can_fallback_to_tcp(state_, payload_submitted_))
        return false;
    state_ = RdmaPathState::TcpFallback;
    return true;
}

} // namespace crowdb::rpc
