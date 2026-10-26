// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_offer.h"

namespace crowdb::rpc
{
namespace
{
constexpr uint32_t OFFER_MAGIC = 0x434d5244;

void put(std::span<uint8_t> bytes, size_t offset, uint64_t value, size_t size)
{
    for (size_t index = 0; index < size; ++index)
        bytes[offset + index] = static_cast<uint8_t>(value >> (index * 8));
}

uint64_t get(std::span<const uint8_t> bytes, size_t offset, size_t size)
{
    uint64_t value = 0;
    for (size_t index = 0; index < size; ++index)
        value |= static_cast<uint64_t>(bytes[offset + index]) << (index * 8);
    return value;
}
} // namespace

std::array<uint8_t, RDMA_CM_OFFER_SIZE> serialize_rdma_offer(const RdmaEndpointCapability &local,
                                                             const RdmaEndpointCapability *expected_peer)
{
    std::array<uint8_t, RDMA_CM_OFFER_SIZE> bytes{};
    put(bytes, 0, OFFER_MAGIC, 4);
    put(bytes, 4, RDMA_PROTOCOL_VERSION, 2);
    put(bytes, 6, local.receive_frame_limit, 2);
    put(bytes, 8, local.receive_slot_capacity, 4);
    put(bytes, 12, static_cast<uint32_t>(local.wire_layout), 4);
    put(bytes, 16, local.generation, 8);
    put(bytes, 24, expected_peer == nullptr ? 0 : expected_peer->generation, 8);
    put(bytes, 32, rdma_capability_identity(local), 8);
    put(bytes, 40, expected_peer == nullptr ? 0 : rdma_capability_identity(*expected_peer), 8);
    put(bytes, 48, local.max_control_message_bytes, 4);
    return bytes;
}

bool parse_rdma_offer(std::span<const uint8_t> bytes, RdmaCmOffer *out)
{
    if (out == nullptr || bytes.size() < RDMA_CM_OFFER_SIZE || get(bytes, 0, 4) != OFFER_MAGIC ||
        get(bytes, 4, 2) != RDMA_PROTOCOL_VERSION ||
        get(bytes, 12, 4) > static_cast<uint32_t>(RdmaWireLayout::ControlData))
        return false;
    RdmaCmOffer offer;
    offer.receive_frame_limit       = static_cast<uint16_t>(get(bytes, 6, 2));
    offer.receive_slot_capacity     = static_cast<uint32_t>(get(bytes, 8, 4));
    offer.wire_layout               = static_cast<RdmaWireLayout>(get(bytes, 12, 4));
    offer.generation                = get(bytes, 16, 8);
    offer.expected_generation       = get(bytes, 24, 8);
    offer.identity                  = get(bytes, 32, 8);
    offer.expected_identity         = get(bytes, 40, 8);
    offer.max_control_message_bytes = static_cast<uint32_t>(get(bytes, 48, 4));
    if (!valid_rdma_wire_capacity(offer.receive_slot_capacity, offer.wire_layout) || offer.receive_frame_limit == 0 ||
        offer.receive_frame_limit > 256 || offer.generation == 0 || offer.identity == 0 ||
        (offer.expected_generation == 0) != (offer.expected_identity == 0) || offer.max_control_message_bytes == 0)
        return false;
    *out = offer;
    return true;
}

std::array<uint8_t, RDMA_CM_OFFER_SIZE> serialize_rdma_reply(const RdmaEndpointCapability &local,
                                                             const RdmaCmOffer            &peer)
{
    auto bytes = serialize_rdma_offer(local);
    put(bytes, 24, peer.generation, 8);
    put(bytes, 40, peer.identity, 8);
    return bytes;
}

bool validate_rdma_offer(const RdmaCmOffer &offer, const RdmaEndpointCapability &local,
                         const RdmaEndpointCapability *expected_peer, bool require_preflight)
{
    if (offer.wire_layout != local.wire_layout)
        return false;
    if (require_preflight && offer.expected_generation == 0)
        return false;
    if (offer.expected_generation != 0 &&
        (offer.expected_generation != local.generation || offer.expected_identity != rdma_capability_identity(local)))
        return false;
    return expected_peer == nullptr || (offer.generation == expected_peer->generation &&
                                        offer.identity == rdma_capability_identity(*expected_peer) &&
                                        offer.receive_slot_capacity == expected_peer->receive_slot_capacity &&
                                        offer.wire_layout == expected_peer->wire_layout &&
                                        offer.max_control_message_bytes == expected_peer->max_control_message_bytes &&
                                        offer.receive_frame_limit == expected_peer->receive_frame_limit);
}
} // namespace crowdb::rpc
