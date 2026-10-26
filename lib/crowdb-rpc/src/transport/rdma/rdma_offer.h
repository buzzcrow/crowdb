// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_capability.h"

namespace crowdb::rpc
{
constexpr size_t RDMA_CM_OFFER_SIZE = 52;

struct RdmaCmOffer
{
    uint16_t       receive_frame_limit       = 0;
    uint32_t       receive_slot_capacity     = 0;
    RdmaWireLayout wire_layout               = RdmaWireLayout::Contiguous;
    uint64_t       generation                = 0;
    uint64_t       expected_generation       = 0;
    uint64_t       identity                  = 0;
    uint64_t       expected_identity         = 0;
    uint32_t       max_control_message_bytes = 0;
};

std::array<uint8_t, RDMA_CM_OFFER_SIZE> serialize_rdma_offer(const RdmaEndpointCapability &local,
                                                             const RdmaEndpointCapability *expected_peer = nullptr);
std::array<uint8_t, RDMA_CM_OFFER_SIZE> serialize_rdma_reply(const RdmaEndpointCapability &local,
                                                             const RdmaCmOffer            &peer);
bool                                    parse_rdma_offer(std::span<const uint8_t> bytes, RdmaCmOffer *out);
bool validate_rdma_offer(const RdmaCmOffer &offer, const RdmaEndpointCapability &local,
                         const RdmaEndpointCapability *expected_peer, bool require_preflight);
} // namespace crowdb::rpc
