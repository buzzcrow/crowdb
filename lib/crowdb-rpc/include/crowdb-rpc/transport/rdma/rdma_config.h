// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_capability.h"

#include <string>

namespace crowdb::rpc
{
struct RdmaConfig
{
    // Service defaults are applied before environment overrides and probing.
    static RdmaConfig for_layout(RdmaWireLayout layout, uint32_t data_capacity);
    bool              use_environment = true;
    std::string       device;
    std::string       source_address{};
    std::string       gid{};
    bool              require_tcp_preflight          = false;
    uint8_t           port                           = 1;
    uint32_t          cq_depth                       = 8192;
    uint32_t          cq_count                       = 1;
    uint32_t          wire_slot_capacity             = RDMA_DATAPATH_CAPACITY;
    RdmaWireLayout    wire_layout                    = RdmaWireLayout::Contiguous;
    uint64_t          send_arena_bytes               = 64ULL << 20;
    uint64_t          receive_arena_bytes            = 1ULL << 30;
    uint64_t          registered_memory_budget_bytes = 0;
    uint32_t          sq_depth                       = 1024;
    uint32_t          rq_depth                       = 1024;
    uint32_t          qp_max_send_sge                = 2;
    uint32_t          qp_max_recv_sge                = 2;
    uint32_t          poll_batch                     = 256;
    uint32_t          verbs_poll_batch               = 32;
    uint32_t          poll_empty_budget              = 256;
    uint32_t          shutdown_drain_ms              = 3000;
    uint32_t          posted_receive_target          = 1024;
    uint8_t           min_rnr_timer                  = 1;
    uint8_t           retry_count                    = 7;
    uint8_t           rnr_retry_count                = 7;
    bool              aggregation_enabled            = false;
    uint32_t          aggregation_max_bytes          = RDMA_DATAPATH_CAPACITY;
    uint32_t          aggregation_max_frames         = 256;
    uint32_t          send_queue_capacity            = 4096;

    [[nodiscard]] bool     apply_environment();
    [[nodiscard]] bool     valid() const;
    [[nodiscard]] uint64_t registration_budget() const;

    [[nodiscard]] uint32_t data_slot_capacity() const
    {
        return rdma_data_slot_capacity(wire_slot_capacity, wire_layout);
    }

    [[nodiscard]] uint64_t receive_control_bytes() const
    {
        return wire_layout == RdmaWireLayout::ControlData
                 ? static_cast<uint64_t>(posted_receive_target) * RDMA_CONTROL_CAPACITY
                 : 0;
    }
};

} // namespace crowdb::rpc
