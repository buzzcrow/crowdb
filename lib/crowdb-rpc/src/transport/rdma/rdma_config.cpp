// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_config.h"

#include "crowdb-common/log.h"
#include "crowdb-rpc/framing.h"

#include <charconv>
#include <cstdlib>
#include <limits>
#include <string_view>

namespace crowdb::rpc
{

RdmaConfig RdmaConfig::for_layout(RdmaWireLayout layout, uint32_t data_capacity)
{
    RdmaConfig config;
    config.wire_layout = layout;
    const uint64_t wire =
        static_cast<uint64_t>(data_capacity) + (layout == RdmaWireLayout::ControlData ? RDMA_CONTROL_CAPACITY : 0);
    config.wire_slot_capacity  = wire > UINT32_MAX ? 0 : static_cast<uint32_t>(wire);
    config.receive_arena_bytes = static_cast<uint64_t>(config.posted_receive_target) * config.wire_slot_capacity;
    return config;
}

bool RdmaConfig::apply_environment()
{
    auto read = []<typename T>(const char *name, T &destination) {
        const char *value = std::getenv(name);
        if (value == nullptr)
            return true;
        const std::string_view input(value);
        uint64_t               parsed = 0;
        const auto             result = std::from_chars(input.data(), input.data() + input.size(), parsed);
        if (result.ec != std::errc{} || result.ptr != input.data() + input.size() ||
            parsed > std::numeric_limits<T>::max()) {
            CRB_LOG_WARN("rdma configuration: invalid unsigned value name={} value={}", name, value);
            return false;
        }
        destination = static_cast<T>(parsed);
        return true;
    };
    auto layout = static_cast<uint32_t>(wire_layout);
    if (!read("CROWDB_RDMA_WIRE_LAYOUT", layout) || layout > static_cast<uint32_t>(RdmaWireLayout::ControlData))
        return false;
    wire_layout = static_cast<RdmaWireLayout>(layout);
    return (port != 1 || read("CROWDB_RDMA_PORT", port)) && read("CROWDB_RDMA_SQ_DEPTH", sq_depth) &&
           read("CROWDB_RDMA_SEND_BATCH_SIZE", send_batch_size) &&
           read("CROWDB_RDMA_RECEIVE_BATCH_SIZE", receive_batch_size) &&
           read("CROWDB_RDMA_SEND_WINDOW_SIZE", send_window_size) &&
           read("CROWDB_RDMA_SEND_WINDOW_BYTES", send_window_bytes) && read("CROWDB_RDMA_RQ_DEPTH", rq_depth) &&
           read("CROWDB_RDMA_POSTED_RECEIVE_TARGET", posted_receive_target) &&
           read("CROWDB_RDMA_WIRE_SLOT_CAPACITY", wire_slot_capacity) &&
           read("CROWDB_RDMA_SEND_ARENA_BYTES", send_arena_bytes) &&
           read("CROWDB_RDMA_RECEIVE_ARENA_BYTES", receive_arena_bytes) && read("CROWDB_RDMA_CQ_DEPTH", cq_depth) &&
           read("CROWDB_RDMA_REGISTERED_MEMORY_BUDGET_BYTES", registered_memory_budget_bytes) &&
           read("CROWDB_RDMA_CQ_COUNT", cq_count) && read("CROWDB_RDMA_POLL_BATCH", poll_batch) &&
           read("CROWDB_RDMA_VERBS_POLL_BATCH", verbs_poll_batch) &&
           read("CROWDB_RDMA_POLL_EMPTY_BUDGET", poll_empty_budget) &&
           read("CROWDB_RDMA_AGGREGATION_ENABLED", aggregation_enabled) &&
           read("CROWDB_RDMA_AGGREGATION_MAX_BYTES", aggregation_max_bytes) &&
           read("CROWDB_RDMA_AGGREGATION_MAX_FRAMES", aggregation_max_frames) &&
           read("CROWDB_RDMA_SEND_QUEUE_CAPACITY", send_queue_capacity) &&
           read("CROWDB_RDMA_MAX_CONTROL_MESSAGE_BYTES", max_control_message_bytes) &&
           read("CROWDB_RDMA_CONTROL_SEND_BUDGET_BYTES", control_send_budget_bytes) &&
           read("CROWDB_RDMA_CONTROL_RECEIVE_BUDGET_BYTES", control_receive_budget_bytes) &&
           read("CROWDB_RDMA_CONTROL_ASSEMBLY_TIMEOUT_MS", control_assembly_timeout_ms);
}

bool RdmaConfig::valid() const
{
    const bool direct = wire_layout == RdmaWireLayout::ControlData;
    return send_batch_size > 0 && send_batch_size <= 256 && receive_batch_size > 0 && receive_batch_size <= 256 &&
           send_window_size <= sq_depth && (send_window_bytes == 0 || send_window_bytes >= wire_slot_capacity) &&
           port > 0 && valid_rdma_wire_capacity(wire_slot_capacity, wire_layout) &&
           (!direct || (qp_max_send_sge >= 2 && qp_max_recv_sge >= 2)) && sq_depth > 0 && rq_depth > 0 &&
           posted_receive_target > 0 && posted_receive_target <= rq_depth && cq_depth > posted_receive_target &&
           cq_depth <= static_cast<uint32_t>(INT32_MAX) && cq_count > 0 && cq_count <= 256 &&
           static_cast<uint64_t>(cq_count) * cq_depth < UINT32_MAX && poll_batch > 0 &&
           poll_batch <= static_cast<uint32_t>(INT32_MAX) && verbs_poll_batch > 0 && verbs_poll_batch <= 256 &&
           poll_empty_budget > 0 && send_arena_bytes >= data_slot_capacity() &&
           receive_arena_bytes >= static_cast<uint64_t>(posted_receive_target) * wire_slot_capacity &&
           min_rnr_timer <= 31 && retry_count <= 7 && rnr_retry_count <= 7 && registration_budget() != 0 &&
           aggregation_max_bytes >= HEADER_SIZE && aggregation_max_bytes <= RDMA_DATAPATH_CAPACITY &&
           aggregation_max_frames > 0 && aggregation_max_frames <= 256 && send_queue_capacity >= 2 &&
           send_queue_capacity <= (1U << 20) && (send_queue_capacity & (send_queue_capacity - 1)) == 0 &&
           max_control_message_bytes != 0 && max_control_message_bytes <= INT32_MAX &&
           max_control_message_bytes <= control_send_budget_bytes &&
           max_control_message_bytes <= control_receive_budget_bytes && control_assembly_timeout_ms != 0;
}

uint64_t RdmaConfig::registration_budget() const
{
    const uint64_t entries = static_cast<uint64_t>(sq_depth) + posted_receive_target;
    const uint64_t maximum = std::numeric_limits<uint64_t>::max();
    if (entries == 0 || cq_depth < entries || send_arena_bytes > maximum / 2 ||
        receive_arena_bytes > maximum - send_arena_bytes)
        return 0;
    const uint64_t compatibility = 2 * send_arena_bytes;
    const uint64_t per_qp        = send_arena_bytes + receive_arena_bytes;
    if (per_qp == 0 || per_qp > maximum - compatibility)
        return 0;
    if (registered_memory_budget_bytes != 0)
        return registered_memory_budget_bytes >= compatibility + per_qp ? registered_memory_budget_bytes : 0;
    const uint64_t connections = (cq_depth / entries) * cq_count;
    if (connections == 0 || connections > (maximum - compatibility) / per_qp)
        return 0;
    return compatibility + connections * per_qp;
}

} // namespace crowdb::rpc
