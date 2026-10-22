// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include "crowdb-common/log.h"

#    include <charconv>
#    include <cstdlib>
#    include <limits>
#    include <string_view>

namespace crowdb::rpc
{

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
    return (port != 1 || read("CROWDB_RDMA_PORT", port)) && read("CROWDB_RDMA_SQ_DEPTH", sq_depth) &&
           read("CROWDB_RDMA_RQ_DEPTH", rq_depth) && read("CROWDB_RDMA_POSTED_RECEIVE_TARGET", posted_receive_target) &&
           read("CROWDB_RDMA_WIRE_SLOT_CAPACITY", wire_slot_capacity) &&
           read("CROWDB_RDMA_SEND_ARENA_BYTES", send_arena_bytes) &&
           read("CROWDB_RDMA_RECEIVE_ARENA_BYTES", receive_arena_bytes) && read("CROWDB_RDMA_CQ_DEPTH", cq_depth) &&
           read("CROWDB_RDMA_CQ_COUNT", cq_count) && read("CROWDB_RDMA_POLL_BATCH", poll_batch) &&
           read("CROWDB_RDMA_VERBS_POLL_BATCH", verbs_poll_batch) &&
           read("CROWDB_RDMA_POLL_EMPTY_BUDGET", poll_empty_budget);
}

bool RdmaConfig::valid() const
{
    BufferCapacityClass capacity_class;
    uint32_t            capacity = 0;
    return port > 0 && classify_rdma_capacity(wire_slot_capacity, &capacity_class, &capacity) &&
           capacity == wire_slot_capacity && sq_depth > 0 && rq_depth > 0 && posted_receive_target > 0 &&
           posted_receive_target <= rq_depth && cq_depth > posted_receive_target &&
           cq_depth <= static_cast<uint32_t>(INT32_MAX) && cq_count > 0 && cq_count <= 256 &&
           static_cast<uint64_t>(cq_count) * cq_depth < UINT32_MAX && poll_batch > 0 &&
           poll_batch <= static_cast<uint32_t>(INT32_MAX) && verbs_poll_batch > 0 && verbs_poll_batch <= 256 &&
           poll_empty_budget > 0 && send_arena_bytes >= wire_slot_capacity &&
           receive_arena_bytes >= static_cast<uint64_t>(posted_receive_target) * wire_slot_capacity &&
           min_rnr_timer <= 31 && retry_count <= 7 && rnr_retry_count <= 7;
}

} // namespace crowdb::rpc

#endif
