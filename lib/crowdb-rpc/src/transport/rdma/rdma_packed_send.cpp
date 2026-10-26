// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_cq.h"
#include "rdma_packed_frame.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <algorithm>

namespace crowdb::rpc
{
bool RdmaTransport::prepare_packed_send(Connection *connection, ConnectionPools &pools, bool &handled)
{
    handled     = false;
    auto &queue = *completion_queues_[pools.cq_index];
    if (!config_.aggregation_enabled || config_.aggregation_max_frames < 2 || pools.peer_receive_frame_limit < 2) {
        return true;
    }
    const auto bytes_limit = std::min({config_.aggregation_max_bytes, pools.effective_data_capacity, 65536U});
    const auto small_limit = std::min(RDMA_SMALL_FRAME_BYTES, bytes_limit / 2);
    uint32_t   first_bytes = 0, second_bytes = 0;
    if (!rdma_packable_frame(*pools.pending_frame, small_limit, first_bytes)) {
        return true;
    }
    if (pools.lookahead_frame == nullptr) {
        (void)pools.send_queue.drain(&pools.lookahead_frame, 1);
    }
    if (pools.lookahead_frame == nullptr || !rdma_packable_frame(*pools.lookahead_frame, small_limit, second_bytes) ||
        !send_window_available(pools, RDMA_CONTROL_CAPACITY + first_bytes + second_bytes) ||
        !queue.can_prepare(RDMA_CONTROL_CAPACITY + first_bytes + second_bytes, 2)) {
        return true;
    }
    auto *prefix = pools.send->alloc(RDMA_CONTROL_CAPACITY);
    auto *data   = prefix == nullptr ? nullptr : send_pool_->alloc(config_.data_slot_capacity());
    if (data == nullptr) {
        if (prefix != nullptr) {
            prefix->release();
        }
        return false;
    }
    handled          = true;
    data->len        = 0;
    OutFrame  *first = nullptr;
    OutFrame **tail  = &first;
    uint32_t   count = 0;
    queue.charge_preparation(RDMA_CONTROL_CAPACITY, 0);
    const auto frame_limit = std::min({config_.aggregation_max_frames, pools.peer_receive_frame_limit, 32U});
    while (pools.pending_frame != nullptr && count < frame_limit) {
        auto    *frame = pools.pending_frame;
        uint32_t bytes = 0;
        if (!rdma_packable_frame(*frame, small_limit, bytes) || bytes > bytes_limit - data->len ||
            !send_window_available(pools, RDMA_CONTROL_CAPACITY + data->len + bytes) || !queue.can_prepare(bytes)) {
            break;
        }
        copy_rdma_frame(data->data + data->len, *frame);
        queue.charge_preparation(bytes);
        const auto payload_bytes = bytes - HEADER_SIZE - frame->control->len;
        queue.copied_payload_bytes.fetch_add(payload_bytes, std::memory_order_relaxed);
        queue.copied_aggregate_payload_bytes.fetch_add(payload_bytes, std::memory_order_relaxed);
        queue.copied_framing_control_bytes.fetch_add(frame->control->len, std::memory_order_relaxed);
        data->len += bytes;
        frame->next_aggregate = nullptr;
        *tail                 = frame;
        tail                  = &frame->next_aggregate;
        pools.queued_frames.fetch_sub(1, std::memory_order_release);
        pools.pending_frame = std::exchange(pools.lookahead_frame, nullptr);
        if (pools.pending_frame == nullptr) {
            (void)pools.send_queue.drain(&pools.pending_frame, 1);
        }
        ++count;
    }
    write_rdma_packed_control(prefix->data, data->len, count);
    prefix->len = RDMA_CONTROL_CAPACITY;
    return prepare_send(connection, pools, data, prefix, first, count, RDMA_CONTROL_CAPACITY + data->len);
}
} // namespace crowdb::rpc
#endif
