// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_aggregate.h"
#include "rdma_control_send.h"
#include "rdma_cq.h"
#include "rdma_direct_frame.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <algorithm>
#    include <cstring>

namespace crowdb::rpc
{
namespace
{
const uint8_t *contiguous_data(const OutFrame &frame, Buffer *&owner)
{
    const uint8_t *first = nullptr;
    uintptr_t      next  = 0;
    for (uint8_t index = 0; index < frame.data_views(); ++index) {
        const auto *view = frame.data_view(index);
        if (view->len == 0)
            continue;
        const auto address = reinterpret_cast<uintptr_t>(view->payload_data());
        if (first == nullptr) {
            first = view->payload_data();
            owner = frame.data_view(index);
            next  = address;
        }
        if (address != next || view->len > UINTPTR_MAX - next)
            return nullptr;
        next += view->len;
    }
    return first;
}
} // namespace

uint32_t RdmaTransport::flush_direct_connection(Connection *connection, ConnectionPools &pools, uint32_t budget)
{
    const auto limit = std::min(config_.wire_slot_capacity, pools.peer_receive_slot_capacity);
    uint32_t   sent  = 0;
    auto      &queue = *completion_queues_[pools.cq_index];
    while (sent < budget && connection->is_open() && send_window_available(pools)) {
        if (pools.control_send == nullptr && pools.pending_frame == nullptr &&
            pools.send_queue.drain(&pools.pending_frame, 1) == 0)
            break;
        if (pools.control_send != nullptr || pools.pending_frame->control->len > RDMA_CONTROL_CAPACITY - HEADER_SIZE) {
            if (!flush_control_fragment(connection, pools)) {
                break;
            }
            ++sent;
            continue;
        }
        bool packed = false;
        if (!prepare_packed_send(connection, pools, packed)) {
            break;
        }
        if (packed) {
            ++sent;
            continue;
        }
        auto    *frame = pools.pending_frame;
        uint32_t bytes = 0;
        if (!rdma_direct_frame_size(*frame, limit, bytes)) {
            connection->close();
            break;
        }
        if (!send_window_available(pools, bytes) || !queue.can_prepare(bytes)) {
            break;
        }
        auto *prefix = pools.send->alloc(RDMA_CONTROL_CAPACITY);
        if (prefix == nullptr)
            break;
        write_rdma_direct_control(prefix->data, *frame);
        queue.copied_framing_control_bytes.fetch_add(frame->control->len, std::memory_order_relaxed);
        prefix->len         = RDMA_CONTROL_CAPACITY;
        const auto length   = bytes - RDMA_CONTROL_CAPACITY;
        Buffer    *data     = nullptr;
        bool       borrowed = false;
        if (length != 0) {
            Buffer     *source_owner = nullptr;
            const auto *source       = contiguous_data(*frame, source_owner);
            if (source != nullptr) {
                data = send_pool_->borrow_owned_range(source_owner, source, length);
                if (data == nullptr)
                    data = source_pool_->borrow_owned_range(source_owner, source, length);
                if (data == nullptr)
                    data = pools.recv->borrow_owned_range(source_owner, source, length);
            }
            borrowed = data != nullptr;
            if (data == nullptr) {
                // Staging descriptors must remain usable for every length in
                // this data class after a burst fills the bounded pool.
                data = send_pool_->alloc(config_.data_slot_capacity());
                if (data != nullptr) {
                    uint32_t offset = 0;
                    for (uint8_t index = 0; index < frame->data_views(); ++index) {
                        const auto *view = frame->data_view(index);
                        if (view->len != 0)
                            std::memcpy(data->data + offset, view->payload_data(), view->len);
                        offset += view->len;
                    }
                    data->len = length;
                    queue.copied_payload_bytes.fetch_add(length, std::memory_order_relaxed);
                    queue.copied_staging_payload_bytes.fetch_add(length, std::memory_order_relaxed);
                }
            }
            if (data == nullptr) {
                prefix->release();
                break;
            }
            if (borrowed)
                (void)data->transition(BufferOwnership::CallerOwned, BufferOwnership::Available);
            data->request_id = frame->request_id;
        }
        pools.pending_frame = std::exchange(pools.lookahead_frame, nullptr);
        queue.charge_preparation(bytes);
        pools.queued_frames.fetch_sub(1, std::memory_order_release);
        frame->next_aggregate = nullptr;
        if (!post_direct(connection, pools, data == nullptr ? prefix : data, data == nullptr ? nullptr : prefix, frame))
            break;
        queue.direct_sends.fetch_add(1, std::memory_order_relaxed);
        if (borrowed)
            queue.direct_payload_bytes.fetch_add(length, std::memory_order_relaxed);
        ++sent;
    }
    return sent;
}

bool RdmaTransport::post_direct(Connection *connection, ConnectionPools &pools, Buffer *slot, Buffer *prefix,
                                OutFrame *frame, std::shared_ptr<ControlSendState> control_send)
{
    const bool last =
        control_send == nullptr || control_send->fragment.offset + slot->len == control_send->fragment.total;
    return prepare_send(connection, pools, slot, prefix, frame, last ? 1U : 0U,
                        RDMA_CONTROL_CAPACITY + (prefix == nullptr ? 0 : slot->len), std::move(control_send));
}
} // namespace crowdb::rpc
#endif
