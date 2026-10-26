// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_aggregate.h"
#include "rdma_control_send.h"
#include "rdma_cq.h"
#include "rdma_direct_frame.h"
#include "rdma_packed_frame.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <algorithm>

namespace crowdb::rpc
{
RdmaTransport::ConnectionPools::~ConnectionPools()
{
    release_rdma_frames(pending_frame);
    release_rdma_frames(lookahead_frame);
    OutFrame *frame = nullptr;
    while (send_queue.drain(&frame, 1) != 0)
        release_rdma_frames(frame);
    Buffer *slot = nullptr;
    while (receive_reposts.drain(&slot, 1) != 0) {
        slot->release();
    }
}

bool RdmaTransport::submit(Connection *connection, OutFrame *frame)
{
    auto reject = [&] {
        stats_.submit_rejects.fetch_add(1, std::memory_order_relaxed);
        return false;
    };
    Operation operation(repost_state_);
    if (!operation || connection == nullptr || frame == nullptr || !connection->is_open() ||
        frame->header.msg_type == RPC_CONTROL_FRAGMENT_MSG_TYPE || frame->header.msg_type == RDMA_PACKED_MSG_TYPE)
        return reject();
    auto *pools = pools_for(connection);
    if (pools == nullptr || pools->send == nullptr || pools->cq_index >= completion_queues_.size())
        return reject();
    Operation  posting(pools->posting_state);
    uint32_t   bytes  = 0;
    const bool direct = config_.wire_layout == RdmaWireLayout::ControlData;
    const auto limit =
        direct
            ? std::min(config_.wire_slot_capacity, pools->peer_receive_slot_capacity)
            : std::min({config_.wire_slot_capacity, pools->peer_receive_slot_capacity, config_.aggregation_max_bytes});
    if (!posting || !connection->is_open())
        return reject();
    const bool stream =
        direct && frame->control != nullptr && frame->control->len > RDMA_CONTROL_CAPACITY - HEADER_SIZE;
    Buffer *original_control = frame->control;
    if (stream) {
        auto *charged = admit_control_frame(*frame, *pools);
        if (charged == nullptr) {
            return reject();
        }
        frame->control = charged;
        // The charged slice now owns the original reference before the CQ can
        // consume it. Rejected publication restores the caller's ownership.
        original_control->release();
    }
    else if (!(direct ? rdma_direct_frame_size(*frame, limit, bytes) : rdma_frame_size(*frame, limit, bytes))) {
        return reject();
    }
    if (!stream && config_.send_window_bytes != 0 && bytes > config_.send_window_bytes) {
        return reject();
    }
    auto restore = [&] {
        if (stream) {
            auto *charged  = frame->control;
            frame->control = original_control->ref_clone();
            charged->release();
        }
        return reject();
    };
    uint32_t queued = pools->queued_frames.load(std::memory_order_relaxed);
    do {
        if (queued >= config_.send_queue_capacity)
            return restore();
    } while (!pools->queued_frames.compare_exchange_weak(queued, queued + 1, std::memory_order_acq_rel));
    if (!pools->send_queue.try_push(frame)) {
        pools->queued_frames.fetch_sub(1, std::memory_order_release);
        return restore();
    }
    uint64_t peak = stats_.send_queue_high_water.load(std::memory_order_relaxed);
    while (peak < queued + 1 &&
           !stats_.send_queue_high_water.compare_exchange_weak(peak, queued + 1, std::memory_order_relaxed)) {
    }
    notify_queue(pools->cq_index);
    return true;
}

uint32_t RdmaTransport::flush_connection(Connection *connection, ConnectionPools &pools, uint32_t budget)
{
    if (config_.wire_layout == RdmaWireLayout::ControlData)
        return flush_direct_connection(connection, pools, budget);
    const auto byte_limit =
        std::min({config_.wire_slot_capacity, pools.peer_receive_slot_capacity, config_.aggregation_max_bytes});
    const auto frame_limit =
        config_.aggregation_enabled ? std::min(config_.aggregation_max_frames, pools.peer_receive_frame_limit) : 1U;
    auto    &queue = *completion_queues_[pools.cq_index];
    uint32_t sent  = 0;
    while (sent < budget && connection->is_open() && send_window_available(pools)) {
        if (pools.pending_frame == nullptr && pools.send_queue.drain(&pools.pending_frame, 1) == 0)
            break;
        uint32_t next_bytes = 0;
        if (!rdma_frame_size(*pools.pending_frame, byte_limit, next_bytes) ||
            !send_window_available(pools, next_bytes) || !queue.can_prepare(next_bytes)) {
            break;
        }
        auto *slot = pools.send->alloc(config_.wire_slot_capacity);
        if (slot == nullptr)
            break;
        slot->len        = 0;
        OutFrame  *first = nullptr;
        OutFrame **tail  = &first;
        uint32_t   count = 0;
        while (pools.pending_frame != nullptr && count < frame_limit) {
            auto    *frame = pools.pending_frame;
            uint32_t bytes = 0;
            // submit validated the immutable frame against the same bound.
            if (!rdma_frame_size(*frame, byte_limit, bytes)) {
                connection->close();
                break;
            }
            if (bytes > byte_limit - slot->len || !send_window_available(pools, slot->len + bytes) ||
                !queue.can_prepare(bytes))
                break;
            copy_rdma_frame(slot->data + slot->len, *frame);
            queue.charge_preparation(bytes);
            completion_queues_[pools.cq_index]->copied_payload_bytes.fetch_add(
                bytes - HEADER_SIZE - frame->control->len, std::memory_order_relaxed);
            completion_queues_[pools.cq_index]->copied_aggregate_payload_bytes.fetch_add(
                bytes - HEADER_SIZE - frame->control->len, std::memory_order_relaxed);
            completion_queues_[pools.cq_index]->copied_framing_control_bytes.fetch_add(frame->control->len,
                                                                                       std::memory_order_relaxed);
            slot->len += bytes;
            frame->next_aggregate = nullptr;
            *tail                 = frame;
            tail                  = &frame->next_aggregate;
            pools.pending_frame   = nullptr;
            pools.queued_frames.fetch_sub(1, std::memory_order_release);
            ++count;
            if (count < frame_limit)
                (void)pools.send_queue.drain(&pools.pending_frame, 1);
        }
        if (count == 0 || !connection->is_open()) {
            release_rdma_frames(first);
            slot->release();
            break;
        }
        if (!post_aggregate(connection, pools, slot, first, count))
            break;
        ++sent;
    }
    return sent;
}

bool RdmaTransport::post_aggregate(Connection *connection, ConnectionPools &pools, Buffer *slot, OutFrame *frames,
                                   uint32_t frame_count)
{
    return prepare_send(connection, pools, slot, nullptr, frames, frame_count, slot->len);
}

uint32_t RdmaTransport::flush_sends(CompletionQueue &queue, uint32_t budget)
{
    queue.prepare_bytes_left    = queue.prepare_byte_limit;
    queue.prepare_frames_left   = CompletionQueue::PREPARE_FRAMES_PER_ROUND;
    queue.preparation_exhausted = false;
    budget                      = std::min(budget, CompletionQueue::SENDS_PER_ROUND);
    uint32_t sent               = 0;
    for (size_t visited = 0; visited < queue.connections.size() && sent < budget && !queue.preparation_exhausted;
         ++visited) {
        const auto &entry     = queue.connections[queue.next_connection % queue.connections.size()];
        queue.next_connection = (queue.next_connection + 1) % queue.connections.size();
        if (entry.connection->is_open()) {
            sent += flush_connection(entry.connection.get(), *entry.pools, budget - sent);
            (void)post_prepared(entry.connection.get(), *entry.pools);
        }
    }
    // Closed producers finish publishing before their bounded queue retires.
    for (size_t index = 0; index < queue.connections.size();) {
        auto &entry = queue.connections[index];
        if (!entry.connection->is_open())
            request_disconnect(*entry.pools);
        if (!entry.connection->is_open() &&
            (entry.pools->posting_state->users.fetch_or(RepostState::STOPPED, std::memory_order_acq_rel) &
             ~RepostState::STOPPED) == 0) {
            retire_sends(queue, entry.connection.get());
        }
        else {
            ++index;
        }
    }
    return sent;
}

void RdmaTransport::retire_sends(CompletionQueue &queue, Connection *connection)
{
    std::erase_if(queue.connections, [&](const auto &entry) {
        if (connection != nullptr && entry.connection.get() != connection)
            return false;
        auto &pools = *entry.pools;
        pools.control_send.reset();
        if (pools.control_receive != nullptr) {
            pools.control_receive->reset();
        }
        release_rdma_frames(std::exchange(pools.pending_frame, nullptr));
        release_rdma_frames(std::exchange(pools.lookahead_frame, nullptr));
        OutFrame *frame = nullptr;
        while (pools.send_queue.drain(&frame, 1) != 0)
            release_rdma_frames(frame);
        pools.queued_frames.store(0, std::memory_order_release);
        return true;
    });
}
} // namespace crowdb::rpc
#endif
