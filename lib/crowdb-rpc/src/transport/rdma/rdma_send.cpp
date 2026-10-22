// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_aggregate.h"
#include "rdma_cq.h"
#include "rdma_direct_frame.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <algorithm>

namespace crowdb::rpc
{
RdmaTransport::ConnectionPools::~ConnectionPools()
{
    release_rdma_frames(pending_frame);
    OutFrame *frame = nullptr;
    while (send_queue.drain(&frame, 1) != 0)
        release_rdma_frames(frame);
}

bool RdmaTransport::submit(Connection *connection, OutFrame *frame)
{
    auto reject = [&] {
        stats_.submit_rejects.fetch_add(1, std::memory_order_relaxed);
        return false;
    };
    Operation operation(repost_state_);
    if (!operation || connection == nullptr || frame == nullptr || !connection->is_open())
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
    if (!posting || !connection->is_open() ||
        !(direct ? rdma_direct_frame_size(*frame, limit, bytes) : rdma_frame_size(*frame, limit, bytes)))
        return reject();
    uint32_t queued = pools->queued_frames.load(std::memory_order_relaxed);
    do {
        if (queued >= config_.send_queue_capacity)
            return reject();
    } while (!pools->queued_frames.compare_exchange_weak(queued, queued + 1, std::memory_order_acq_rel));
    if (!pools->send_queue.try_push(frame)) {
        pools->queued_frames.fetch_sub(1, std::memory_order_release);
        return reject();
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
    uint32_t sent = 0;
    while (sent < budget && connection->is_open() &&
           pools.sends_in_flight < std::min(config_.sq_depth, pools.effective_capacity.max_send_wr)) {
        if (pools.pending_frame == nullptr && pools.send_queue.drain(&pools.pending_frame, 1) == 0)
            break;
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
            if (bytes > byte_limit - slot->len)
                break;
            copy_rdma_frame(slot->data + slot->len, *frame);
            completion_queues_[pools.cq_index]->copied_payload_bytes.fetch_add(
                bytes - HEADER_SIZE - frame->control->len, std::memory_order_relaxed);
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
    Operation    posting(pools.posting_state);
    WorkRequest *work = nullptr;
    if (posting && connection->is_open() && connection->transport_handle != 0 &&
        slot->transition(BufferOwnership::Available, BufferOwnership::SendInFlight))
        work = acquire_work(WorkKind::Send, connection, slot, frames);
    if (work != nullptr) {
        ibv_sge sge{};
        sge.addr   = reinterpret_cast<uintptr_t>(slot->data);
        sge.length = slot->len;
        sge.lkey   = slot->registration_key;
        ibv_send_wr wr{};
        wr.wr_id         = work->token.load(std::memory_order_acquire);
        wr.sg_list       = &sge;
        wr.num_sge       = 1;
        wr.opcode        = IBV_WR_SEND;
        wr.send_flags    = IBV_SEND_SIGNALED;
        ibv_send_wr *bad = nullptr;
        if (ibv_post_send(reinterpret_cast<ibv_qp *>(connection->transport_handle), &wr, &bad) == 0) {
            ++pools.sends_in_flight;
            stats_.sends.fetch_add(1, std::memory_order_relaxed);
            stats_.rpc_frames_sent.fetch_add(frame_count, std::memory_order_relaxed);
            stats_.wire_bytes_sent.fetch_add(slot->len, std::memory_order_relaxed);
            return true;
        }
        release_work(work);
    }
    stats_.send_errors.fetch_add(1, std::memory_order_relaxed);
    connection->close();
    (void)slot->transition(BufferOwnership::SendInFlight, BufferOwnership::Available);
    slot->release();
    release_rdma_frames(frames);
    return false;
}

uint32_t RdmaTransport::flush_sends(CompletionQueue &queue, uint32_t budget)
{
    uint32_t sent = 0;
    for (size_t visited = 0; visited < queue.connections.size() && sent < budget; ++visited) {
        const auto &entry     = queue.connections[queue.next_connection % queue.connections.size()];
        queue.next_connection = (queue.next_connection + 1) % queue.connections.size();
        if (entry.connection->is_open())
            sent += flush_connection(entry.connection.get(), *entry.pools, budget - sent);
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
        release_rdma_frames(std::exchange(pools.pending_frame, nullptr));
        OutFrame *frame = nullptr;
        while (pools.send_queue.drain(&frame, 1) != 0)
            release_rdma_frames(frame);
        pools.queued_frames.store(0, std::memory_order_release);
        return true;
    });
}
} // namespace crowdb::rpc
#endif
