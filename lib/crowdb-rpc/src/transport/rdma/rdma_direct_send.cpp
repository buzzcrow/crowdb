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
const uint8_t *contiguous_data(const OutFrame &frame)
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
    while (sent < budget && connection->is_open() &&
           pools.sends_in_flight < std::min(config_.sq_depth, pools.effective_capacity.max_send_wr)) {
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
        auto    *frame = pools.pending_frame;
        uint32_t bytes = 0;
        if (!rdma_direct_frame_size(*frame, limit, bytes)) {
            connection->close();
            break;
        }
        auto *prefix = pools.send->alloc(RDMA_CONTROL_CAPACITY);
        if (prefix == nullptr)
            break;
        write_rdma_direct_control(prefix->data, *frame);
        prefix->len         = RDMA_CONTROL_CAPACITY;
        const auto length   = bytes - RDMA_CONTROL_CAPACITY;
        Buffer    *data     = nullptr;
        bool       borrowed = false;
        if (length != 0) {
            const auto *source = contiguous_data(*frame);
            if (source != nullptr) {
                data = send_pool_->borrow_range(source, length);
                if (data == nullptr)
                    data = pools.recv->borrow_range(source, length);
            }
            borrowed = data != nullptr;
            if (data == nullptr) {
                data = send_pool_->alloc(length);
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
        pools.pending_frame = nullptr;
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
    Operation    posting(pools.posting_state);
    WorkRequest *work = nullptr;
    if (posting && connection->is_open() && connection->transport_handle != 0 &&
        slot->transition(BufferOwnership::Available, BufferOwnership::SendInFlight) &&
        (prefix == nullptr || prefix->transition(BufferOwnership::Available, BufferOwnership::SendInFlight)))
        work = acquire_work(WorkKind::Send, connection, slot, frame, prefix);
    if (work != nullptr) {
        work->control_send             = std::move(control_send);
        auto                  *control = prefix == nullptr ? slot : prefix;
        std::array<ibv_sge, 2> regions{};
        regions[0] = {reinterpret_cast<uintptr_t>(control->data), RDMA_CONTROL_CAPACITY, control->registration_key};
        if (prefix != nullptr)
            regions[1] = {reinterpret_cast<uintptr_t>(slot->payload_data()), slot->len, slot->registration_key};
        ibv_send_wr request{};
        request.wr_id      = work->token.load(std::memory_order_acquire);
        request.sg_list    = regions.data();
        request.num_sge    = prefix == nullptr ? 1 : 2;
        request.opcode     = IBV_WR_SEND;
        request.send_flags = IBV_SEND_SIGNALED;
        ibv_send_wr *bad   = nullptr;
        if (ibv_post_send(reinterpret_cast<ibv_qp *>(connection->transport_handle), &request, &bad) == 0) {
            ++pools.sends_in_flight;
            stats_.sends.fetch_add(1, std::memory_order_relaxed);
            if (work->control_send == nullptr ||
                work->control_send->fragment.offset + slot->len == work->control_send->fragment.total) {
                stats_.rpc_frames_sent.fetch_add(1, std::memory_order_relaxed);
            }
            stats_.wire_bytes_sent.fetch_add(RDMA_CONTROL_CAPACITY + (prefix == nullptr ? 0 : slot->len),
                                             std::memory_order_relaxed);
            return true;
        }
        release_work(work);
    }
    stats_.send_errors.fetch_add(1, std::memory_order_relaxed);
    connection->close();
    (void)slot->transition(BufferOwnership::SendInFlight, BufferOwnership::Available);
    slot->release();
    if (prefix != nullptr) {
        (void)prefix->transition(BufferOwnership::SendInFlight, BufferOwnership::Available);
        prefix->release();
    }
    release_rdma_frames(frame);
    return false;
}
} // namespace crowdb::rpc
#endif
