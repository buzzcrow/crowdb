// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "rdma_control_stream.h"
#include "rdma_cq.h"
#include "rdma_packed_frame.h"

#ifdef CROWDB_RPC_HAVE_RDMA
namespace crowdb::rpc
{
bool RdmaTransport::release_completed_packed_receive(WorkRequest *work, const ibv_wc &wc)
{
    auto          *slot   = work->slot;
    auto          *prefix = work->prefix;
    RdmaAggregate  aggregate;
    const uint32_t limit = config_.aggregation_enabled ? config_.aggregation_max_frames : 1;
    bool  valid = wc.byte_len >= RDMA_CONTROL_CAPACITY &&
                  wc.byte_len - RDMA_CONTROL_CAPACITY <= work->pools->effective_data_capacity &&
                  (work->pools->control_receive == nullptr || !work->pools->control_receive->active()) &&
                  parse_rdma_packed_frame(std::span(prefix->data, RDMA_CONTROL_CAPACITY),
                                          std::span(slot->data, wc.byte_len - RDMA_CONTROL_CAPACITY), limit, aggregate);
    auto &queue = *completion_queues_[work->pools->cq_index];
    if (valid && !queue.can_receive(wc.byte_len, aggregate.count)) {
        return false;
    }
    valid = valid && slot->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight) &&
            prefix->transition(BufferOwnership::PostedReceive, BufferOwnership::Available) &&
            slot->transition(BufferOwnership::ReceiveInFlight, BufferOwnership::CallerOwned);
    if (!valid) {
        auto connection = work->conn;
        stats_.protocol_errors.fetch_add(1, std::memory_order_relaxed);
        (void)slot->quarantine();
        (void)prefix->quarantine();
        release_work(work);
        connection->close();
        slot->release();
        prefix->release();
        return true;
    }
    queue.charge_receive(wc.byte_len, aggregate.count);
    auto connection        = std::move(work->conn);
    auto pools             = std::move(work->pools);
    slot->last_release_cb  = &RdmaTransport::repost_frame_release;
    slot->last_release_ctx = new RepostContext{repost_state_, connection, std::move(pools)};
    release_work(work);
    prefix->release();
    stats_.receives.fetch_add(1, std::memory_order_relaxed);
    stats_.rpc_frames_received.fetch_add(aggregate.count, std::memory_order_relaxed);
    stats_.wire_bytes_received.fetch_add(wc.byte_len, std::memory_order_relaxed);
    for (uint32_t index = 0; index < aggregate.count; ++index) {
        const auto &view    = aggregate.frames[index];
        auto       *frame   = new Frame;
        frame->header       = view.header;
        const auto *control = slot->data + view.offset + HEADER_SIZE;
        frame->control.assign(control, control + view.header.msg_size);
        extract_control_fields(control, view.header.msg_size, frame->request_id, frame->rpc_create_nano,
                               frame->response_create_nano);
        frame->data_buf = slot->slice(view.data_offset, view.header.data_size);
        if (connection->is_open()) {
            connection->on_frame(frame);
        }
        else {
            delete frame;
        }
    }
    slot->release();
    return true;
}
} // namespace crowdb::rpc
#endif
