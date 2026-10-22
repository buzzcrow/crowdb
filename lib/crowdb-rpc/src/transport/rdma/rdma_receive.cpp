// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "rdma_aggregate.h"

#ifdef CROWDB_RPC_HAVE_RDMA

namespace crowdb::rpc
{
bool RdmaTransport::release_completed_receive(WorkRequest *work, const ibv_wc &wc)
{
    if (work->prefix != nullptr) {
        return release_completed_direct_receive(work, wc);
    }
    auto fail = [&] {
        if (wc.status != IBV_WC_SUCCESS)
            record_completion_error(wc, !work->conn->is_open());
        else if (work->conn->is_open())
            stats_.protocol_errors.fetch_add(1, std::memory_order_relaxed);
        auto  connection = work->conn;
        auto *slot       = work->slot;
        (void)slot->quarantine();
        release_work(work);
        connection->close();
        slot->release();
    };
    if (wc.status != IBV_WC_SUCCESS || !work->conn->is_open() ||
        !work->slot->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight)) {
        fail();
        return true;
    }
    RdmaAggregate aggregate;
    if (wc.byte_len > work->slot->capacity ||
        !parse_rdma_aggregate(std::span(work->slot->data, wc.byte_len), aggregate) ||
        !work->slot->transition(BufferOwnership::ReceiveInFlight, BufferOwnership::CallerOwned)) {
        fail();
        return true;
    }

    auto  connection       = std::move(work->conn);
    auto  pools            = std::move(work->pools);
    auto *slot             = work->slot;
    slot->last_release_cb  = &RdmaTransport::repost_frame_release;
    slot->last_release_ctx = new RepostContext{repost_state_, connection, std::move(pools)};
    release_work(work);
    stats_.receives.fetch_add(1, std::memory_order_relaxed);
    stats_.rpc_frames_received.fetch_add(aggregate.count, std::memory_order_relaxed);
    stats_.wire_bytes_received.fetch_add(wc.byte_len, std::memory_order_relaxed);
    // The parent reference spans all dispatches. A callback may release its
    // own view immediately; another frame may retain its view past teardown.
    for (uint32_t index = 0; index < aggregate.count; ++index) {
        const auto &view  = aggregate.frames[index];
        auto       *frame = new Frame;
        frame->header     = view.header;
        if (view.header.msg_size != 0) {
            const auto *control = slot->data + view.offset + HEADER_SIZE;
            frame->control.assign(control, control + view.header.msg_size);
            extract_control_fields(control, view.header.msg_size, frame->request_id, frame->rpc_create_nano,
                                   frame->response_create_nano);
        }
        frame->data_buf = slot->slice(view.data_offset, view.header.data_size);
        if (connection->is_open())
            connection->on_frame(frame);
        else
            delete frame;
    }
    slot->release();
    return true;
}

void RdmaTransport::repost_frame_release(void *context, Buffer *slot)
{
    auto repost = std::unique_ptr<RepostContext>(static_cast<RepostContext *>(context));
    slot->reset_view();
    if (!slot->transition(BufferOwnership::CallerOwned, BufferOwnership::Available))
        (void)slot->quarantine();
    {
        Operation operation(repost->state);
        if (operation && repost->connection->is_open() &&
            slot->ownership.load(std::memory_order_acquire) == BufferOwnership::Available &&
            !repost->state->transport->post_receive(repost->connection.get(), slot)) {
            repost->state->transport->stats_.receive_post_errors.fetch_add(1, std::memory_order_relaxed);
            repost->connection->close();
        }
    }
    slot->release();
}
} // namespace crowdb::rpc
#endif
