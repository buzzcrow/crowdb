// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "rdma_direct_frame.h"

#ifdef CROWDB_RPC_HAVE_RDMA
namespace crowdb::rpc
{
bool RdmaTransport::post_direct_receive(Connection *connection, Buffer *slot)
{
    Operation operation(repost_state_);
    if (!operation || connection == nullptr || slot == nullptr || slot->data == nullptr ||
        slot->capacity < config_.data_slot_capacity())
        return false;
    auto *pools = pools_for(connection);
    if (pools == nullptr || pools->recv_control == nullptr)
        return false;
    Operation posting(pools->posting_state);
    if (!posting || !connection->is_open() || connection->transport_handle == 0)
        return false;
    auto                                       release = [](Buffer *buffer) { buffer->release(); };
    std::unique_ptr<Buffer, decltype(release)> prefix(pools->recv_control->alloc(RDMA_CONTROL_CAPACITY), release);
    if (prefix == nullptr || !prefix->transition(BufferOwnership::Available, BufferOwnership::PostedReceive))
        return false;
    if (!slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive) &&
        !slot->transition(BufferOwnership::CallerOwned, BufferOwnership::PostedReceive)) {
        (void)prefix->transition(BufferOwnership::PostedReceive, BufferOwnership::Available);
        return false;
    }
    auto rollback = [&] {
        (void)slot->transition(BufferOwnership::PostedReceive, BufferOwnership::Available);
        (void)prefix->transition(BufferOwnership::PostedReceive, BufferOwnership::Available);
    };
    if (slot->ref_clone() == nullptr) {
        rollback();
        return false;
    }
    auto *work = acquire_work(WorkKind::Receive, connection, slot, nullptr, prefix.get());
    if (work == nullptr) {
        rollback();
        slot->release();
        return false;
    }
    std::array<ibv_sge, 2> regions{};
    regions[0] = {reinterpret_cast<uintptr_t>(prefix->data), RDMA_CONTROL_CAPACITY, prefix->registration_key};
    regions[1] = {reinterpret_cast<uintptr_t>(slot->data), config_.data_slot_capacity(), slot->registration_key};
    ibv_recv_wr request{};
    request.wr_id    = work->token.load(std::memory_order_acquire);
    request.sg_list  = regions.data();
    request.num_sge  = 2;
    ibv_recv_wr *bad = nullptr;
    if (ibv_post_recv(reinterpret_cast<ibv_qp *>(connection->transport_handle), &request, &bad) != 0) {
        release_work(work);
        rollback();
        slot->release();
        return false;
    }
    (void)prefix.release();
    stats_.posted_receives.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void RdmaTransport::release_completed_direct_receive(WorkRequest *work, const ibv_wc &completion)
{
    auto      *slot   = work->slot;
    auto      *prefix = work->prefix;
    Header     header;
    const bool valid = completion.status == IBV_WC_SUCCESS && work->conn->is_open() &&
                       slot->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight) &&
                       prefix->transition(BufferOwnership::PostedReceive, BufferOwnership::Available) &&
                       parse_rdma_direct_control(std::span(prefix->data, RDMA_CONTROL_CAPACITY),
                                                 config_.data_slot_capacity(), completion.byte_len, header) &&
                       slot->transition(BufferOwnership::ReceiveInFlight, BufferOwnership::CallerOwned);
    if (!valid) {
        if (completion.status != IBV_WC_SUCCESS)
            record_completion_error(completion, !work->conn->is_open());
        else if (work->conn->is_open())
            stats_.protocol_errors.fetch_add(1, std::memory_order_relaxed);
        auto connection = work->conn;
        (void)slot->quarantine();
        (void)prefix->quarantine();
        release_work(work);
        connection->close();
        slot->release();
        prefix->release();
        return;
    }
    auto connection        = std::move(work->conn);
    auto pools             = std::move(work->pools);
    slot->last_release_cb  = &RdmaTransport::repost_frame_release;
    slot->last_release_ctx = new RepostContext{repost_state_, connection, std::move(pools)};
    auto *frame            = new Frame;
    frame->header          = header;
    frame->control.assign(prefix->data + HEADER_SIZE, prefix->data + RDMA_CONTROL_CAPACITY);
    extract_control_fields(frame->control.data(), header.msg_size, frame->request_id, frame->rpc_create_nano,
                           frame->response_create_nano);
    frame->data_buf = slot->slice(0, header.data_size);
    release_work(work);
    prefix->release();
    stats_.receives.fetch_add(1, std::memory_order_relaxed);
    stats_.rpc_frames_received.fetch_add(1, std::memory_order_relaxed);
    stats_.wire_bytes_received.fetch_add(completion.byte_len, std::memory_order_relaxed);
    if (connection->is_open())
        connection->on_frame(frame);
    else
        delete frame;
    slot->release();
}
} // namespace crowdb::rpc
#endif
