// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_control_send.h"

#include "rdma_control_codec.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include "rdma_aggregate.h"

#    include <algorithm>
#    include <cstring>
#    include <utility>

namespace crowdb::rpc
{
RdmaTransport::ControlSendState::~ControlSendState()
{
    release_rdma_frames(frame);
}

Buffer *RdmaTransport::admit_control_frame(const OutFrame &frame, const ConnectionPools &pools)
{
    const auto *control = frame.control;
    if (control == nullptr || control->data == nullptr || control->view_offset > control->capacity ||
        control->len > control->capacity - control->view_offset || frame.data_views() != 0 ||
        frame.header.magic != MAGIC || frame.header.msg_offset != HEADER_SIZE || frame.request_id == 0 ||
        (frame.header.flags & ~FLAG_ONE_WAY) != 0 || frame.header.msg_type == RDMA_HANDSHAKE_MSG_TYPE ||
        frame.header.msg_type == RPC_CONTROL_FRAGMENT_MSG_TYPE ||
        control->len > std::min(config_.max_control_message_bytes, pools.peer_max_control_message_bytes)) {
        return nullptr;
    }
    uint64_t id = 0, created = 0, response = 0;
    extract_control_fields(control->payload_data(), control->len, id, created, response);
    if (id != frame.request_id) {
        return nullptr;
    }
    auto admission = control_send_budget_->try_reserve(control->len);
    if (admission == nullptr) {
        return nullptr;
    }
    auto *view = frame.control->slice(0, control->len);
    if (view != nullptr) {
        view->pool_owner = std::move(admission);
    }
    return view;
}

bool RdmaTransport::flush_control_fragment(Connection *connection, ConnectionPools &pools)
{
    if (pools.control_send == nullptr) {
        if (pools.next_control_stream_id == 0) {
            connection->close();
            return false;
        }
        auto state   = std::make_shared<ControlSendState>();
        state->frame = std::exchange(pools.pending_frame, nullptr);
        auto &f      = state->fragment;
        f.request_id = state->frame->request_id;
        extract_control_fields(state->frame->control->data, state->frame->control->len, f.request_id, f.rpc_create_nano,
                               f.response_create_nano);
        f.stream_id        = pools.next_control_stream_id++;
        f.generation       = local_capability_.generation;
        f.total            = state->frame->control->len;
        f.capacity         = pools.effective_data_capacity;
        f.message_type     = state->frame->header.msg_type;
        f.message_flags    = state->frame->header.flags;
        pools.control_send = std::move(state);
    }
    auto       state   = pools.control_send;
    auto      &f       = state->fragment;
    const auto length  = static_cast<uint32_t>(std::min<uint64_t>(f.capacity, f.total - f.offset));
    auto       release = [](Buffer *buffer) { buffer->release(); };
    std::unique_ptr<Buffer, decltype(release)> prefix(pools.send->alloc(RDMA_CONTROL_CAPACITY), release);
    std::unique_ptr<Buffer, decltype(release)> data(send_pool_->alloc(length), release);
    if (prefix == nullptr || data == nullptr) {
        return false;
    }
    f.stream_flags = static_cast<uint8_t>((f.offset == 0 ? CONTROL_STREAM_START : 0) |
                                          (f.offset + length == f.total ? CONTROL_STREAM_END : 0));
    if (!write_control_fragment(std::span(prefix->data, RDMA_CONTROL_CAPACITY), f, length)) {
        connection->close();
        return false;
    }
    prefix->len = RDMA_CONTROL_CAPACITY;
    std::memcpy(data->data, state->frame->control->data + f.offset, length);
    data->len = length;
    data->set_metadata(f.request_id, f.stream_id, f.sequence, f.offset, data->registration_id);
    auto *data_slot    = data.release();
    auto *control_slot = prefix.release();
    if (!post_direct(connection, pools, data_slot, control_slot, nullptr, state)) {
        return false;
    }
    stats_.control_fragments_sent.fetch_add(1, std::memory_order_relaxed);
    stats_.copied_control_send_bytes.fetch_add(length, std::memory_order_relaxed);
    f.offset += length;
    ++f.sequence;
    if (f.offset == f.total) {
        pools.control_send.reset();
        pools.queued_frames.fetch_sub(1, std::memory_order_release);
    }
    return true;
}
} // namespace crowdb::rpc
#endif
