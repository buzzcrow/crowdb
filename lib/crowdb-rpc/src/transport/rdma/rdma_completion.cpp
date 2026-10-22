// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "crowdb-rpc/transport/socket_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <unistd.h>

#    include <algorithm>

namespace crowdb::rpc
{

int RdmaTransport::drain_cq(uint32_t max_completions)
{
    if (cq_ == nullptr || cq_polling_.test_and_set(std::memory_order_acquire))
        return 0;
    // Only the CQ consumer may clear its continuation. A competing worker
    // must leave the wakeup readable when it cannot acquire the CQ.
    if (continuation_fd_ >= 0) {
        uint64_t value = 0;
        (void)::read(continuation_fd_, &value, sizeof(value));
    }
    if (max_completions == 0)
        max_completions = std::max(1U, config_.poll_batch);
    stats_.cq_polls.fetch_add(1, std::memory_order_relaxed);
    uint32_t               drained          = 0;
    uint32_t               empty_polls      = 0;
    const uint32_t         kEmptyPollBudget = config_.poll_empty_budget;
    std::array<ibv_wc, 32> completions{};
    auto                   poll_one = [&](ibv_cq *cq) {
        while (drained < max_completions && cq != nullptr) {
            const int batch = ibv_poll_cq(cq, static_cast<int>(std::min<uint32_t>(32, max_completions - drained)),
                                          completions.data());
            if (batch < 0)
                break;
            if (batch == 0) {
                stats_.cq_empty_polls.fetch_add(1, std::memory_order_relaxed);
                if (++empty_polls >= kEmptyPollBudget)
                    break;
                continue;
            }
            empty_polls = 0;
            for (int i = 0; i < batch; ++i) {
                dispatch_completion(completions[static_cast<size_t>(i)]);
            }
            drained += static_cast<uint32_t>(batch);
        }
    };
    if (comp_channel_ != nullptr) {
        ibv_cq *event_cq = nullptr;
        void   *context  = nullptr;
        while (ibv_get_cq_event(comp_channel_, &event_cq, &context) == 0) {
            ibv_ack_cq_events(event_cq, 1);
        }
    }
    poll_one(cq_);
    (void)ibv_req_notify_cq(cq_, 0);
    // Completions arriving between the empty poll and notification arming
    // need one final poll. A full batch gets an explicit epoll continuation.
    empty_polls = 0;
    poll_one(cq_);
    if (drained == max_completions && continuation_fd_ >= 0) {
        const uint64_t value = 1;
        (void)::write(continuation_fd_, &value, sizeof(value));
    }
    cq_polling_.clear(std::memory_order_release);
    return static_cast<int>(drained);
}

void RdmaTransport::register_events(SocketEngine *engine)
{
    if (engine == nullptr)
        return;
    // A shared CQ has one completion channel. Register that channel with one
    // epoll engine only; registering the same fd in every engine causes
    // duplicate wakeups and lets different workers race the CQ event.
    if (event_engine_ != nullptr && event_engine_ != engine)
        return;
    event_engine_ = engine;
    if (completion_fd() >= 0)
        engine->add_external_fd(completion_fd(), 1);
    if (cm_event_fd() >= 0)
        engine->add_external_fd(cm_event_fd(), 2);
    if (continuation_fd_ >= 0)
        engine->add_external_fd(continuation_fd_, 3);
}

int RdmaTransport::handle_external_event(uint64_t tag, uint32_t max_completions)
{
    if (tag == 1 || tag == 3) {
        return drain_cq(max_completions);
    }
    if (tag == 2) {
        // The shared CM channel has one consumer while passive setup waits
        // for ESTABLISHED. Another worker must not consume that event.
        if (cm_polling_.test_and_set(std::memory_order_acquire))
            return 0;
        auto release = [](std::atomic_flag *flag) { flag->clear(std::memory_order_release); };
        const std::unique_ptr<std::atomic_flag, decltype(release)> owner(&cm_polling_, release);
        int                                                        accepted = 0;
        while (true) {
            auto connection = accept();
            if (connection == nullptr)
                break;
            ++accepted;
            if (connection_handler_)
                connection_handler_(connection);
        }
        return accepted;
    }
    return 0;
}

bool RdmaTransport::prime_receive(const std::shared_ptr<Connection> &connection)
{
    if (connection == nullptr || connection->pool() == nullptr) {

        return false;
    }
    const uint32_t depth = config_.posted_receive_target;
    if (depth == 0 || depth > config_.rq_depth ||
        static_cast<uint64_t>(depth) * config_.wire_slot_capacity > config_.receive_arena_bytes)
        return false;
    BufferPool *pool = connection->pool();
    for (uint32_t i = 0; i < depth; ++i) {
        Buffer *slot = pool->alloc(config_.wire_slot_capacity);
        if (slot == nullptr) {
            return false;
        }
        if (!post_receive(connection.get(), slot)) {
            slot->release();
            return false;
        }
        // post_receive retains the work-request reference; the completion
        // owns the slot after the caller reference is released.
        slot->release();
    }
    return true;
}

void RdmaTransport::record_completion_error(const ibv_wc &wc)
{
    stats_.cq_errors.fetch_add(1, std::memory_order_relaxed);
    switch (wc.status) {
    case IBV_WC_WR_FLUSH_ERR:
        stats_.flush_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    case IBV_WC_LOC_PROT_ERR:
        stats_.local_protection_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    case IBV_WC_RETRY_EXC_ERR:
        stats_.retry_exceeded_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    case IBV_WC_REM_ACCESS_ERR:
        stats_.remote_access_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    default:
        break;
    }
}

void RdmaTransport::release_completed_receive(WorkRequest *work, const ibv_wc &wc)
{

    auto quarantine_and_release = [&] {
        record_completion_error(wc);
        work->conn->close();
        (void)work->slot->quarantine();
        work->slot->release();
        release_work(work);
    };
    if (wc.status != IBV_WC_SUCCESS) {
        CRB_LOG_WARN("rdma receive: completion failed status={} vendor_err={} opcode={}",
                     static_cast<unsigned>(wc.status), static_cast<unsigned>(wc.vendor_err),
                     static_cast<unsigned>(wc.opcode));
        quarantine_and_release();
        return;
    }
    if (!work->slot->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight)) {
        quarantine_and_release();
        return;
    }
    const uint32_t slot_capacity = work->slot->capacity;
    if (slot_capacity < HEADER_SIZE || wc.byte_len < HEADER_SIZE || !work->conn->is_open()) {
        CRB_LOG_WARN("rdma receive: slot capacity too small capacity={} header={}", slot_capacity, HEADER_SIZE);
        quarantine_and_release();
        return;
    }
    const Header   header         = parse_header(work->slot->data);
    const uint64_t data_offset    = static_cast<uint64_t>(header.msg_offset) + header.msg_size;
    const uint64_t expected_bytes = data_offset + header.data_size;
    if (header.magic != MAGIC || header.msg_offset != HEADER_SIZE || data_offset > RDMA_CONTROL_CAPACITY ||
        expected_bytes != wc.byte_len || expected_bytes > slot_capacity) {
        CRB_LOG_WARN(
            "rdma receive: invalid frame magic={} type={} msg_size={} data_size={} offset={} capacity={} byte_len={}",
            header.magic, header.msg_type, header.msg_size, header.data_size, header.msg_offset, slot_capacity,
            wc.byte_len);
        quarantine_and_release();
        return;
    }

    auto *frame   = new Frame;
    frame->header = header;
    if (header.msg_size != 0) {
        frame->control.assign(work->slot->data + header.msg_offset,
                              work->slot->data + header.msg_offset + header.msg_size);
        extract_control_fields(work->slot->data + header.msg_offset, header.msg_size, frame->request_id,
                               frame->rpc_create_nano, frame->response_create_nano);
    }
    work->request_id        = frame->request_id;
    work->stream_id         = work->slot->stream_id;
    work->sequence          = work->slot->sequence;
    work->logical_offset    = work->slot->logical_offset;
    work->slot->view_offset = static_cast<uint32_t>(data_offset);
    work->slot->capacity    = slot_capacity - static_cast<uint32_t>(data_offset);
    work->slot->len         = header.data_size;
    if (!work->slot->transition(BufferOwnership::ReceiveInFlight, BufferOwnership::CallerOwned)) {
        quarantine_and_release();
        delete frame;
        return;
    }
    auto  pools                  = work->conn->user_data == nullptr
                                     ? nullptr
                                     : static_cast<ConnectionPools *>(work->conn->user_data)->shared_from_this();
    auto *repost                 = new RepostContext{repost_state_, work->conn, std::move(pools)};
    frame->data_buf              = work->slot;
    work->slot->last_release_cb  = &RdmaTransport::repost_frame_release;
    work->slot->last_release_ctx = repost;
    work->conn->on_frame(frame);
    stats_.receives.fetch_add(1, std::memory_order_relaxed);
    release_work(work);
}

void RdmaTransport::repost_frame_release(void *context, Buffer *slot)
{
    auto repost = std::unique_ptr<RepostContext>(static_cast<RepostContext *>(context));
    slot->reset_view();
    if (!slot->transition(BufferOwnership::CallerOwned, BufferOwnership::Available))
        (void)slot->quarantine();
    {
        Operation operation(repost->state);
        if (operation && slot->ownership.load(std::memory_order_acquire) == BufferOwnership::Available)
            (void)repost->state->transport->post_receive(repost->connection.get(), slot);
    }
    slot->release();
}

void RdmaTransport::release_completed_frame(WorkRequest *work, const ibv_wc &wc)
{
    OutFrame *frame = work->frame;

    if (wc.status != IBV_WC_SUCCESS) {
        record_completion_error(wc);
        // A failed SEND makes the stream uncertain; do not allow a TCP
        // suffix to be appended to this RDMA connection.
        if (work->slot != nullptr)
            (void)work->slot->quarantine();
        for (uint8_t i = 0; i < frame->data_views(); ++i)
            (void)frame->data_view(i)->quarantine();
        if (work->conn != nullptr)
            work->conn->close();
    }
    else if (work->slot != nullptr &&
             !work->slot->transition(BufferOwnership::SendInFlight, BufferOwnership::Available)) {
        (void)work->slot->quarantine();
        if (work->conn != nullptr)
            work->conn->close();
    }
    if (work->slot != nullptr)
        work->slot->release();
    if (frame->control != nullptr)
        frame->control->release();
    frame->release_data();
    delete frame;
    release_work(work);
}

} // namespace crowdb::rpc

#endif
