// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_aggregate.h"
#include "rdma_cq.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <algorithm>
#    include <cerrno>

namespace crowdb::rpc
{
bool RdmaTransport::send_window_available(const ConnectionPools &pools, uint32_t bytes) const
{
    const auto requested = config_.send_window_size == 0 ? config_.sq_depth : config_.send_window_size;
    const auto limit     = std::min(requested, pools.effective_capacity.max_send_wr);
    if (pools.sends_in_flight + pools.prepared_sends.size() >= limit) {
        return false;
    }
    const uint64_t used = pools.send_bytes_in_flight + pools.prepared_send_bytes;
    return config_.send_window_bytes == 0 ||
           (used <= config_.send_window_bytes && bytes <= config_.send_window_bytes - used);
}

bool RdmaTransport::prepare_send(Connection *connection, ConnectionPools &pools, Buffer *slot, Buffer *prefix,
                                 OutFrame *frames, uint32_t frame_count, uint32_t wire_bytes,
                                 std::shared_ptr<ControlSendState> control_send)
{
    Operation    posting(pools.posting_state);
    WorkRequest *work = nullptr;
    if (posting && connection->is_open() && connection->transport_handle != 0 &&
        slot->transition(BufferOwnership::Available, BufferOwnership::SendInFlight) &&
        (prefix == nullptr || prefix->transition(BufferOwnership::Available, BufferOwnership::SendInFlight))) {
        work = acquire_work(WorkKind::Send, connection, slot, frames, prefix);
    }
    if (work == nullptr) {
        stats_.send_errors.fetch_add(1, std::memory_order_relaxed);
        connection->close();
        (void)slot->transition(BufferOwnership::SendInFlight, BufferOwnership::Available);
        slot->release();
        if (prefix != nullptr) {
            (void)prefix->transition(BufferOwnership::SendInFlight, BufferOwnership::Available);
            prefix->release();
        }
        release_rdma_frames(frames);
        return false;
    }
    work->control_send     = std::move(control_send);
    work->send_frame_count = frame_count;
    work->send_wire_bytes  = wire_bytes;
    auto *control          = prefix == nullptr ? slot : prefix;
    work->regions[0]       = {.addr   = reinterpret_cast<uintptr_t>(control->data),
                              .length = prefix == nullptr ? wire_bytes : RDMA_CONTROL_CAPACITY,
                              .lkey   = control->registration_key};
    if (prefix != nullptr) {
        work->regions[1] = {.addr   = reinterpret_cast<uintptr_t>(slot->payload_data()),
                            .length = slot->len,
                            .lkey   = slot->registration_key};
    }
    work->send_wr         = {};
    work->send_wr.wr_id   = work->token.load(std::memory_order_acquire);
    work->send_wr.sg_list = work->regions.data();
    work->send_wr.num_sge = prefix == nullptr ? 1 : 2;
    work->send_wr.opcode  = IBV_WR_SEND;
    if (!pools.prepared_sends.empty()) {
        pools.prepared_sends.back()->send_wr.next = &work->send_wr;
    }
    pools.prepared_sends.push_back(work);
    pools.prepared_send_bytes += wire_bytes;
    return pools.prepared_sends.size() < config_.send_batch_size || post_prepared(connection, pools);
}

bool RdmaTransport::post_prepared(Connection *connection, ConnectionPools &pools)
{
    if (pools.prepared_sends.empty()) {
        return true;
    }
    Operation posting(pools.posting_state);
    auto     &batch                  = pools.prepared_sends;
    batch.back()->send_wr.send_flags = IBV_SEND_SIGNALED;
    ibv_send_wr *bad                 = &batch.front()->send_wr;
    const bool   can_post            = posting && connection->is_open() && connection->transport_handle != 0;
    int          error               = EINVAL;
    if (can_post) {
        stats_.send_post_calls.fetch_add(1, std::memory_order_relaxed);
        error = ibv_post_send(reinterpret_cast<ibv_qp *>(connection->transport_handle), &batch.front()->send_wr, &bad);
    }
    bool accepted = error == 0 || (can_post && bad != &batch.front()->send_wr);
    for (auto *work : batch) {
        if (error != 0 && &work->send_wr == bad) {
            accepted = false;
        }
        if (accepted) {
            work->send_posted = true;
            if (pools.send_tail != nullptr) {
                pools.send_tail->send_next = work;
            }
            else {
                pools.send_head = work;
            }
            pools.send_tail = work;
            ++pools.sends_in_flight;
            pools.send_bytes_in_flight += work->send_wire_bytes;
            stats_.sends.fetch_add(1, std::memory_order_relaxed);
            stats_.rpc_frames_sent.fetch_add(work->send_frame_count, std::memory_order_relaxed);
            stats_.wire_bytes_sent.fetch_add(work->send_wire_bytes, std::memory_order_relaxed);
        }
        else {
            ibv_wc completion{};
            completion.status = IBV_WC_SUCCESS;
            release_completed_frame(work, completion);
        }
    }
    batch.clear();
    pools.prepared_send_bytes = 0;
    if (error != 0) {
        stats_.send_errors.fetch_add(1, std::memory_order_relaxed);
        connection->close();
    }
    return error == 0;
}

void RdmaTransport::unlink_send(WorkRequest *work)
{
    auto pools = work->pools;
    if (pools == nullptr || !work->send_posted) {
        return;
    }
    WorkRequest *previous = nullptr;
    for (auto *entry = pools->send_head; entry != nullptr; entry = entry->send_next) {
        if (entry == work) {
            if (previous == nullptr) {
                pools->send_head = entry->send_next;
            }
            else {
                previous->send_next = entry->send_next;
            }
            if (pools->send_tail == work) {
                pools->send_tail = previous;
            }
            return;
        }
        previous = entry;
    }
}

void RdmaTransport::complete_send(WorkRequest *work, const ibv_wc &completion)
{
    auto pools = work->pools;
    if (completion.status != IBV_WC_SUCCESS || !work->send_posted || pools == nullptr) {
        unlink_send(work);
        release_completed_frame(work, completion);
        return;
    }
    stats_.send_completions.fetch_add(1, std::memory_order_relaxed);
    while (pools->send_head != nullptr) {
        auto      *entry = pools->send_head;
        const bool tail  = entry == work;
        if (!tail && claim_work(entry->token.load(std::memory_order_acquire)) != entry) {
            work->conn->close();
            unlink_send(work);
            release_completed_frame(work, completion);
            return;
        }
        pools->send_head = entry->send_next;
        if (pools->send_head == nullptr) {
            pools->send_tail = nullptr;
        }
        release_completed_frame(entry, completion);
        if (tail) {
            return;
        }
    }
    stats_.send_errors.fetch_add(1, std::memory_order_relaxed);
    work->conn->close();
    release_completed_frame(work, completion);
}
} // namespace crowdb::rpc
#endif
