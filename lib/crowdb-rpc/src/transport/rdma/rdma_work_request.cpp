// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "rdma_cq.h"

#ifdef CROWDB_RPC_HAVE_RDMA

namespace crowdb::rpc
{

RdmaTransport::WorkRequest *RdmaTransport::acquire_work(WorkKind kind, Connection *conn, Buffer *slot, OutFrame *frame)
{
    auto owner = conn->weak_from_this().lock();
    if (owner == nullptr)
        return nullptr;
    auto          *pools    = pools_for(conn);
    const uint32_t cq_index = pools == nullptr ? 0 : pools->cq_index;
    if (cq_index >= completion_queues_.size())
        return nullptr;
    auto &queue = *completion_queues_[cq_index];
    for (uint32_t attempt = 0; attempt < config_.cq_depth; ++attempt) {
        const uint32_t index = queue.next_work.fetch_add(1, std::memory_order_relaxed) % config_.cq_depth;
        auto          &work  = queue.work_requests[index];
        uint64_t       empty = 0;
        if (!work.token.compare_exchange_strong(empty, WORK_RESERVED, std::memory_order_acq_rel))
            continue;
        work.kind           = kind;
        work.conn           = std::move(owner);
        work.pools          = pools == nullptr ? nullptr : pools->shared_from_this();
        work.slot           = slot;
        work.frame          = frame;
        work.request_id     = frame == nullptr ? slot->request_id : frame->request_id;
        work.stream_id      = slot->stream_id;
        work.sequence       = slot->sequence;
        work.logical_offset = slot->logical_offset;
        uint64_t generation = queue.generation.fetch_add(1, std::memory_order_relaxed);
        if (generation == 0)
            generation = queue.generation.fetch_add(1, std::memory_order_relaxed);
        const uint64_t global_index = static_cast<uint64_t>(cq_index) * config_.cq_depth + index;
        work.token.store((generation << 32) | global_index, std::memory_order_release);
        return &work;
    }
    return nullptr;
}

RdmaTransport::WorkRequest *RdmaTransport::claim_work(uint64_t token)
{
    const auto index = static_cast<uint32_t>(token);
    if (token == 0 || token == WORK_RESERVED ||
        static_cast<uint64_t>(index) >= static_cast<uint64_t>(config_.cq_depth) * config_.cq_count)
        return nullptr;
    auto &work = completion_queues_[index / config_.cq_depth]->work_requests[index % config_.cq_depth];
    return work.token.compare_exchange_strong(token, WORK_RESERVED, std::memory_order_acq_rel) ? &work : nullptr;
}

void RdmaTransport::release_work(WorkRequest *work)
{
    work->conn.reset();
    work->pools.reset();
    work->slot  = nullptr;
    work->frame = nullptr;
    work->token.store(0, std::memory_order_release);
}

void RdmaTransport::dispatch_completion(const ibv_wc &wc)
{
    WorkRequest *work = claim_work(wc.wr_id);
    if (work == nullptr)
        return;
    // Error completions do not provide a trustworthy opcode. The retained
    // work record owns dispatch, even after disconnect or QP destruction.
    if (work->kind == WorkKind::Receive && wc.status == IBV_WC_SUCCESS && !work->conn->has_frame_handler()) {
        const bool handshake = wc.byte_len >= HEADER_SIZE && work->slot->capacity >= HEADER_SIZE &&
                               parse_header(work->slot->data).msg_type == RDMA_HANDSHAKE_MSG_TYPE;
        if (!handshake) {
            const uint32_t index = static_cast<uint32_t>(wc.wr_id) / config_.cq_depth;
            completion_queues_[index]->deferred.push_back({work, wc});
            stats_.pending_deferred_receives.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
    if (work->kind == WorkKind::Receive)
        release_completed_receive(work, wc);
    else
        release_completed_frame(work, wc);
}

void RdmaTransport::cancel_work(Connection *connection)
{
    // Called only after the affected QPs are destroyed and device access has
    // stopped. Missing/uncertain completions must still release their owners.
    for (const auto &queue : completion_queues_) {
        while (queue->polling.test_and_set(std::memory_order_acquire))
            std::this_thread::yield();
        cancel_deferred(*queue, connection);
        for (uint32_t index = 0; index < config_.cq_depth; ++index) {
            auto          &work  = queue->work_requests[index];
            const uint64_t token = work.token.load(std::memory_order_acquire);
            if (token == 0 || token == WORK_RESERVED || (connection != nullptr && work.conn.get() != connection))
                continue;
            ibv_wc completion{};
            completion.wr_id  = token;
            completion.status = IBV_WC_WR_FLUSH_ERR;
            dispatch_completion(completion);
        }
        queue->polling.clear(std::memory_order_release);
    }
}

void RdmaTransport::close_after_drain(Connection *connection)
{
    // A disconnect notification can race CQ readiness. Publish completions
    // already acknowledged by the NIC before failing the remaining requests.
    const auto    *pools = pools_for(connection);
    const uint32_t index = pools == nullptr ? 0 : pools->cq_index;
    (void)drain_queue(index, config_.cq_depth, true);
    connection->close();
}

} // namespace crowdb::rpc

#endif
