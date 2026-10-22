// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

namespace crowdb::rpc
{

RdmaTransport::WorkRequest *RdmaTransport::acquire_work(WorkKind kind, Connection *conn, Buffer *slot, OutFrame *frame)
{
    auto owner = conn->weak_from_this().lock();
    if (owner == nullptr)
        return nullptr;
    for (uint32_t attempt = 0; attempt < config_.cq_depth; ++attempt) {
        const uint32_t index = next_work_.fetch_add(1, std::memory_order_relaxed) % config_.cq_depth;
        auto          &work  = work_requests_[index];
        uint64_t       empty = 0;
        if (!work.token.compare_exchange_strong(empty, WORK_RESERVED, std::memory_order_acq_rel))
            continue;
        work.kind                 = kind;
        work.conn                 = std::move(owner);
        work.slot                 = slot;
        work.frame                = frame;
        work.request_id           = frame == nullptr ? slot->request_id : frame->request_id;
        work.stream_id            = slot->stream_id;
        work.sequence             = slot->sequence;
        work.logical_offset       = slot->logical_offset;
        const uint64_t generation = work_generation_.fetch_add(1, std::memory_order_relaxed);
        work.token.store((generation << 32) | index, std::memory_order_release);
        return &work;
    }
    return nullptr;
}

RdmaTransport::WorkRequest *RdmaTransport::claim_work(uint64_t token)
{
    const auto index = static_cast<uint32_t>(token);
    if (token == 0 || token == WORK_RESERVED || index >= config_.cq_depth)
        return nullptr;
    auto &work = work_requests_[index];
    return work.token.compare_exchange_strong(token, WORK_RESERVED, std::memory_order_acq_rel) ? &work : nullptr;
}

void RdmaTransport::release_work(WorkRequest *work)
{
    work->conn.reset();
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
    if (work->kind == WorkKind::Receive)
        release_completed_receive(work, wc);
    else
        release_completed_frame(work, wc);
}

void RdmaTransport::cancel_work(Connection *connection)
{
    // Called only after the affected QPs are destroyed and device access has
    // stopped. Missing/uncertain completions must still release their owners.
    for (uint32_t index = 0; index < config_.cq_depth; ++index) {
        auto          &work  = work_requests_[index];
        const uint64_t token = work.token.load(std::memory_order_acquire);
        if (token == 0 || token == WORK_RESERVED || (connection != nullptr && work.conn.get() != connection))
            continue;
        ibv_wc completion{};
        completion.wr_id  = token;
        completion.status = IBV_WC_WR_FLUSH_ERR;
        dispatch_completion(completion);
    }
}

void RdmaTransport::close_after_drain(Connection *connection)
{
    // A disconnect notification can race CQ readiness. Publish completions
    // already acknowledged by the NIC before failing the remaining requests.
    while (drain_cq(config_.poll_batch) > 0) {
    }
    connection->close();
}

} // namespace crowdb::rpc

#endif
