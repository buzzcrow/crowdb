// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_cq.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <algorithm>
#    include <cerrno>

namespace crowdb::rpc
{
bool RdmaTransport::stage_receive(WorkRequest *work, std::span<const ibv_sge> regions)
{
    auto &pools = *work->pools;
    std::copy(regions.begin(), regions.end(), work->regions.begin());
    work->receive_wr         = {};
    work->receive_wr.wr_id   = work->token.load(std::memory_order_acquire);
    work->receive_wr.sg_list = work->regions.data();
    work->receive_wr.num_sge = static_cast<int>(regions.size());
    if (!pools.prepared_receives.empty()) {
        pools.prepared_receives.back()->receive_wr.next = &work->receive_wr;
    }
    pools.prepared_receives.push_back(work);
    return pools.prepared_receives.size() < config_.receive_batch_size ||
           post_prepared_receives(work->conn.get(), pools);
}

bool RdmaTransport::post_prepared_receives(Connection *connection, ConnectionPools &pools)
{
    auto &batch = pools.prepared_receives;
    if (batch.empty()) {
        return true;
    }
    Operation    operation(repost_state_);
    Operation    posting(pools.posting_state);
    const bool   can_post = operation && posting && connection->is_open() && connection->transport_handle != 0;
    ibv_recv_wr *bad      = &batch.front()->receive_wr;
    int          error    = EINVAL;
    if (can_post) {
        stats_.receive_post_calls.fetch_add(1, std::memory_order_relaxed);
        error =
            ibv_post_recv(reinterpret_cast<ibv_qp *>(connection->transport_handle), &batch.front()->receive_wr, &bad);
    }
    bool accepted = error == 0 || (can_post && bad != &batch.front()->receive_wr);
    for (auto *work : batch) {
        if (error != 0 && &work->receive_wr == bad) {
            accepted = false;
        }
        if (accepted) {
            stats_.posted_receives.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        auto *slot   = work->slot;
        auto *prefix = work->prefix;
        (void)slot->transition(BufferOwnership::PostedReceive, BufferOwnership::Available);
        if (prefix != nullptr) {
            (void)prefix->transition(BufferOwnership::PostedReceive, BufferOwnership::Available);
        }
        release_work(work);
        slot->release();
        if (prefix != nullptr) {
            prefix->release();
        }
    }
    batch.clear();
    if (error != 0) {
        connection->close();
    }
    return error == 0;
}

uint32_t RdmaTransport::flush_receive_reposts(CompletionQueue &queue, uint32_t budget)
{
    if (config_.receive_batch_size == 1) {
        return 0;
    }
    uint32_t                  count = 0;
    std::array<Buffer *, 256> slots{};
    for (size_t visited = 0; visited < queue.connections.size() && count < budget; ++visited) {
        const auto &entry            = queue.connections[queue.next_repost_connection % queue.connections.size()];
        queue.next_repost_connection = (queue.next_repost_connection + 1) % queue.connections.size();
        auto     &pools              = *entry.pools;
        Operation posting(pools.posting_state);
        if (!posting || !entry.connection->is_open()) {
            continue;
        }
        const auto limit = std::min(config_.receive_batch_size, budget - count);
        const auto taken = static_cast<uint32_t>(pools.receive_reposts.drain(slots.data(), static_cast<int>(limit)));
        bool       valid = true;
        for (uint32_t index = 0; index < taken; ++index) {
            if (valid) {
                valid = post_receive(entry.connection.get(), slots[index]);
            }
            slots[index]->release();
        }
        valid = post_prepared_receives(entry.connection.get(), pools) && valid;
        if (!valid) {
            stats_.receive_post_errors.fetch_add(1, std::memory_order_relaxed);
            entry.connection->close();
        }
        count += taken;
    }
    return count;
}
} // namespace crowdb::rpc
#endif
