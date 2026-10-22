// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_cq.h"

#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "rdma_control_stream.h"
#include "rdma_device.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include "crowdb-common/log.h"
#    include "rdma_channel.h"

#    include <unistd.h>

#    include <utility>

namespace crowdb::rpc
{

RdmaTransport::CqReservation::~CqReservation()
{
    if (queue != nullptr && !committed) {
        queue->reserved_capacity.fetch_sub(capacity, std::memory_order_release);
    }
}

RdmaTransport::CompletionQueue *RdmaTransport::reserve_cq(uint32_t capacity)
{
    const uint32_t first = next_cq_.fetch_add(1, std::memory_order_relaxed) % config_.cq_count;
    for (uint32_t offset = 0; offset < config_.cq_count; ++offset) {
        auto    &queue = *completion_queues_[(first + offset) % config_.cq_count];
        uint32_t used  = queue.reserved_capacity.load(std::memory_order_relaxed);
        while (used <= config_.cq_depth && capacity <= config_.cq_depth - used) {
            if (queue.reserved_capacity.compare_exchange_weak(used, used + capacity, std::memory_order_acq_rel)) {
                return &queue;
            }
        }
    }
    CRB_LOG_WARN("rdma CQ capacity exhausted: cq_count={} cq_depth={} required_entries={}", config_.cq_count,
                 config_.cq_depth, capacity);
    return nullptr;
}

bool RdmaTransport::create_completion_queues(ibv_context *context)
{
    for (const auto &owner : completion_queues_) {
        auto &queue   = *owner;
        queue.channel = ibv_create_comp_channel(context);
        if (queue.channel == nullptr) {
            destroy_completion_queues();
            return false;
        }
        rdma_detail::make_nonblocking(queue.channel->fd);
        queue.cq = ibv_create_cq(context, static_cast<int>(config_.cq_depth), &queue, queue.channel, 0);
        if (queue.cq == nullptr || std::cmp_less(queue.cq->cqe, config_.cq_depth) ||
            ibv_req_notify_cq(queue.cq, 0) != 0) {
            destroy_completion_queues();
            return false;
        }
        queue.effective_depth = static_cast<uint32_t>(queue.cq->cqe);
    }
    return true;
}

void RdmaTransport::refresh_poll_stats() const
{
    uint64_t polls        = 0;
    uint64_t empty        = 0;
    uint64_t direct_sends = 0;
    uint64_t direct_bytes = 0;
    uint64_t copied_bytes = 0;
    for (const auto &queue : completion_queues_) {
        polls += queue->poll_calls.load(std::memory_order_relaxed);
        empty += queue->empty_polls.load(std::memory_order_relaxed);
        direct_sends += queue->direct_sends.load(std::memory_order_relaxed);
        direct_bytes += queue->direct_payload_bytes.load(std::memory_order_relaxed);
        copied_bytes += queue->copied_payload_bytes.load(std::memory_order_relaxed);
    }
    stats_.cq_polls.store(polls, std::memory_order_relaxed);
    stats_.cq_empty_polls.store(empty, std::memory_order_relaxed);
    stats_.direct_sends.store(direct_sends, std::memory_order_relaxed);
    stats_.direct_payload_bytes.store(direct_bytes, std::memory_order_relaxed);
    stats_.copied_payload_bytes.store(copied_bytes, std::memory_order_relaxed);
    stats_.registered_memory_budget_bytes.store(memory_stats_->budget, std::memory_order_relaxed);
    stats_.registered_bytes.store(memory_stats_->current.load(std::memory_order_acquire), std::memory_order_relaxed);
    stats_.registered_bytes_high_water.store(memory_stats_->high_water.load(std::memory_order_relaxed),
                                             std::memory_order_relaxed);
    stats_.registration_rejects.store(memory_stats_->rejects.load(std::memory_order_relaxed),
                                      std::memory_order_relaxed);
    stats_.registration_failures.store(memory_stats_->failures.load(std::memory_order_relaxed),
                                       std::memory_order_relaxed);
    if (control_send_budget_ != nullptr) {
        stats_.control_send_bytes.store(control_send_budget_->used(), std::memory_order_relaxed);
        stats_.control_send_bytes_high_water.store(control_send_budget_->high_water(), std::memory_order_relaxed);
        stats_.control_send_stalls.store(control_send_budget_->stalls(), std::memory_order_relaxed);
        stats_.control_receive_bytes.store(control_receive_budget_->used(), std::memory_order_relaxed);
        stats_.control_receive_bytes_high_water.store(control_receive_budget_->high_water(), std::memory_order_relaxed);
        stats_.control_receive_stalls.store(control_receive_budget_->stalls(), std::memory_order_relaxed);
    }
}

void RdmaTransport::destroy_completion_queues()
{
    for (const auto &owner : completion_queues_) {
        auto &queue = *owner;
        if (queue.cq != nullptr) {
            ibv_destroy_cq(queue.cq);
            queue.cq = nullptr;
        }
        if (queue.channel != nullptr) {
            ibv_destroy_comp_channel(queue.channel);
            queue.channel = nullptr;
        }
    }
}

} // namespace crowdb::rpc

#endif
