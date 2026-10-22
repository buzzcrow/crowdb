// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_cq.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include "crowdb-common/log.h"
#    include "crowdb-rpc/transport/epoll/epoll_engine.h"

#    include <unistd.h>

namespace crowdb::rpc
{

void RdmaTransport::start_pollers()
{
    for (const auto &owner : completion_queues_) {
        auto &queue = *owner;
        if (queue.cq == nullptr || queue.worker.joinable())
            continue;
        auto engine = std::make_unique<EpollEngine>();
        if (engine->init() != 0) {
            capability_available_.store(false, std::memory_order_release);
            CRB_LOG_ERROR("rdma CQ poller epoll initialization failed; RDMA setup unavailable: cq_index={}",
                          queue.index);
            return;
        }
        if (event_engine_ != nullptr) {
            if (queue.channel != nullptr)
                event_engine_->remove_external_fd(queue.channel->fd);
            event_engine_->remove_external_fd(queue.continuation_fd);
        }
        if (queue.channel != nullptr)
            engine->add_external_fd(queue.channel->fd, 1);
        engine->add_external_fd(queue.continuation_fd, 2);
        queue.engine = std::move(engine);
        queue.managed.store(true, std::memory_order_release);
        queue.event_wakeup.store(true, std::memory_order_release);
        try {
            queue.worker = std::thread([this, pointer = &queue] { run_poller(pointer); });
        }
        catch (const std::system_error &error) {
            queue.managed.store(false, std::memory_order_release);
            capability_available_.store(false, std::memory_order_release);
            CRB_LOG_ERROR("rdma CQ poller thread creation failed; RDMA setup unavailable: cq_index={} error={}",
                          queue.index, error.what());
            return;
        }
        stats_.poller_count.fetch_add(1, std::memory_order_relaxed);
    }
}

void RdmaTransport::run_poller(CompletionQueue *queue)
{
    const std::string name = "rdma-cq-" + std::to_string(queue->index);
    crowdb::common::set_current_thread_name(name.c_str());
    std::array<EngineEvent, 8> events{};
    while (running_.load(std::memory_order_acquire)) {
        queue->wake_state.exchange(0, std::memory_order_acq_rel);
        if (drain_queue(queue->index, config_.poll_batch) > 0)
            continue;
        const auto pending = queue->wake_state.fetch_or(CompletionQueue::IDLE, std::memory_order_acq_rel);
        if ((pending & CompletionQueue::PENDING) != 0) {
            queue->wake_state.fetch_and(~CompletionQueue::IDLE, std::memory_order_acq_rel);
            continue;
        }
        const int count = queue->engine->wait(events.data(), events.size(), 1000);
        if (count > 0) {
            queue->event_wakeup.store(true, std::memory_order_release);
            stats_.cq_wakeups.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void RdmaTransport::notify_queue(uint32_t index)
{
    auto      &queue    = *completion_queues_[index];
    const auto previous = queue.wake_state.exchange(CompletionQueue::PENDING, std::memory_order_acq_rel);
    if (queue.managed.load(std::memory_order_acquire) && (previous & CompletionQueue::IDLE) == 0)
        return;
    const uint64_t value = 1;
    (void)::write(queue.continuation_fd, &value, sizeof(value));
}

void RdmaTransport::install_connection_notifiers(const std::shared_ptr<Connection> &connection, uint32_t index)
{
    auto notify = [state = repost_state_, index] {
        Operation operation(state);
        if (operation)
            state->transport->notify_queue(index);
    };
    connection->set_frame_ready_notifier(notify);
    connection->on_transport_close_.set(std::move(notify));
}

uint32_t RdmaTransport::process_deferred(CompletionQueue &queue, uint32_t max_completions)
{
    uint32_t completed = 0;
    size_t   retained  = 0;
    for (auto pending : queue.deferred) {
        if (completed < max_completions &&
            (pending.work->conn->has_frame_handler() || !pending.work->conn->is_open())) {
            if (!pending.work->conn->is_open())
                pending.completion.status = IBV_WC_WR_FLUSH_ERR;
            release_completed_receive(pending.work, pending.completion);
            stats_.pending_deferred_receives.fetch_sub(1, std::memory_order_relaxed);
            ++completed;
        }
        else {
            queue.deferred[retained++] = pending;
        }
    }
    queue.deferred.resize(retained);
    return completed;
}

void RdmaTransport::cancel_deferred(CompletionQueue &queue, Connection *connection)
{
    size_t retained = 0;
    for (auto pending : queue.deferred) {
        if (connection == nullptr || pending.work->conn.get() == connection) {
            pending.completion.status = IBV_WC_WR_FLUSH_ERR;
            release_completed_receive(pending.work, pending.completion);
            stats_.pending_deferred_receives.fetch_sub(1, std::memory_order_relaxed);
        }
        else {
            queue.deferred[retained++] = pending;
        }
    }
    queue.deferred.resize(retained);
}

} // namespace crowdb::rpc

#endif
