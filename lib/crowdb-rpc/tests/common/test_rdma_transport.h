// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "../../src/transport/rdma/rdma_cq.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)

#    include <algorithm>
#    include <deque>

namespace crowdb::rpc
{

struct TestRdmaTransport
{
    static uint64_t send(RdmaTransport &transport, const std::shared_ptr<Connection> &connection, Buffer *slot,
                         OutFrame *frame)
    {
        auto *work = transport.acquire_work(RdmaTransport::WorkKind::Send, connection.get(), slot, frame);
        return work == nullptr ? 0 : work->token.load(std::memory_order_acquire);
    }

    static uint64_t receive(RdmaTransport &transport, const std::shared_ptr<Connection> &connection, Buffer *slot)
    {
        auto *work = transport.acquire_work(RdmaTransport::WorkKind::Receive, connection.get(), slot);
        return work == nullptr ? 0 : work->token.load(std::memory_order_acquire);
    }

    static void complete(RdmaTransport &transport, uint64_t token, ibv_wc_status status = IBV_WC_SUCCESS,
                         uint32_t length = 0)
    {
        ibv_wc completion{};
        completion.wr_id    = token;
        completion.status   = status;
        completion.byte_len = length;
        // Error providers may leave opcode unset or unrelated to the WR.
        completion.opcode = IBV_WC_RDMA_WRITE;
        transport.dispatch_completion(completion);
    }

    static void cancel(RdmaTransport &transport)
    {
        transport.cancel_work();
    }

    static void disconnect(RdmaTransport &transport, Connection *connection)
    {
        transport.close_after_drain(connection);
    }

    static void set_cq(RdmaTransport &transport, ibv_cq *cq, uint32_t index = 0)
    {
        transport.completion_queues_[index]->cq = cq;
        if (cq == nullptr)
            transport.event_engine_ = nullptr;
    }

    static void set_cq_polling(RdmaTransport &transport, bool polling, uint32_t index = 0)
    {
        if (polling)
            transport.completion_queues_[index]->polling.test_and_set(std::memory_order_acquire);
        else
            transport.completion_queues_[index]->polling.clear(std::memory_order_release);
    }

    static std::shared_ptr<void> bind_cq(const std::shared_ptr<Connection> &connection, uint32_t index)
    {
        auto pools      = std::make_shared<RdmaTransport::ConnectionPools>();
        pools->cq_index = index;
        RdmaTransport::set_connection_pools(connection.get(), pools);
        return pools;
    }

    static bool reserve(RdmaTransport &transport, uint32_t capacity, bool commit)
    {
        RdmaTransport::CqReservation reservation(transport.reserve_cq(capacity), capacity);
        reservation.committed = commit;
        return reservation.queue != nullptr;
    }

    static void watch_frames(RdmaTransport &transport, const std::shared_ptr<Connection> &connection, uint32_t index)
    {
        transport.install_frame_notifier(connection, index);
    }

    static bool pending(RdmaTransport &transport, uint64_t token)
    {
        const auto index = static_cast<uint32_t>(token);
        return transport.completion_queues_[index / transport.config_.cq_depth]
                   ->work_requests[index % transport.config_.cq_depth]
                   .token.load(std::memory_order_acquire) == token;
    }
};

struct TestCompletionQueue
{
    ibv_context        context{};
    ibv_cq             cq{};
    std::deque<ibv_wc> ready;
    std::deque<ibv_wc> on_arm;
    uint32_t           arms       = 0;
    uint64_t           poll_calls = 0;

    TestCompletionQueue()
    {
        cq.context          = &context;
        cq.cq_context       = this;
        context.ops.poll_cq = [](ibv_cq *queue, int capacity, ibv_wc *output) {
            auto &self = *static_cast<TestCompletionQueue *>(queue->cq_context);
            ++self.poll_calls;
            const auto count = std::min(static_cast<size_t>(capacity), self.ready.size());
            for (size_t index = 0; index < count; ++index) {
                output[index] = self.ready.front();
                self.ready.pop_front();
            }
            return static_cast<int>(count);
        };
        context.ops.req_notify_cq = [](ibv_cq *queue, int) {
            auto &self = *static_cast<TestCompletionQueue *>(queue->cq_context);
            ++self.arms;
            while (!self.on_arm.empty()) {
                self.ready.push_back(self.on_arm.front());
                self.on_arm.pop_front();
            }
            return 0;
        };
    }

    static ibv_wc completion(uint64_t token)
    {
        ibv_wc result{};
        result.wr_id  = token;
        result.status = IBV_WC_SUCCESS;
        return result;
    }
};

} // namespace crowdb::rpc

#endif
