// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

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

    static void set_cq(RdmaTransport &transport, ibv_cq *cq)
    {
        transport.cq_ = cq;
        if (cq == nullptr)
            transport.event_engine_ = nullptr;
    }

    static void set_cq_polling(RdmaTransport &transport, bool polling)
    {
        if (polling)
            transport.cq_polling_.test_and_set(std::memory_order_acquire);
        else
            transport.cq_polling_.clear(std::memory_order_release);
    }
};

struct TestCompletionQueue
{
    ibv_context        context{};
    ibv_cq             cq{};
    std::deque<ibv_wc> ready;
    std::deque<ibv_wc> on_arm;
    uint32_t           arms = 0;

    TestCompletionQueue()
    {
        cq.context          = &context;
        cq.cq_context       = this;
        context.ops.poll_cq = [](ibv_cq *queue, int capacity, ibv_wc *output) {
            auto      &self  = *static_cast<TestCompletionQueue *>(queue->cq_context);
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
