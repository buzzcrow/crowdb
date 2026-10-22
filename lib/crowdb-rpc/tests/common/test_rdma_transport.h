// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "../../src/transport/rdma/rdma_cq.h"
#include "../../src/transport/rdma/rdma_device.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "crowdb-rpc/transport/socket_transport.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)

#    include <algorithm>
#    include <deque>

namespace crowdb::rpc
{

struct TestRdmaTransport
{
    static void advertise_expired_endpoint_for_tests(RdmaTransport &transport, const std::string &address,
                                                     uint16_t port)
    {
        transport.local_capability_.cm_address = address;
        transport.local_capability_.port       = port;
        transport.listen_active_.store(true, std::memory_order_release);
    }

    static void set_aggregation_for_tests(RdmaTransport &transport, bool enabled)
    {
        transport.config_.aggregation_enabled = enabled;
    }

    static void bind_sender_for_tests(RdmaTransport &transport, const std::shared_ptr<Connection> &connection,
                                      std::unique_ptr<RdmaBufferPool> pool, ibv_qp *qp, uint32_t peer_slot,
                                      uint32_t peer_frames)
    {
        auto pools  = std::make_shared<RdmaTransport::ConnectionPools>(transport.config_.send_queue_capacity);
        pools->send = std::move(pool);
        pools->peer_receive_slot_capacity     = peer_slot;
        pools->peer_receive_frame_limit       = peer_frames;
        pools->effective_capacity.max_send_wr = transport.config_.sq_depth;
        RdmaTransport::set_connection_pools(connection.get(), pools);
        connection->transport_handle = reinterpret_cast<uint64_t>(qp);
        transport.completion_queues_[0]->connections.push_back({connection, std::move(pools)});
    }

    static void install_device_for_tests(RdmaTransport &transport, const std::shared_ptr<RdmaDevice> &device)
    {
        transport.device_       = device;
        transport.memory_stats_ = device->memory;
        transport.pd_           = device->pd;
    }

    static std::unique_ptr<RdmaBufferPool> make_pool_for_tests(RdmaTransport &transport, uint64_t bytes, uint32_t count)
    {
        return transport.make_pool(bytes, count);
    }

    static std::weak_ptr<void> device_owner(RdmaTransport &transport)
    {
        return std::shared_ptr<void>(transport.device_);
    }

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
        transport.install_connection_notifiers(connection, index);
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

struct TestSocketTransport
{
    static RdmaTransport &rdma_for_tests(SocketTransport &transport)
    {
        return *transport.rdma_transport_;
    }
};

} // namespace crowdb::rpc

#endif
