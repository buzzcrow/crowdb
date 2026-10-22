// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../../status_json.h"
#include "crowdb-common/log.h"
#include "rdma_cq.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <cerrno>

namespace crowdb::rpc
{

RdmaTransport::ConnectionPools *RdmaTransport::pools_for(Connection *connection)
{
    return static_cast<ConnectionPools *>(connection->transport_context_.get());
}

void RdmaTransport::set_connection_pools(Connection *connection, std::shared_ptr<ConnectionPools> pools)
{
    connection->transport_context_ = std::move(pools);
}

bool RdmaTransport::publish_cm_connection(rdma_cm_id *id, const std::shared_ptr<Connection> &connection,
                                          std::shared_ptr<ConnectionPools> pools)
{
    pools->cm_id = id;
    // The shared CM consumer must see the owner before a migrated terminal
    // event can retire this ID. Setup events stay private until this point.
    StatusJson info;
    info.number("qpair_number", pools->qp_number);
    info.number("cq_index", pools->cq_index);
    info.number("effective_sq_depth", pools->effective_capacity.max_send_wr);
    info.number("effective_rq_depth", pools->effective_capacity.max_recv_wr);
    info.number("effective_cq_depth", completion_queues_[pools->cq_index]->effective_depth);
    info.number("peer_receive_slot_capacity", pools->peer_receive_slot_capacity);
    info.number("peer_receive_frame_limit", pools->peer_receive_frame_limit);
    connection->transport_info_json_ = info.finish();
    std::scoped_lock lock(cm_mu_);
    if (rdma_migrate_id(id, cm_channel_) != 0)
        return false;
    cm_ids_.push_back(id);
    cm_connections_.emplace(id, connection);
    cm_pools_.emplace(id, pools);
    auto &queue = *completion_queues_[pools->cq_index];
    while (queue.polling.test_and_set(std::memory_order_acquire))
        std::this_thread::yield();
    queue.connections.push_back({connection, std::move(pools)});
    queue.polling.clear(std::memory_order_release);
    notify_queue(queue.index);
    return true;
}

void RdmaTransport::request_disconnect(ConnectionPools &pools)
{
    // The existing posting gate pins the CM ID through disconnect. A terminal
    // CM consumer stops this gate before destroying the QP and ID.
    Operation posting(pools.posting_state);
    if (!posting || pools.cm_id == nullptr || pools.disconnect_requested.exchange(true, std::memory_order_acq_rel))
        return;
    if (rdma_disconnect(pools.cm_id) != 0)
        CRB_LOG_DEBUG("rdma disconnect request completed with errno={}", errno);
}

void RdmaTransport::destroy_connection_qp(Connection *connection, rdma_cm_id *id)
{
    connection->close();
    auto *pools = pools_for(connection);
    if (pools != nullptr) {
        request_disconnect(*pools);
        pools->posting_state->users.fetch_or(RepostState::STOPPED, std::memory_order_acq_rel);
        while ((pools->posting_state->users.load(std::memory_order_acquire) & ~RepostState::STOPPED) != 0)
            std::this_thread::yield();
    }
    if (id->qp != nullptr)
        rdma_destroy_qp(id);
    connection->transport_handle = 0;
    cancel_work(connection);
    if (pools != nullptr && pools->reserved_capacity != 0) {
        completion_queues_[pools->cq_index]->reserved_capacity.fetch_sub(pools->reserved_capacity,
                                                                         std::memory_order_release);
        pools->reserved_capacity = 0;
    }
}

} // namespace crowdb::rpc

#endif
