// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_cq.h"

#ifdef CROWDB_RPC_HAVE_RDMA

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
    // The shared CM consumer must see the owner before a migrated terminal
    // event can retire this ID. Setup events stay private until this point.
    std::scoped_lock lock(cm_mu_);
    if (rdma_migrate_id(id, cm_channel_) != 0)
        return false;
    cm_ids_.push_back(id);
    cm_connections_.emplace(id, connection);
    cm_pools_.emplace(id, std::move(pools));
    return true;
}

void RdmaTransport::destroy_connection_qp(Connection *connection, rdma_cm_id *id)
{
    connection->close();
    auto *pools = pools_for(connection);
    if (pools != nullptr) {
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
