// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../../status_json.h"
#include "crowdb-common/log.h"
#include "rdma_control_stream.h"
#include "rdma_cq.h"
#include "rdma_offer.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <cerrno>

namespace crowdb::rpc
{

bool RdmaTransport::create_connection_pools(ConnectionPools &pools)
{
    pools.prepared_sends.reserve(config_.send_batch_size);
    pools.prepared_receives.reserve(config_.receive_batch_size);
    const auto     control_bytes = config_.receive_control_bytes();
    const uint64_t data_bytes    = config_.wire_layout == RdmaWireLayout::ControlData
                                     ? uint64_t{config_.posted_receive_target} * pools.effective_data_capacity
                                     : config_.receive_arena_bytes;
    pools.send                   = make_pool(config_.send_arena_bytes, config_.sq_depth);
    pools.recv                   = make_pool(data_bytes, config_.rq_depth);
    if (control_bytes != 0)
        pools.recv_control = make_pool(control_bytes, config_.posted_receive_target);
    return pools.send != nullptr && pools.recv != nullptr && (control_bytes == 0 || pools.recv_control != nullptr);
}

void RdmaTransport::configure_peer(ConnectionPools &pools, const RdmaCmOffer &peer)
{
    pools.peer_receive_slot_capacity     = peer.receive_slot_capacity;
    pools.peer_receive_frame_limit       = peer.receive_frame_limit;
    pools.peer_generation                = peer.generation;
    pools.peer_max_control_message_bytes = peer.max_control_message_bytes;
    pools.effective_data_capacity        = config_.data_slot_capacity();
    if (config_.wire_layout == RdmaWireLayout::ControlData) {
        pools.effective_data_capacity = std::min(
            pools.effective_data_capacity, rdma_data_slot_capacity(peer.receive_slot_capacity, config_.wire_layout));
        pools.control_receive = std::make_shared<ControlStreamReceiver>(
            *control_receive_budget_, pools.effective_data_capacity, peer.generation, config_.max_control_message_bytes,
            std::chrono::milliseconds(config_.control_assembly_timeout_ms));
    }
}

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
    info.number("peer_receive_data_capacity",
                rdma_data_slot_capacity(pools->peer_receive_slot_capacity, config_.wire_layout));
    info.number("effective_max_send_sge", pools->effective_capacity.max_send_sge);
    info.number("effective_max_recv_sge", pools->effective_capacity.max_recv_sge);
    info.string("wire_layout", config_.wire_layout == RdmaWireLayout::Contiguous ? "contiguous" : "control_data");
    info.number("peer_receive_frame_limit", pools->peer_receive_frame_limit);
    info.number("effective_data_capacity", pools->effective_data_capacity);
    info.number("peer_generation", pools->peer_generation);
    info.number("peer_max_control_message_bytes", pools->peer_max_control_message_bytes);
    info.number("effective_max_control_message_bytes",
                std::min(config_.max_control_message_bytes, pools->peer_max_control_message_bytes));
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
