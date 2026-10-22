// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "rdma_channel.h"
#include "rdma_cq.h"
#include "rdma_offer.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <arpa/inet.h>

#    include <algorithm>
#    include <cstdlib>
#    include <cstring>

namespace crowdb::rpc
{

std::shared_ptr<Connection> RdmaTransport::connect(const std::string &addr, int port,
                                                   const RdmaEndpointCapability *expected_peer)
{
    if (!ready() || addr.empty() || port <= 0 || port > 65535 ||
        (config_.require_tcp_preflight && expected_peer == nullptr)) {

        CRB_LOG_WARN("rdma connect: transport unavailable addr={} port={}", addr, port);
        return nullptr;
    }
    stats_.cm_attempts.fetch_add(1, std::memory_order_relaxed);
    {
        std::scoped_lock lock(cm_mu_);
        if (cm_channel_ == nullptr)
            cm_channel_ = rdma_create_event_channel();
        if (cm_channel_ == nullptr)
            return nullptr;
        rdma_detail::make_nonblocking(cm_channel_->fd);
    }
    // Active setup has a private channel. The epoll worker may drain existing
    // connections without consuming this attempt's address/route events.
    auto setup_channel = std::unique_ptr<rdma_event_channel, decltype(&rdma_destroy_event_channel)>(
        rdma_create_event_channel(), &rdma_destroy_event_channel);
    if (setup_channel == nullptr)
        return nullptr;
    rdma_detail::make_nonblocking(setup_channel->fd);
    rdma_cm_id *id = nullptr;
    if (rdma_create_id(setup_channel.get(), &id, this, RDMA_PS_TCP) != 0)
        return nullptr;
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &destination.sin_addr) != 1) {
        CRB_LOG_WARN("rdma connect: invalid destination {}", addr);
        rdma_destroy_id(id);
        return nullptr;
    }
    sockaddr_in source{};
    sockaddr   *source_address = nullptr;
    if (const char *source_env = config_.source_address.c_str(); *source_env != '\0') {
        source.sin_family = AF_INET;
        if (::inet_pton(AF_INET, source_env, &source.sin_addr) != 1) {
            CRB_LOG_WARN("rdma connect: invalid source address {}", source_env);
            rdma_destroy_id(id);
            return nullptr;
        }
        source_address = reinterpret_cast<sockaddr *>(&source);
        if (rdma_bind_addr(id, source_address) != 0) {

            CRB_LOG_WARN("rdma connect: bind source failed source={} errno={}", source_env, errno);
            rdma_destroy_id(id);
            return nullptr;
        }
    }
    // When a source address was explicitly bound above, librdmacm expects a
    // null source here; passing it twice can fail immediately with EINVAL.
    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr *>(&destination), 2000) != 0) {

        CRB_LOG_WARN("rdma connect: resolve_addr failed addr={} port={} errno={}", addr, port, errno);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_cm_event *event = nullptr;
    if (!rdma_detail::wait_cm_event(setup_channel.get(), &event, 3000) || event == nullptr ||
        event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {

        CRB_LOG_WARN("rdma connect: ADDR_RESOLVED missing addr={} event={} errno={}", addr,
                     event == nullptr ? -1 : event->event, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);
    event = nullptr;
    if (rdma_resolve_route(id, 2000) != 0) {
        CRB_LOG_WARN("rdma connect: resolve_route failed addr={} errno={}", addr, errno);
        rdma_destroy_id(id);
        return nullptr;
    }
    if (!rdma_detail::wait_cm_event(setup_channel.get(), &event, 3000) || event == nullptr ||
        event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {

        CRB_LOG_WARN("rdma connect: ROUTE_RESOLVED missing addr={} event={} errno={}", addr,
                     event == nullptr ? -1 : event->event, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);
    event = nullptr;

    if (!adopt_cm_context(id->verbs)) {
        rdma_destroy_id(id);
        return nullptr;
    }

    ibv_device_attr device_attributes{};
    if (ibv_query_device(id->verbs, &device_attributes) != 0) {
        rdma_destroy_id(id);
        return nullptr;
    }
    const uint64_t required_capacity = static_cast<uint64_t>(config_.sq_depth) + config_.posted_receive_target;
    CqReservation  reservation(required_capacity > UINT32_MAX ? nullptr
                                                              : reserve_cq(static_cast<uint32_t>(required_capacity)),
                               static_cast<uint32_t>(required_capacity));
    if (reservation.queue == nullptr) {
        rdma_destroy_id(id);
        return nullptr;
    }
    ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq             = reservation.queue->cq;
    qp_attr.recv_cq             = reservation.queue->cq;
    qp_attr.qp_type             = IBV_QPT_RC;
    qp_attr.cap.max_send_wr     = std::min(config_.sq_depth, static_cast<uint32_t>(device_attributes.max_qp_wr));
    qp_attr.cap.max_recv_wr     = std::min(config_.rq_depth, static_cast<uint32_t>(device_attributes.max_qp_wr));
    qp_attr.cap.max_send_sge    = std::min(config_.qp_max_send_sge, static_cast<uint32_t>(device_attributes.max_sge));
    qp_attr.cap.max_recv_sge    = std::min(config_.qp_max_recv_sge, static_cast<uint32_t>(device_attributes.max_sge));
    qp_attr.cap.max_inline_data = 0;
    qp_attr.sq_sig_all          = 1;
    qp_attr.qp_context          = id;

    if (rdma_create_qp(id, pd_, &qp_attr) != 0) {

        rdma_destroy_id(id);
        return nullptr;
    }
    if (config_.wire_layout == RdmaWireLayout::ControlData &&
        (qp_attr.cap.max_send_sge < 2 || qp_attr.cap.max_recv_sge < 2)) {
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    auto pools                = std::make_shared<ConnectionPools>(config_.send_queue_capacity);
    pools->effective_capacity = qp_attr.cap;
    pools->cq_index           = reservation.queue->index;
    pools->reserved_capacity  = reservation.capacity;
    if (!create_connection_pools(*pools)) {
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    auto connection = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), addr, application_pool_);
    set_connection_pools(connection.get(), pools);
    reservation.committed = true;
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    install_connection_notifiers(connection, pools->cq_index);
    if (!prime_receive(connection)) {
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_conn_param params{};
    const auto      local      = local_capability();
    const auto      offer      = serialize_rdma_offer(local, expected_peer);
    params.private_data        = offer.data();
    params.private_data_len    = static_cast<uint8_t>(offer.size());
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = config_.retry_count;
    params.rnr_retry_count     = config_.rnr_retry_count;
    if (rdma_connect(id, &params) != 0) {

        CRB_LOG_WARN("rdma connect: rdma_connect failed addr={} errno={}", addr, errno);
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    if (!rdma_detail::wait_cm_event(setup_channel.get(), &event, 5000) || event == nullptr ||
        event->event != RDMA_CM_EVENT_ESTABLISHED) {

        CRB_LOG_WARN("rdma connect: ESTABLISHED missing addr={} event={} status={} errno={}", addr,
                     event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    RdmaCmOffer peer_offer;
    const bool  valid_peer = event->param.conn.private_data != nullptr &&
                             parse_rdma_offer(std::span(static_cast<const uint8_t *>(event->param.conn.private_data),
                                                        event->param.conn.private_data_len),
                                              &peer_offer) &&
                             validate_rdma_offer(peer_offer, local, expected_peer, true);
    rdma_ack_cm_event(event);
    if (!valid_peer) {
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    pools->peer_receive_slot_capacity = peer_offer.receive_slot_capacity;
    pools->peer_receive_frame_limit   = peer_offer.receive_frame_limit;
    pools->qp_number                  = id->qp->qp_num;
    ibv_qp_attr receive_attributes{};
    receive_attributes.min_rnr_timer = config_.min_rnr_timer;
    if (ibv_modify_qp(id->qp, &receive_attributes, IBV_QP_MIN_RNR_TIMER) != 0) {
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    if (!publish_cm_connection(id, connection, std::move(pools))) {
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    return connection;
}

bool RdmaTransport::listen(const std::string &addr, int port)
{
    if (addr.empty() || port <= 0 || port > 65535 || listen_id_ != nullptr) {

        CRB_LOG_WARN("rdma listen: transport unavailable pd={} addr={} port={} existing_listener={}",
                     static_cast<void *>(pd_), addr, port, listen_id_ != nullptr);
        return false;
    }
    if (cm_channel_ == nullptr)
        cm_channel_ = rdma_create_event_channel();
    if (cm_channel_ == nullptr || rdma_create_id(cm_channel_, &listen_id_, this, RDMA_PS_TCP) != 0) {
        CRB_LOG_WARN("rdma listen: create CM id failed addr={} port={} errno={}", addr, port, errno);
        return false;
    }
    rdma_detail::make_nonblocking(cm_channel_->fd);
    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &local.sin_addr) != 1) {
        CRB_LOG_WARN("rdma listen: invalid address {}", addr);
        rdma_destroy_id(listen_id_);
        listen_id_ = nullptr;
        return false;
    }
    if (rdma_bind_addr(listen_id_, reinterpret_cast<sockaddr *>(&local)) != 0 || rdma_listen(listen_id_, 16) != 0) {
        CRB_LOG_WARN("rdma listen: bind/listen failed addr={} port={} errno={}", addr, port, errno);
        rdma_destroy_id(listen_id_);
        listen_id_ = nullptr;
        return false;
    }
    local_capability_.cm_address = addr;
    local_capability_.port       = static_cast<uint16_t>(port);
    listen_active_.store(true, std::memory_order_release);
    return true;
}

std::shared_ptr<Connection> RdmaTransport::accept()
{
    if (cm_channel_ == nullptr)
        return nullptr;
    rdma_cm_event *event = nullptr;
    while (rdma_get_cm_event(cm_channel_, &event) == 0) {
        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST && listen_id_ != nullptr)
            break;
        if (event != nullptr) {
            rdma_cm_id *event_id   = event->id;
            const int   event_type = event->event;
            rdma_ack_cm_event(event);
            if (event_id != nullptr && event_id != listen_id_ &&
                (event_type == RDMA_CM_EVENT_DISCONNECTED || event_type == RDMA_CM_EVENT_CONNECT_ERROR ||
                 event_type == RDMA_CM_EVENT_DEVICE_REMOVAL || event_type == RDMA_CM_EVENT_ADDR_CHANGE)) {
                std::shared_ptr<Connection> closed_connection;
                {
                    std::scoped_lock lock(cm_mu_);
                    if (auto it = cm_connections_.find(event_id); it != cm_connections_.end()) {
                        closed_connection = it->second;
                        cm_connections_.erase(it);
                    }
                    if (auto it = cm_pools_.find(event_id); it != cm_pools_.end()) {
                        cm_pools_.erase(it);
                    }
                    cm_ids_.erase(std::remove(cm_ids_.begin(), cm_ids_.end(), event_id), cm_ids_.end());
                }
                if (closed_connection == nullptr)
                    continue;
                close_after_drain(closed_connection.get());
                if (event_type == RDMA_CM_EVENT_DISCONNECTED || event_type == RDMA_CM_EVENT_CONNECT_ERROR)
                    stats_.cm_disconnects.fetch_add(1, std::memory_order_relaxed);
                if (closed_connection != nullptr)
                    destroy_connection_qp(closed_connection.get(), event_id);
                else if (event_id->qp != nullptr)
                    rdma_destroy_qp(event_id);
                rdma_destroy_id(event_id);
            }
        }
        event = nullptr;
    }
    if (event == nullptr)
        return nullptr;
    rdma_cm_id *id = event->id;
    RdmaCmOffer peer_offer;
    const bool valid_peer = event->param.conn.private_data != nullptr &&
                            parse_rdma_offer(std::span(static_cast<const uint8_t *>(event->param.conn.private_data),
                                                       event->param.conn.private_data_len),
                                             &peer_offer) &&
                            validate_rdma_offer(peer_offer, local_capability(), nullptr, config_.require_tcp_preflight);
    rdma_ack_cm_event(event);
    if (!valid_peer) {
        rdma_reject(id, nullptr, 0);
        rdma_destroy_id(id);
        return nullptr;
    }

    // A new CONNECT_REQUEST may precede this connection's ESTABLISHED on
    // the listener channel. Keep setup events private until readiness.
    auto setup_channel = std::unique_ptr<rdma_event_channel, decltype(&rdma_destroy_event_channel)>(
        rdma_create_event_channel(), &rdma_destroy_event_channel);
    if (setup_channel == nullptr || rdma_migrate_id(id, setup_channel.get()) != 0) {
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_detail::make_nonblocking(setup_channel->fd);

    if (!adopt_cm_context(id->verbs)) {
        rdma_destroy_id(id);
        return nullptr;
    }

    const uint64_t required_capacity = static_cast<uint64_t>(config_.sq_depth) + config_.posted_receive_target;
    CqReservation  reservation(required_capacity > UINT32_MAX ? nullptr
                                                              : reserve_cq(static_cast<uint32_t>(required_capacity)),
                               static_cast<uint32_t>(required_capacity));
    if (reservation.queue == nullptr) {
        rdma_destroy_id(id);
        return nullptr;
    }
    ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq             = reservation.queue->cq;
    qp_attr.recv_cq             = reservation.queue->cq;
    qp_attr.qp_type             = IBV_QPT_RC;
    qp_attr.cap.max_send_wr     = config_.sq_depth;
    qp_attr.cap.max_recv_wr     = config_.rq_depth;
    qp_attr.cap.max_send_sge    = config_.qp_max_send_sge;
    qp_attr.cap.max_recv_sge    = config_.qp_max_recv_sge;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.sq_sig_all          = 1;
    qp_attr.qp_context          = id;

    if (rdma_create_qp(id, pd_, &qp_attr) != 0) {
        rdma_destroy_id(id);
        return nullptr;
    }
    auto pools                        = std::make_shared<ConnectionPools>(config_.send_queue_capacity);
    pools->peer_receive_slot_capacity = peer_offer.receive_slot_capacity;
    pools->peer_receive_frame_limit   = peer_offer.receive_frame_limit;
    pools->qp_number                  = id->qp->qp_num;
    pools->effective_capacity         = qp_attr.cap;
    pools->cq_index                   = reservation.queue->index;
    pools->reserved_capacity          = reservation.capacity;
    if ((config_.wire_layout == RdmaWireLayout::ControlData &&
         (qp_attr.cap.max_send_sge < 2 || qp_attr.cap.max_recv_sge < 2)) ||
        !create_connection_pools(*pools)) {
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    auto connection = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), "rdma-peer", application_pool_);
    set_connection_pools(connection.get(), pools);
    reservation.committed = true;
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    install_connection_notifiers(connection, pools->cq_index);
    if (!prime_receive(connection)) {
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }

    rdma_conn_param params{};
    const auto      reply      = serialize_rdma_reply(local_capability(), peer_offer);
    params.private_data        = reply.data();
    params.private_data_len    = static_cast<uint8_t>(reply.size());
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = config_.retry_count;
    params.rnr_retry_count     = config_.rnr_retry_count;
    const int accept_rc        = rdma_accept(id, &params);

    if (accept_rc != 0) {
        CRB_LOG_WARN("rdma accept: rdma_accept failed errno={}", errno);
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    event              = nullptr;
    const int event_rc = rdma_detail::wait_cm_event(setup_channel.get(), &event, 5000) ? 0 : -1;

    if (event_rc != 0 || event == nullptr || event->event != RDMA_CM_EVENT_ESTABLISHED) {
        CRB_LOG_WARN("rdma accept: ESTABLISHED missing event={} status={} errno={}",
                     event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);

    ibv_qp_attr receive_attributes{};
    receive_attributes.min_rnr_timer = config_.min_rnr_timer;
    if (ibv_modify_qp(id->qp, &receive_attributes, IBV_QP_MIN_RNR_TIMER) != 0) {
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    if (!publish_cm_connection(id, connection, std::move(pools))) {
        destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
        return nullptr;
    }
    return connection;
}

} // namespace crowdb::rpc

#endif
