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

namespace
{
bool resolve_active_route(rdma_cm_id *id, rdma_event_channel *channel, const std::string &addr, int port,
                          const std::string &source_address_text)
{
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &destination.sin_addr) != 1) {
        CRB_LOG_WARN("rdma connect: invalid destination {}", addr);
        return false;
    }
    sockaddr_in source{};
    sockaddr   *source_address = nullptr;
    if (const char *source_env = source_address_text.c_str(); *source_env != '\0') {
        source.sin_family = AF_INET;
        if (::inet_pton(AF_INET, source_env, &source.sin_addr) != 1) {
            CRB_LOG_WARN("rdma connect: invalid source address {}", source_env);
            return false;
        }
        source_address = reinterpret_cast<sockaddr *>(&source);
        if (rdma_bind_addr(id, source_address) != 0) {

            CRB_LOG_WARN("rdma connect: bind source failed source={} errno={}", source_env, errno);
            return false;
        }
    }
    // When a source address was explicitly bound above, librdmacm expects a
    // null source here; passing it twice can fail immediately with EINVAL.
    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr *>(&destination), 2000) != 0) {

        CRB_LOG_WARN("rdma connect: resolve_addr failed addr={} port={} errno={}", addr, port, errno);
        return false;
    }
    rdma_cm_event *event = nullptr;
    if (!rdma_detail::wait_cm_event(channel, &event, 3000) || event == nullptr ||
        event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {

        CRB_LOG_WARN("rdma connect: ADDR_RESOLVED missing addr={} event={} errno={}", addr,
                     event == nullptr ? -1 : event->event, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        return false;
    }
    rdma_ack_cm_event(event);
    event = nullptr;
    if (rdma_resolve_route(id, 2000) != 0) {
        CRB_LOG_WARN("rdma connect: resolve_route failed addr={} errno={}", addr, errno);
        return false;
    }
    if (!rdma_detail::wait_cm_event(channel, &event, 3000) || event == nullptr ||
        event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {

        CRB_LOG_WARN("rdma connect: ROUTE_RESOLVED missing addr={} event={} errno={}", addr,
                     event == nullptr ? -1 : event->event, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        return false;
    }
    rdma_ack_cm_event(event);
    event = nullptr;
    return true;
}
} // namespace

std::shared_ptr<Connection> RdmaTransport::create_active_connection(rdma_cm_id *id, const std::string &addr,
                                                                    std::shared_ptr<ConnectionPools> &pools,
                                                                    const RdmaEndpointCapability     *expected_peer)
{
    if (!adopt_cm_context(id->verbs)) {
        return nullptr;
    }

    ibv_device_attr device_attributes{};
    if (ibv_query_device(id->verbs, &device_attributes) != 0) {
        return nullptr;
    }
    const uint64_t required_capacity = static_cast<uint64_t>(config_.sq_depth) + config_.posted_receive_target;
    CqReservation  reservation(required_capacity > UINT32_MAX ? nullptr
                                                              : reserve_cq(static_cast<uint32_t>(required_capacity)),
                               static_cast<uint32_t>(required_capacity));
    if (reservation.queue == nullptr) {
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
    qp_attr.sq_sig_all          = 0;
    qp_attr.qp_context          = id;

    if (rdma_create_qp(id, pd_, &qp_attr) != 0) {

        return nullptr;
    }
    if (config_.wire_layout == RdmaWireLayout::ControlData &&
        (qp_attr.cap.max_send_sge < 2 || qp_attr.cap.max_recv_sge < 2)) {
        rdma_destroy_qp(id);
        return nullptr;
    }
    pools = std::make_shared<ConnectionPools>(config_.send_queue_capacity,
                                              config_.receive_batch_size > 1 ? config_.posted_receive_target : 0);
    pools->effective_capacity = qp_attr.cap;
    pools->cq_index           = reservation.queue->index;
    pools->reserved_capacity  = reservation.capacity;
    auto connection           = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), addr, application_pool_);
    set_connection_pools(connection.get(), pools);
    reservation.committed = true;
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    install_connection_notifiers(connection, pools->cq_index);
    // Preflight already binds the direct receive class. Prepost its negotiated
    // slots before CM connect so an immediate peer response finds RQ credits.
    if (config_.wire_layout == RdmaWireLayout::Contiguous || expected_peer != nullptr) {
        if (config_.wire_layout == RdmaWireLayout::ControlData) {
            RdmaCmOffer peer{.receive_frame_limit       = static_cast<uint16_t>(expected_peer->receive_frame_limit),
                             .receive_slot_capacity     = expected_peer->receive_slot_capacity,
                             .wire_layout               = expected_peer->wire_layout,
                             .generation                = expected_peer->generation,
                             .max_control_message_bytes = expected_peer->max_control_message_bytes};
            configure_peer(*pools, peer);
        }
        else {
            pools->effective_data_capacity = config_.data_slot_capacity();
        }
        if (!create_connection_pools(*pools) || !prime_receive(connection)) {
            destroy_connection_qp(connection.get(), id);
            return nullptr;
        }
    }
    return connection;
}

bool RdmaTransport::establish_active_connection(rdma_cm_id *id, rdma_event_channel *channel, const std::string &addr,
                                                const RdmaEndpointCapability *expected_peer, RdmaCmOffer &peer_offer)
{
    rdma_cm_event  *event = nullptr;
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
        return false;
    }
    if (!rdma_detail::wait_cm_event(channel, &event, 5000) || event == nullptr ||
        event->event != RDMA_CM_EVENT_ESTABLISHED) {

        CRB_LOG_WARN("rdma connect: ESTABLISHED missing addr={} event={} status={} errno={}", addr,
                     event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        return false;
    }
    const bool valid_peer = event->param.conn.private_data != nullptr &&
                            parse_rdma_offer(std::span(static_cast<const uint8_t *>(event->param.conn.private_data),
                                                       event->param.conn.private_data_len),
                                             &peer_offer) &&
                            validate_rdma_offer(peer_offer, local, expected_peer, true);
    rdma_ack_cm_event(event);
    if (!valid_peer) {
        return false;
    }
    return true;
}

std::shared_ptr<Connection> RdmaTransport::connect(const std::string &addr, int port,
                                                   const RdmaEndpointCapability *expected_peer)
{
    if (!ready() || addr.empty() || port <= 0 || port > 65535 ||
        (config_.require_tcp_preflight && expected_peer == nullptr) ||
        (expected_peer != nullptr &&
         (expected_peer->wire_layout != config_.wire_layout ||
          !validate_rdma_capability(*expected_peer, RDMA_PROTOCOL_VERSION, expected_peer->generation, "")))) {

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
    std::shared_ptr<Connection> connection;
    auto                        destroy_pending = [this, &connection](rdma_cm_id *id) {
        if (connection != nullptr)
            destroy_connection_qp(connection.get(), id);
        rdma_destroy_id(id);
    };
    rdma_cm_id *id = nullptr;
    if (rdma_create_id(setup_channel.get(), &id, this, RDMA_PS_TCP) != 0)
        return nullptr;
    auto pending = std::unique_ptr<rdma_cm_id, decltype(destroy_pending)>(id, destroy_pending);
    if (!resolve_active_route(id, setup_channel.get(), addr, port, config_.source_address))
        return nullptr;
    std::shared_ptr<ConnectionPools> pools;
    connection = create_active_connection(id, addr, pools, expected_peer);
    if (connection == nullptr)
        return nullptr;
    RdmaCmOffer peer_offer;
    if (!establish_active_connection(id, setup_channel.get(), addr, expected_peer, peer_offer))
        return nullptr;
    if (pools->recv == nullptr) {
        configure_peer(*pools, peer_offer);
        if (!create_connection_pools(*pools) || !prime_receive(connection)) {
            return nullptr;
        }
    }
    else if (config_.wire_layout == RdmaWireLayout::Contiguous) {
        configure_peer(*pools, peer_offer);
    }
    pools->qp_number = id->qp->qp_num;
    ibv_qp_attr receive_attributes{};
    receive_attributes.min_rnr_timer = config_.min_rnr_timer;
    if (ibv_modify_qp(id->qp, &receive_attributes, IBV_QP_MIN_RNR_TIMER) != 0) {
        return nullptr;
    }
    if (!publish_cm_connection(id, connection, std::move(pools))) {
        return nullptr;
    }
    pending.release();
    return connection;
}

} // namespace crowdb::rpc

#endif // CROWDB_RPC_HAVE_RDMA
