// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#include "crowdb-common/log.h"
#include "crowdb-rpc/transport/socket_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <arpa/inet.h>
#    include <fcntl.h>
#    include <netinet/in.h>
#    include <poll.h>
#    include <unistd.h>

#    include <algorithm>
#    include <cassert>
#    include <chrono>
#    include <cstdlib>
#    include <cstring>
#    include <thread>

namespace crowdb::rpc
{

namespace
{
void make_nonblocking(int fd)
{
    if (fd < 0)
        return;
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        (void)::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

bool wait_cm_event(rdma_event_channel *channel, rdma_cm_event **event, int timeout_ms)
{
    if (channel == nullptr || event == nullptr)
        return false;
    pollfd descriptor{channel->fd, POLLIN, 0};
    if (::poll(&descriptor, 1, timeout_ms) <= 0 || (descriptor.revents & POLLIN) == 0)
        return false;
    return rdma_get_cm_event(channel, event) == 0;
}
} // namespace

// ── RdmaBufferPool ────────────────────────────────────────────────

RdmaBufferPool::RdmaBufferPool(struct ibv_pd *pd, uint32_t mr_size, uint32_t max_buffers)
    : pd_(pd),
      mr_size_(mr_size),
      max_buffers_(max_buffers)
{
    if (pd_ == nullptr || mr_size_ == 0 || max_buffers_ == 0) {
        return;
    }
    if (posix_memalign(&region_, 64, mr_size_) != 0) {
        region_ = nullptr;
        return;
    }
    mr_ = ibv_reg_mr(pd_, region_, mr_size_,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_ATOMIC);
    if (mr_ == nullptr) {
        std::free(region_);
        region_ = nullptr;
        return;
    }
}

RdmaBufferPool::~RdmaBufferPool()
{
    std::scoped_lock lock(mu_);
    if (outstanding_ != 0) {
        // Keep the MR registered while a caller, disk operation, or device
        // completion still owns a slice. Process teardown reclaims it.
        return;
    }
    for (Buffer *buf : all_buffers_) {
        delete buf->ref;
        delete buf;
    }
    all_buffers_.clear();
    free_list_.clear();
    quarantine_.clear();
    if (mr_ != nullptr) {
        ibv_dereg_mr(mr_);
        mr_ = nullptr;
    }
    if (region_ != nullptr) {
        std::free(region_);
        region_ = nullptr;
    }
}

Buffer *RdmaBufferPool::alloc_fresh(uint32_t capacity)
{
    if (outstanding_ >= max_buffers_ || mr_ == nullptr || region_ == nullptr) {
        return nullptr;
    }
    const uint32_t aligned_offset = (next_offset_ + 63U) & ~63U;
    if (capacity > mr_size_ - aligned_offset) {
        return nullptr;
    }

    auto *buf     = new Buffer;
    buf->capacity = capacity;
    buf->type     = BufferType::Registered;
    buf->pool     = this;
    buf->ref      = new std::atomic<int32_t>(1);

    buf->data                = static_cast<uint8_t *>(region_) + aligned_offset;
    buf->len                 = 0;
    buf->allocation_data     = buf->data;
    buf->allocation_capacity = capacity;
    buf->registration_handle = reinterpret_cast<uintptr_t>(mr_);
    buf->registration_key    = mr_->lkey;
    next_offset_             = aligned_offset + capacity;
    ++outstanding_;
    all_buffers_.push_back(buf);
    return buf;
}

Buffer *RdmaBufferPool::alloc(uint32_t capacity)
{
    BufferCapacityClass cls;
    uint32_t            bucket;
    if (!classify_rdma_capacity(capacity, &cls, &bucket)) {
        return nullptr;
    }
    std::scoped_lock lock(mu_);
    auto             it = free_list_.find(bucket);
    if (it != free_list_.end() && !it->second.empty()) {
        Buffer *buf = it->second.back();
        it->second.pop_back();
        buf->reset_view();
        buf->len = 0;
        buf->ref->store(1, std::memory_order_relaxed);
        buf->capacity_class = cls;
        buf->ownership.store(BufferOwnership::Available, std::memory_order_release);
        ++outstanding_;
        return buf;
    }
    Buffer *buf = alloc_fresh(bucket);
    if (buf != nullptr) {
        buf->capacity_class  = cls;
        buf->registration_id = mr_ == nullptr ? 0 : mr_->lkey;
    }
    return buf;
}

void RdmaBufferPool::recycle(Buffer *buf)
{
    {
        std::scoped_lock lock(mu_);
        buf->reset_view();
        if (buf->ownership.load(std::memory_order_acquire) == BufferOwnership::Quarantined) {
            quarantine_.push_back(buf);
        }
        else {
            buf->len = 0;
            buf->ownership.store(BufferOwnership::Available, std::memory_order_release);
            free_list_[buf->capacity].push_back(buf);
        }
    }
    --outstanding_;
}

// ── RdmaTransport ─────────────────────────────────────────────────

RdmaTransport::RdmaTransport(RdmaConfig config) : config_(std::move(config))
{
    if (config_.device.empty()) {
        if (const char *requested_device = std::getenv("CROWDB_RDMA_DEVICE"); requested_device != nullptr)
            config_.device = requested_device;
    }
    requested_device_ = config_.device;
    if (const char *requested_port = std::getenv("CROWDB_RDMA_PORT"); requested_port != nullptr) {
        const int value = std::atoi(requested_port);
        if (value > 0 && value <= 255 && config.port == 1)
            config_.port = static_cast<uint8_t>(value);
    }
    port_ = config_.port;

    int          device_count = 0;
    ibv_device **devices      = ibv_get_device_list(&device_count);
    if (devices == nullptr || device_count == 0) {
        if (devices != nullptr)
            ibv_free_device_list(devices);
        return;
    }
    ibv_device *selected = nullptr;
    for (int i = 0; i < device_count; ++i) {
        if (requested_device_.empty() || requested_device_ == ibv_get_device_name(devices[i])) {
            selected = devices[i];
            break;
        }
    }
    if (selected != nullptr) {
        if (ibv_context *probe = ibv_open_device(selected); probe != nullptr) {
            ibv_port_attr attributes{};
            capability_available_ = ibv_query_port(probe, port_, &attributes) == 0 &&
                                    attributes.state == IBV_PORT_ACTIVE;
            ibv_close_device(probe);
        }
    }
    ibv_free_device_list(devices);
}

RdmaTransport::~RdmaTransport()
{
    RdmaTransport::shutdown();
    send_pool_.reset();
    recv_pool_.reset();
    if (cq_ != nullptr) {
        ibv_destroy_cq(cq_);
        cq_ = nullptr;
    }
    if (comp_channel_ != nullptr) {
        ibv_destroy_comp_channel(comp_channel_);
        comp_channel_ = nullptr;
    }
    if (pd_ != nullptr) {
        ibv_dealloc_pd(pd_);
        pd_ = nullptr;
    }
    if (context_ != nullptr && owns_context_) {
        ibv_close_device(context_);
        context_ = nullptr;
    }
}

bool RdmaTransport::adopt_cm_context(struct ibv_context *context)
{
    if (context == nullptr)
        return false;
    if (context_ == context)
        return true;

    auto *new_pd = ibv_alloc_pd(context);
    if (new_pd == nullptr)
        return false;
    auto *new_channel = ibv_create_comp_channel(context);
    if (new_channel == nullptr) {
        ibv_dealloc_pd(new_pd);
        return false;
    }
    make_nonblocking(new_channel->fd);
    auto *new_cq = ibv_create_cq(context, static_cast<int>(config_.cq_depth), this, new_channel, 0);
    if (new_cq == nullptr || ibv_req_notify_cq(new_cq, 0) != 0) {
        if (new_cq != nullptr)
            ibv_destroy_cq(new_cq);
        ibv_destroy_comp_channel(new_channel);
        ibv_dealloc_pd(new_pd);
        return false;
    }
    auto new_send = std::make_unique<RdmaBufferPool>(new_pd, config_.buffer_region_bytes, config_.buffer_ring_depth);
    auto new_recv = std::make_unique<RdmaBufferPool>(new_pd, config_.buffer_region_bytes, config_.buffer_ring_depth);
    if (new_send == nullptr || new_recv == nullptr) {
        ibv_destroy_cq(new_cq);
        ibv_destroy_comp_channel(new_channel);
        ibv_dealloc_pd(new_pd);
        return false;
    }

    send_pool_.reset();
    recv_pool_.reset();
    if (cq_ != nullptr)
        ibv_destroy_cq(cq_);
    if (comp_channel_ != nullptr)
        ibv_destroy_comp_channel(comp_channel_);
    if (pd_ != nullptr)
        ibv_dealloc_pd(pd_);
    if (context_ != nullptr && owns_context_)
        ibv_close_device(context_);

    context_      = context;
    owns_context_ = false;
    pd_           = new_pd;
    comp_channel_ = new_channel;
    cq_           = new_cq;
    send_pool_    = std::move(new_send);
    recv_pool_    = std::move(new_recv);
    return true;
}

bool RdmaTransport::submit(Connection *conn, OutFrame *frame)
{
    if (conn == nullptr || frame == nullptr || cq_ == nullptr || frame->control == nullptr ||
        conn->transport_handle == 0) {
        return false;
    }
    if (frame->data_views() > 1) {
        return false;
    }
    auto *pools = static_cast<ConnectionPools *>(conn->user_data);
    if (pools == nullptr || pools->send == nullptr) {
        return false;
    }

    // TCP serializes the header and buffers independently. RDMA materializes
    // the complete wire message in one registered slot so the receiver can
    // post and recycle one contiguous buffer.
    Buffer        *source_control = frame->control;
    const uint32_t control_len    = source_control->len;
    if (source_control->data == nullptr || control_len > source_control->capacity ||
        control_len > RDMA_CONTROL_CAPACITY - HEADER_SIZE) {
        CRB_LOG_WARN("rdma send: control buffer exceeds wire capacity len={} capacity={}", control_len,
                     source_control->capacity);
        return false;
    }
    Buffer *slot = pools->send->alloc(RDMA_WIRE_SLOT_CAPACITY);
    if (slot == nullptr)
        return false;
    const uint32_t data_len = frame->data == nullptr ? 0 : frame->data->len;
    const uint64_t wire_len = static_cast<uint64_t>(HEADER_SIZE) + control_len + data_len;
    if (wire_len > slot->capacity) {
        slot->release();
        CRB_LOG_WARN("rdma send: message exceeds wire slot control_len={} data_len={} capacity={}", control_len,
                     data_len, slot->capacity);
        return false;
    }
    frame->header.msg_size  = control_len;
    frame->header.data_size = data_len;
    serialize_header(slot->data, frame->header);
    if (control_len != 0)
        std::memcpy(slot->data + HEADER_SIZE, source_control->data, control_len);
    if (data_len != 0) {
        if (frame->data->payload_data() == nullptr || data_len > frame->data->capacity) {
            slot->release();
            CRB_LOG_WARN("rdma send: data buffer exceeds capacity len={} capacity={}", data_len, frame->data->capacity);
            return false;
        }
        std::memcpy(slot->data + HEADER_SIZE + control_len, frame->data->payload_data(), data_len);
    }
    slot->len            = static_cast<uint32_t>(wire_len);
    slot->request_id     = source_control->request_id;
    slot->stream_id      = source_control->stream_id;
    slot->sequence       = source_control->sequence;
    slot->logical_offset = source_control->logical_offset;

    ibv_sge sge{};
    sge.addr   = reinterpret_cast<uintptr_t>(slot->data);
    sge.length = slot->len;
    sge.lkey   = slot->registration_key;
    ibv_send_wr wr{};
    wr.wr_id         = reinterpret_cast<uintptr_t>(frame);
    wr.sg_list       = &sge;
    wr.num_sge       = 1;
    wr.opcode        = IBV_WR_SEND;
    wr.send_flags    = IBV_SEND_SIGNALED;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(reinterpret_cast<ibv_qp *>(conn->transport_handle), &wr, &bad) != 0) {
        CRB_LOG_WARN("rdma send: post failed qp={} errno={}", reinterpret_cast<void *>(conn->transport_handle), errno);
        slot->release();
        return false;
    }

    source_control->release();
    if (frame->data != nullptr)
        frame->data->release();
    frame->control = slot;
    frame->data    = nullptr;

    stats_.sends.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool RdmaTransport::post_receive(Connection *conn, Buffer *slot)
{
    if (conn == nullptr || slot == nullptr || slot->data == nullptr || cq_ == nullptr || conn->transport_handle == 0 ||
        slot->capacity < RDMA_WIRE_SLOT_CAPACITY) {
        return false;
    }
    ibv_sge sge{};
    sge.addr   = reinterpret_cast<uintptr_t>(slot->data);
    sge.length = slot->capacity;
    sge.lkey   = slot->registration_key;
    auto work  = std::make_unique<ReceiveWork>(ReceiveWork{conn, slot});
    slot->ref_clone();
    ibv_recv_wr wr{};
    wr.wr_id                 = reinterpret_cast<uintptr_t>(work.get()) | 1U;
    wr.sg_list               = &sge;
    wr.num_sge               = 1;
    ibv_recv_wr *bad         = nullptr;
    const bool   slot_posted = slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive) ||
                               slot->transition(BufferOwnership::CallerOwned, BufferOwnership::PostedReceive);
    int          post_rc     = -1;
    if (slot_posted)
        post_rc = ibv_post_recv(reinterpret_cast<ibv_qp *>(conn->transport_handle), &wr, &bad);
    if (!slot_posted || post_rc != 0) {
        CRB_LOG_WARN("rdma receive: post failed rc={} slot_state={}", post_rc,
                     static_cast<unsigned>(slot->ownership.load(std::memory_order_relaxed)));
        slot->ownership.store(BufferOwnership::Available, std::memory_order_release);
        slot->release();
        return false;
    }

    stats_.posted_receives.fetch_add(1, std::memory_order_relaxed);
    work.release();
    return true;
}

Buffer *RdmaTransport::register_buffer(Buffer *buf)
{
    if (buf == nullptr || buf->type == BufferType::Registered) {
        return buf; // already registered
    }
    if (send_pool_ == nullptr || buf->len == 0) {
        return nullptr;
    }
    Buffer *registered = send_pool_->alloc(buf->len);
    if (registered == nullptr) {
        return nullptr;
    }
    std::memcpy(registered->data, buf->payload_data(), buf->len);
    registered->len            = buf->len;
    registered->request_id     = buf->request_id;
    registered->stream_id      = buf->stream_id;
    registered->sequence       = buf->sequence;
    registered->logical_offset = buf->logical_offset;
    return registered;
}

void RdmaTransport::shutdown()
{
    // A QP may still own SEND or flushed receive WRs after the worker stops.
    // Drain bounded CQ batches before destroying the IDs so provider teardown
    // never observes live completion objects.
    for (int attempt = 0; attempt < 4; ++attempt) {
        const int drained = drain_cq(config_.poll_batch);
        if (drained == 0)
            break;
    }
    stop();
    {
        std::scoped_lock lock(cm_mu_);
        for (rdma_cm_id *id : cm_ids_) {
            if (id->qp != nullptr)
                rdma_destroy_qp(id);
            rdma_destroy_id(id);
        }
        cm_ids_.clear();
        cm_connections_.clear();
        cm_pools_.clear();
    }
    if (listen_id_ != nullptr) {
        if (listen_id_->qp != nullptr)
            rdma_destroy_qp(listen_id_);
        rdma_destroy_id(listen_id_);
        listen_id_ = nullptr;
    }
    if (cm_channel_ != nullptr) {
        rdma_destroy_event_channel(cm_channel_);
        cm_channel_ = nullptr;
    }
}

std::shared_ptr<Connection> RdmaTransport::connect(const std::string &addr, int port)
{
    if (addr.empty() || port <= 0 || port > 65535) {

        CRB_LOG_WARN("rdma connect: transport unavailable pd={} cq={} addr={} port={}", static_cast<const void *>(pd_),
                     static_cast<const void *>(cq_), addr, port);
        return nullptr;
    }
    if (cm_channel_ == nullptr)
        cm_channel_ = rdma_create_event_channel();
    if (cm_channel_ == nullptr) {

        return nullptr;
    }
    make_nonblocking(cm_channel_->fd);
    rdma_cm_id *id = nullptr;
    if (rdma_create_id(cm_channel_, &id, this, RDMA_PS_TCP) != 0) {

        return nullptr;
    }
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
    if (const char *source_env = std::getenv("CROWDB_RDMA_SOURCE_ADDR"); source_env != nullptr && *source_env != '\0') {
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
    if (!wait_cm_event(cm_channel_, &event, 3000) || event == nullptr || event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {

        CRB_LOG_WARN("rdma connect: ADDR_RESOLVED missing addr={} event={} errno={}", addr,
                     event == nullptr ? -1 : event->event, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);
    if (rdma_resolve_route(id, 2000) != 0) {
        CRB_LOG_WARN("rdma connect: resolve_route failed addr={} errno={}", addr, errno);
        rdma_destroy_id(id);
        return nullptr;
    }
    if (!wait_cm_event(cm_channel_, &event, 3000) || event == nullptr || event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {

        CRB_LOG_WARN("rdma connect: ROUTE_RESOLVED missing addr={} event={} errno={}", addr,
                     event == nullptr ? -1 : event->event, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);

    if (!adopt_cm_context(id->verbs)) {
        rdma_destroy_id(id);
        return nullptr;
    }

    ibv_device_attr device_attributes{};
    if (ibv_query_device(id->verbs, &device_attributes) != 0) {
        rdma_destroy_id(id);
        return nullptr;
    }
    ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq             = cq_;
    qp_attr.recv_cq             = cq_;
    qp_attr.qp_type             = IBV_QPT_RC;
    qp_attr.cap.max_send_wr     = std::min(config_.qp_max_send_wr, static_cast<uint32_t>(device_attributes.max_qp_wr));
    qp_attr.cap.max_recv_wr     = std::min(config_.qp_max_recv_wr, static_cast<uint32_t>(device_attributes.max_qp_wr));
    qp_attr.cap.max_send_sge    = std::min(config_.qp_max_send_sge, static_cast<uint32_t>(device_attributes.max_sge));
    qp_attr.cap.max_recv_sge    = std::min(config_.qp_max_recv_sge, static_cast<uint32_t>(device_attributes.max_sge));
    qp_attr.cap.max_inline_data = 0;
    qp_attr.sq_sig_all          = 1;
    qp_attr.qp_context          = id;

    if (rdma_create_qp(id, pd_, &qp_attr) != 0) {

        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_conn_param params{};
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = config_.retry_count;
    params.rnr_retry_count     = config_.rnr_retry_count;
    if (rdma_connect(id, &params) != 0) {

        CRB_LOG_WARN("rdma connect: rdma_connect failed addr={} errno={}", addr, errno);
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    if (!wait_cm_event(cm_channel_, &event, 5000) || event == nullptr || event->event != RDMA_CM_EVENT_ESTABLISHED) {

        CRB_LOG_WARN("rdma connect: ESTABLISHED missing addr={} event={} status={} errno={}", addr,
                     event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);
    auto pools  = std::make_unique<ConnectionPools>();
    pools->send = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    pools->recv = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    if (pools->send == nullptr || pools->recv == nullptr) {
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    auto connection       = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), addr, pools->recv.get());
    connection->user_data = pools.get();
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    if (!prime_receive(connection)) {

        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    {
        std::scoped_lock lock(cm_mu_);
        cm_ids_.push_back(id);
        cm_connections_.emplace(id, connection);
        cm_pools_.emplace(id, std::move(pools));
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
    make_nonblocking(cm_channel_->fd);
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
    return true;
}

std::shared_ptr<Connection> RdmaTransport::accept()
{
    if (listen_id_ == nullptr || cm_channel_ == nullptr)
        return nullptr;
    rdma_cm_event *event = nullptr;
    if (rdma_get_cm_event(cm_channel_, &event) != 0 || event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        if (event != nullptr) {
            rdma_cm_id *event_id = event->id;
            const int event_type = event->event;
            rdma_ack_cm_event(event);
            if (event_id != nullptr && event_id != listen_id_) {
                std::shared_ptr<Connection> closed_connection;
                {
                    std::scoped_lock lock(cm_mu_);
                    if (auto it = cm_connections_.find(event_id); it != cm_connections_.end()) {
                        closed_connection = it->second;
                        cm_connections_.erase(it);
                    }
                    if (auto it = cm_pools_.find(event_id); it != cm_pools_.end()) {
                        retired_pools_.push_back(std::move(it->second));
                        cm_pools_.erase(it);
                    }
                    cm_ids_.erase(std::remove(cm_ids_.begin(), cm_ids_.end(), event_id), cm_ids_.end());
                }
                if (closed_connection != nullptr)
                    closed_connection->close();
                if (event_type == RDMA_CM_EVENT_DISCONNECTED || event_type == RDMA_CM_EVENT_CONNECT_ERROR)
                    stats_.cm_disconnects.fetch_add(1, std::memory_order_relaxed);
                if (event_id->qp != nullptr)
                    rdma_destroy_qp(event_id);
                rdma_destroy_id(event_id);
            }
        }
        return nullptr;
    }
    rdma_cm_id *id = event->id;
    rdma_ack_cm_event(event);

    if (!adopt_cm_context(id->verbs)) {
        rdma_destroy_id(id);
        return nullptr;
    }

    ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq             = cq_;
    qp_attr.recv_cq             = cq_;
    qp_attr.qp_type             = IBV_QPT_RC;
    qp_attr.cap.max_send_wr     = config_.qp_max_send_wr;
    qp_attr.cap.max_recv_wr     = config_.qp_max_recv_wr;
    qp_attr.cap.max_send_sge    = config_.qp_max_send_sge;
    qp_attr.cap.max_recv_sge    = config_.qp_max_recv_sge;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.sq_sig_all          = 1;
    qp_attr.qp_context          = id;

    if (rdma_create_qp(id, pd_, &qp_attr) != 0) {
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_conn_param params{};
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = config_.retry_count;
    params.rnr_retry_count     = config_.rnr_retry_count;
    const int accept_rc        = rdma_accept(id, &params);

    if (accept_rc != 0) {
        CRB_LOG_WARN("rdma accept: rdma_accept failed errno={}", errno);
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    event              = nullptr;
    const int event_rc = wait_cm_event(cm_channel_, &event, 5000) ? 0 : -1;

    if (event_rc != 0 || event == nullptr || event->event != RDMA_CM_EVENT_ESTABLISHED) {
        CRB_LOG_WARN("rdma accept: ESTABLISHED missing event={} status={} errno={}",
                     event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);

    auto pools  = std::make_unique<ConnectionPools>();
    pools->send = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    pools->recv = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    if (pools->send == nullptr || pools->recv == nullptr) {
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    auto connection       = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), "rdma-peer", pools->recv.get());
    connection->user_data = pools.get();
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    if (!prime_receive(connection)) {
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }

    {
        std::scoped_lock lock(cm_mu_);
        cm_ids_.push_back(id);
        cm_connections_.emplace(id, connection);
        cm_pools_.emplace(id, std::move(pools));
    }
    return connection;
}

void RdmaTransport::start()
{
    running_.store(true, std::memory_order_relaxed);
}

void RdmaTransport::stop()
{
    running_.store(false, std::memory_order_release);
    if (event_engine_ != nullptr)
        event_engine_->notify_worker();
}

int RdmaTransport::completion_fd() const
{
    return comp_channel_ == nullptr ? -1 : comp_channel_->fd;
}

int RdmaTransport::cm_event_fd() const
{
    return cm_channel_ == nullptr ? -1 : cm_channel_->fd;
}

int RdmaTransport::drain_cq(uint32_t max_completions)
{
    if (max_completions == 0)
        max_completions = config_.poll_batch;
    stats_.cq_polls.fetch_add(1, std::memory_order_relaxed);
    uint32_t               drained          = 0;
    uint32_t               empty_polls      = 0;
    const uint32_t         kEmptyPollBudget = config_.poll_empty_budget;
    std::array<ibv_wc, 32> completions{};
    auto                   poll_one = [&](ibv_cq *cq) {
        while (drained < max_completions && cq != nullptr) {
            const int batch = ibv_poll_cq(cq, static_cast<int>(std::min<uint32_t>(32, max_completions - drained)),
                                          completions.data());
            if (batch < 0)
                break;
            if (batch == 0) {
                stats_.cq_empty_polls.fetch_add(1, std::memory_order_relaxed);
                if (++empty_polls >= kEmptyPollBudget)
                    break;
                continue;
            }
            empty_polls = 0;
            for (int i = 0; i < batch; ++i) {
                const auto opcode = completions[static_cast<size_t>(i)].opcode;
                const auto wr_id  = completions[static_cast<size_t>(i)].wr_id;
                if ((wr_id & uintptr_t{1}) != 0 || opcode == IBV_WC_RECV || opcode == IBV_WC_RECV_RDMA_WITH_IMM)
                    release_completed_receive(completions[static_cast<size_t>(i)]);
                else if (opcode == IBV_WC_SEND)
                    release_completed_frame(completions[static_cast<size_t>(i)]);
                else {
                }
            }
            drained += static_cast<uint32_t>(batch);
        }
    };
    if (comp_channel_ != nullptr && cq_ != nullptr) {
        ibv_cq *event_cq = nullptr;
        void   *context  = nullptr;
        if (ibv_get_cq_event(comp_channel_, &event_cq, &context) == 0) {
            ibv_ack_cq_events(event_cq, 1);
        }
        poll_one(cq_);
        (void)ibv_req_notify_cq(cq_, 0);
    }
    return static_cast<int>(drained);
}

void RdmaTransport::register_events(SocketEngine *engine)
{
    if (engine == nullptr)
        return;
    // A shared CQ has one completion channel. Register that channel with one
    // epoll engine only; registering the same fd in every engine causes
    // duplicate wakeups and lets different workers race the CQ event.
    if (event_engine_ != nullptr && event_engine_ != engine)
        return;
    event_engine_ = engine;
    if (completion_fd() >= 0)
        engine->add_external_fd(completion_fd(), 1);
    if (cm_event_fd() >= 0)
        engine->add_external_fd(cm_event_fd(), 2);
}

int RdmaTransport::handle_external_event(uint64_t tag, uint32_t max_completions)
{
    if (tag == 1)
        return drain_cq(max_completions);
    if (tag == 2) {
        int accepted = 0;
        while (true) {
            auto connection = accept();
            if (connection == nullptr)
                break;
            ++accepted;
            if (connection_handler_)
                connection_handler_(connection);
        }
        return accepted;
    }
    return 0;
}

bool RdmaTransport::prime_receive(const std::shared_ptr<Connection> &connection)
{
    if (connection == nullptr || connection->pool() == nullptr) {

        return false;
    }
    constexpr uint32_t kInitialReceiveDepth = 8;
    BufferPool        *pool                 = connection->pool();
    for (uint32_t i = 0; i < kInitialReceiveDepth; ++i) {
        Buffer *slot = pool->alloc(RDMA_WIRE_SLOT_CAPACITY);
        if (slot == nullptr) {
            return false;
        }
        if (!post_receive(connection.get(), slot)) {
            slot->release();
            return false;
        }
        // post_receive retains the work-request reference; the completion
        // owns the slot after the caller reference is released.
        slot->release();
    }
    return true;
}

void RdmaTransport::record_completion_error(const ibv_wc &wc)
{
    stats_.cq_errors.fetch_add(1, std::memory_order_relaxed);
    switch (wc.status) {
    case IBV_WC_WR_FLUSH_ERR:
        stats_.flush_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    case IBV_WC_LOC_PROT_ERR:
        stats_.local_protection_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    case IBV_WC_RETRY_EXC_ERR:
        stats_.retry_exceeded_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    case IBV_WC_REM_ACCESS_ERR:
        stats_.remote_access_errors.fetch_add(1, std::memory_order_relaxed);
        break;
    default:
        break;
    }
}

void RdmaTransport::release_completed_receive(ibv_wc &wc)
{

    auto *work = reinterpret_cast<ReceiveWork *>(wc.wr_id & ~uintptr_t{1});
    if (work == nullptr)
        return;
    auto quarantine_and_release = [&] {
        record_completion_error(wc);
        work->conn->close();
        (void)work->slot->quarantine();
        work->slot->release();
        delete work;
    };
    if (wc.status != IBV_WC_SUCCESS) {
        CRB_LOG_WARN("rdma receive: completion failed status={} vendor_err={} opcode={}",
                     static_cast<unsigned>(wc.status), static_cast<unsigned>(wc.vendor_err),
                     static_cast<unsigned>(wc.opcode));
        quarantine_and_release();
        return;
    }
    if (!work->slot->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight)) {
        quarantine_and_release();
        return;
    }
    const uint32_t slot_capacity = work->slot->capacity;
    if (slot_capacity < HEADER_SIZE) {
        CRB_LOG_WARN("rdma receive: slot capacity too small capacity={} header={}", slot_capacity, HEADER_SIZE);
        quarantine_and_release();
        return;
    }
    const Header   header         = parse_header(work->slot->data);
    const uint32_t control_end    = static_cast<uint32_t>(HEADER_SIZE) + header.msg_size;
    const uint64_t expected_bytes = static_cast<uint64_t>(control_end) + header.data_size;
    if (header.magic != MAGIC || header.msg_offset < HEADER_SIZE ||
        static_cast<uint32_t>(header.msg_offset) + header.msg_size > slot_capacity || expected_bytes > wc.byte_len ||
        expected_bytes > slot_capacity) {
        CRB_LOG_WARN(
            "rdma receive: invalid frame magic={} type={} msg_size={} data_size={} offset={} capacity={} byte_len={}",
            header.magic, header.msg_type, header.msg_size, header.data_size, header.msg_offset, slot_capacity,
            wc.byte_len);
        quarantine_and_release();
        return;
    }

    auto *frame   = new Frame;
    frame->header = header;
    if (header.msg_size != 0) {
        frame->control.assign(work->slot->data + header.msg_offset,
                              work->slot->data + header.msg_offset + header.msg_size);
        extract_control_fields(work->slot->data + header.msg_offset, header.msg_size, frame->request_id,
                               frame->rpc_create_nano, frame->response_create_nano);
    }
    work->slot->view_offset = control_end;
    work->slot->capacity    = slot_capacity - control_end;
    work->slot->len         = header.data_size;
    work->slot->ownership.store(BufferOwnership::CallerOwned, std::memory_order_release);
    frame->data_buf         = work->slot;
    Buffer *repost_slot     = work->slot->ref_clone();
    auto   *repost          = new RepostContext{this, work->conn, repost_slot};
    frame->release_callback = &RdmaTransport::repost_frame_release;
    frame->release_context  = repost;
    work->conn->on_frame(frame);
    stats_.receives.fetch_add(1, std::memory_order_relaxed);
    delete work;
}

void RdmaTransport::repost_frame_release(void *context, Buffer *)
{
    auto *repost = static_cast<RepostContext *>(context);
    if (repost == nullptr)
        return;
    repost->slot->reset_view();
    repost->slot->ownership.store(BufferOwnership::Available, std::memory_order_release);
    if (repost->transport != nullptr)
        (void)repost->transport->post_receive(repost->connection, repost->slot);
    repost->slot->release();
    delete repost;
}

void RdmaTransport::release_completed_frame(ibv_wc &wc)
{
    auto *frame = reinterpret_cast<OutFrame *>(wc.wr_id);
    if (frame == nullptr)
        return;

    if (wc.status != IBV_WC_SUCCESS) {
        record_completion_error(wc);
        // A failed SEND makes the stream uncertain; do not allow a TCP
        // suffix to be appended to this RDMA connection.
        if (frame->control != nullptr)
            (void)frame->control->quarantine();
        // The completion is no longer trustworthy; the pool keeps these
        // slices out of reuse until transport shutdown.
        if (frame->control != nullptr)
            (void)frame->control->quarantine();
        for (uint8_t i = 0; i < frame->data_views(); ++i)
            (void)frame->data_view(i)->quarantine();
    }
    if (frame->control != nullptr)
        frame->control->release();
    frame->release_data();
    delete frame;
}

} // namespace crowdb::rpc

#endif // CROWDB_RPC_HAVE_RDMA
