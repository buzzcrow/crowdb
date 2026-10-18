// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#include "crowdb-rpc/transport/socket_transport.h"
#include "crowdb-common/log.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <arpa/inet.h>
#    include <fcntl.h>
#    include <netinet/in.h>
#    include <poll.h>
#    include <unistd.h>

#    include <algorithm>
#    include <cassert>
#    include <cstdlib>
#    include <cstring>

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
}

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

RdmaTransport::RdmaTransport()
{
    int          device_count = 0;
    ibv_device **devices      = ibv_get_device_list(&device_count);
    if (devices == nullptr || device_count == 0) {
        std::fprintf(stderr, "rdma init: no ibv devices count=%d errno=%d\\n", device_count, errno);
        if (devices != nullptr) {
            ibv_free_device_list(devices);
        }
        return;
    }
    const char *requested_device = std::getenv("CROWDB_RDMA_DEVICE");
    if (const char *requested_port = std::getenv("CROWDB_RDMA_PORT"); requested_port != nullptr) {
        const int value = std::atoi(requested_port);
        if (value > 0 && value <= 255)
            port_ = static_cast<uint8_t>(value);
    }
    ibv_device *selected = devices[0];
    if (requested_device != nullptr) {
        selected = nullptr;
        for (int i = 0; i < device_count; ++i) {
            if (std::strcmp(ibv_get_device_name(devices[i]), requested_device) == 0) {
                selected = devices[i];
                break;
            }
        }
        if (selected == nullptr) {
            std::fprintf(stderr, "rdma init: requested device not found name=%s count=%d\\n", requested_device,
                         device_count);
            ibv_free_device_list(devices);
            return;
        }
    }
    context_ = ibv_open_device(selected);
    ibv_free_device_list(devices);
    if (context_ == nullptr) {
        std::fprintf(stderr, "rdma init: ibv_open_device failed name=%s errno=%d\\n",
                     requested_device == nullptr ? "default" : requested_device, errno);
        return;
    }
    ibv_port_attr port_attributes{};
    if (ibv_query_port(context_, port_, &port_attributes) != 0 || port_attributes.state != IBV_PORT_ACTIVE) {
        std::fprintf(stderr, "rdma init: port unavailable port=%u state=%u errno=%d\\n", port_,
                     port_attributes.state, errno);
        ibv_close_device(context_);
        context_ = nullptr;
        return;
    }
    pd_ = ibv_alloc_pd(context_);
    if (pd_ == nullptr) {
        std::fprintf(stderr, "rdma init: ibv_alloc_pd failed errno=%d\\n", errno);
        ibv_close_device(context_);
        context_ = nullptr;
        return;
    }
    comp_channel_ = ibv_create_comp_channel(context_);
    if (comp_channel_ == nullptr) {
        std::fprintf(stderr, "rdma init: ibv_create_comp_channel failed errno=%d\\n", errno);
        ibv_dealloc_pd(pd_);
        pd_ = nullptr;
        ibv_close_device(context_);
        context_ = nullptr;
        return;
    }
    const int channel_flags = ::fcntl(comp_channel_->fd, F_GETFL, 0);
    if (channel_flags >= 0)
        ::fcntl(comp_channel_->fd, F_SETFL, channel_flags | O_NONBLOCK);
    cq_ = ibv_create_cq(context_, 2048, this, comp_channel_, 0);
    if (cq_ == nullptr || ibv_req_notify_cq(cq_, 0) != 0) {
        std::fprintf(stderr, "rdma init: CQ setup failed cq=%p errno=%d\\n", static_cast<void *>(cq_), errno);
        if (cq_ != nullptr)
            ibv_destroy_cq(cq_);
        cq_ = nullptr;
        ibv_destroy_comp_channel(comp_channel_);
        comp_channel_ = nullptr;
        ibv_dealloc_pd(pd_);
        pd_ = nullptr;
        ibv_close_device(context_);
        context_ = nullptr;
        return;
    }
    constexpr uint32_t pool_bytes = 64U << 20;
    send_pool_                    = std::make_unique<RdmaBufferPool>(pd_, pool_bytes, 1024);
    recv_pool_                    = std::make_unique<RdmaBufferPool>(pd_, pool_bytes, 1024);
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
    if (context_ != nullptr) {
        ibv_close_device(context_);
        context_ = nullptr;
    }
}

bool RdmaTransport::submit(Connection *conn, OutFrame *frame)
{
    if (conn == nullptr || frame == nullptr || cq_ == nullptr || frame->control == nullptr ||
        conn->transport_handle == 0) {
        return false;
    }
    auto *pools = static_cast<ConnectionPools *>(conn->user_data);
    if (pools != nullptr && pools->send != nullptr && frame->control->pool != pools->send.get()) {
        Buffer *copy = pools->send->alloc(frame->control->len);
        if (copy == nullptr)
            return false;
        std::memcpy(copy->data, frame->control->data, frame->control->len);
        copy->len = frame->control->len;
        copy->request_id = frame->control->request_id;
        copy->stream_id = frame->control->stream_id;
        copy->sequence = frame->control->sequence;
        copy->logical_offset = frame->control->logical_offset;
        frame->control->release();
        frame->control = copy;
    }
    RdmaFrameSges frame_sges;
    if (!build_rdma_frame_sges(frame->control, frame->data, &frame_sges)) {
        return false;
    }
    ibv_sge sges[2]{};
    for (uint8_t i = 0; i < frame_sges.count; ++i) {
        sges[i].addr   = frame_sges.entries[i].address;
        sges[i].length = frame_sges.entries[i].length;
        sges[i].lkey   = frame_sges.entries[i].lkey;
    }
    ibv_send_wr wr{};
    wr.wr_id         = reinterpret_cast<uintptr_t>(frame);
    wr.sg_list       = sges;
    wr.num_sge       = frame_sges.count;
    wr.opcode        = IBV_WR_SEND;
    wr.send_flags    = IBV_SEND_SIGNALED;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(reinterpret_cast<ibv_qp *>(conn->transport_handle), &wr, &bad) != 0) {
        std::fprintf(stderr, "rdma send: ibv_post_send failed qp=%p errno=%d\n",
                     reinterpret_cast<void *>(conn->transport_handle), errno);
        return false;
    }
    std::fprintf(stderr, "rdma send: posted qp=%p frame=%p\n",
                 reinterpret_cast<void *>(conn->transport_handle), static_cast<void *>(frame));
    stats_.sends.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool RdmaTransport::post_receive(Connection *conn, Buffer *control, Buffer *data)
{
    if (conn == nullptr || control == nullptr || control->data == nullptr || cq_ == nullptr ||
        conn->transport_handle == 0 || control->capacity < RDMA_CONTROL_CAPACITY) {
        return false;
    }
    ibv_sge sges[2]{};
    sges[0].addr   = reinterpret_cast<uintptr_t>(control->data);
    sges[0].length = control->capacity;
    sges[0].lkey   = control->registration_key;
    int num_sge    = 1;
    if (data != nullptr) {
        if (data->data == nullptr || data->capacity == 0)
            return false;
        sges[1].addr   = reinterpret_cast<uintptr_t>(data->data);
        sges[1].length = data->capacity;
        sges[1].lkey   = data->registration_key;
        num_sge        = 2;
    }
    auto work = std::make_unique<ReceiveWork>(ReceiveWork{conn, control, data});
    control->ref_clone();
    if (data != nullptr)
        data->ref_clone();
    ibv_recv_wr wr{};
    wr.wr_id                    = reinterpret_cast<uintptr_t>(work.get()) | 1U;
    wr.sg_list                  = sges;
    wr.num_sge                  = num_sge;
    ibv_recv_wr *bad            = nullptr;
    const bool   control_posted = control->transition(BufferOwnership::Available, BufferOwnership::PostedReceive);
    const bool   data_posted    = data == nullptr ||
                                  data->transition(BufferOwnership::Available, BufferOwnership::PostedReceive) ||
                                  data->transition(BufferOwnership::CallerOwned, BufferOwnership::PostedReceive);
    int post_rc = -1;
    if (control_posted && data_posted)
        post_rc = ibv_post_recv(reinterpret_cast<ibv_qp *>(conn->transport_handle), &wr, &bad);
    if (!control_posted || !data_posted || post_rc != 0) {
        std::fprintf(stderr, "rdma receive: ibv_post_recv failed qp=%p rc=%d errno=%d bad=%p control_state=%u data_state=%u\n",
                     reinterpret_cast<void *>(conn->transport_handle), post_rc, errno, static_cast<void *>(bad),
                     static_cast<unsigned>(control->ownership.load(std::memory_order_relaxed)),
                     data == nullptr ? 0U : static_cast<unsigned>(data->ownership.load(std::memory_order_relaxed)));
        control->ownership.store(BufferOwnership::Available, std::memory_order_release);
        if (data != nullptr)
            data->ownership.store(BufferOwnership::Available, std::memory_order_release);
        control->release();
        if (data != nullptr)
            data->release();
        return false;
    }
    std::fprintf(stderr, "rdma receive: posted qp=%p wr_id=%#lx\n",
                 reinterpret_cast<void *>(conn->transport_handle), static_cast<unsigned long>(wr.wr_id));
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
    std::memcpy(registered->data, buf->data, buf->len);
    registered->len            = buf->len;
    registered->request_id     = buf->request_id;
    registered->stream_id      = buf->stream_id;
    registered->sequence       = buf->sequence;
    registered->logical_offset = buf->logical_offset;
    return registered;
}

void RdmaTransport::shutdown()
{
    stop();
    {
        std::scoped_lock lock(cm_mu_);
        for (rdma_cm_id *id : cm_ids_) {
            if (id->qp != nullptr)
                rdma_destroy_qp(id);
            if (auto it = cm_cqs_.find(id); it != cm_cqs_.end()) {
                ibv_destroy_cq(it->second);
                cm_cqs_.erase(it);
            }
            if (auto it = cm_channels_.find(id); it != cm_channels_.end()) {
                if (event_engine_ != nullptr)
                    event_engine_->remove_external_fd(it->second->fd);
                ibv_destroy_comp_channel(it->second);
                cm_channels_.erase(it);
            }
            rdma_destroy_id(id);
        }
        cm_ids_.clear();
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
    if (pd_ == nullptr || cq_ == nullptr || addr.empty() || port <= 0 || port > 65535) {
        std::fprintf(stderr, "rdma connect: transport unavailable pd=%p cq=%p addr=%s port=%d\\n",
                     static_cast<void *>(pd_), static_cast<void *>(cq_), addr.c_str(), port);
        CRB_LOG_WARN("rdma connect: transport unavailable pd={} cq={} addr={} port={}",
                     static_cast<const void *>(pd_), static_cast<const void *>(cq_), addr, port);
        return nullptr;
    }
    if (cm_channel_ == nullptr)
        cm_channel_ = rdma_create_event_channel();
    if (cm_channel_ == nullptr) {
        std::fprintf(stderr, "rdma connect: create event channel failed errno=%d\\n", errno);
        return nullptr;
    }
    make_nonblocking(cm_channel_->fd);
    rdma_cm_id *id = nullptr;
    if (rdma_create_id(cm_channel_, &id, this, RDMA_PS_TCP) != 0) {
        std::fprintf(stderr, "rdma connect: create id failed errno=%d\\n", errno);
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
    sockaddr *source_address = nullptr;
    if (const char *source_env = std::getenv("CROWDB_RDMA_SOURCE_ADDR"); source_env != nullptr && *source_env != '\0') {
        source.sin_family = AF_INET;
        if (::inet_pton(AF_INET, source_env, &source.sin_addr) != 1) {
            CRB_LOG_WARN("rdma connect: invalid source address {}", source_env);
            rdma_destroy_id(id);
            return nullptr;
        }
        source_address = reinterpret_cast<sockaddr *>(&source);
        if (rdma_bind_addr(id, source_address) != 0) {
            std::fprintf(stderr, "rdma connect: bind source failed source=%s errno=%d\\n", source_env, errno);
            CRB_LOG_WARN("rdma connect: bind source failed source={} errno={}", source_env, errno);
            rdma_destroy_id(id);
            return nullptr;
        }
    }
    // When a source address was explicitly bound above, librdmacm expects a
    // null source here; passing it twice can fail immediately with EINVAL.
    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr *>(&destination), 2000) != 0) {
        std::fprintf(stderr, "rdma connect: resolve_addr failed addr=%s errno=%d\\n", addr.c_str(), errno);
        CRB_LOG_WARN("rdma connect: resolve_addr failed addr={} port={} errno={}", addr, port, errno);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_cm_event *event = nullptr;
    if (!wait_cm_event(cm_channel_, &event, 3000) || event == nullptr || event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        std::fprintf(stderr, "rdma connect: ADDR_RESOLVED missing event=%d errno=%d\n", event == nullptr ? -1 : event->event, errno);
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
        std::fprintf(stderr, "rdma connect: ROUTE_RESOLVED missing event=%d errno=%d\n", event == nullptr ? -1 : event->event, errno);
        CRB_LOG_WARN("rdma connect: ROUTE_RESOLVED missing addr={} event={} errno={}", addr,
                     event == nullptr ? -1 : event->event, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);
    std::fprintf(stderr, "rdma connect: creating connection CQ verbs=%p\n", static_cast<void *>(id->verbs));
    ibv_comp_channel *connection_channel = ibv_create_comp_channel(id->verbs);
    if (connection_channel != nullptr)
        make_nonblocking(connection_channel->fd);
    ibv_cq *connection_cq = connection_channel == nullptr
                                ? nullptr
                                : ibv_create_cq(id->verbs, 2048, nullptr, connection_channel, 0);
    std::fprintf(stderr, "rdma connect: connection CQ=%p errno=%d\n", static_cast<void *>(connection_cq), errno);
    if (connection_cq == nullptr) {
        std::fprintf(stderr, "rdma connect: create CM CQ failed errno=%d\n", errno);
        if (connection_channel != nullptr)
            ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    ibv_device_attr device_attributes{};
    if (ibv_query_device(id->verbs, &device_attributes) != 0) {
        std::fprintf(stderr, "rdma connect: query device failed errno=%d\n", errno);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq          = connection_cq;
    qp_attr.recv_cq          = connection_cq;
    qp_attr.qp_type          = IBV_QPT_RC;
    qp_attr.cap.max_send_wr  = std::min<uint32_t>(1024, device_attributes.max_qp_wr);
    qp_attr.cap.max_recv_wr  = std::min<uint32_t>(1024, device_attributes.max_qp_wr);
    qp_attr.cap.max_send_sge = std::min<uint32_t>(2, device_attributes.max_sge);
    qp_attr.cap.max_recv_sge = std::min<uint32_t>(2, device_attributes.max_sge);
    qp_attr.cap.max_inline_data = 0;
    qp_attr.sq_sig_all         = 1;
    qp_attr.qp_context         = id;
    std::fprintf(stderr, "rdma connect: qp caps send_wr=%u recv_wr=%u send_sge=%u recv_sge=%u max_wr=%u max_sge=%u verbs_match=%d\n",
                 qp_attr.cap.max_send_wr, qp_attr.cap.max_recv_wr, qp_attr.cap.max_send_sge,
                 qp_attr.cap.max_recv_sge, device_attributes.max_qp_wr, device_attributes.max_sge,
                 id->verbs == context_ ? 1 : 0);
    std::fprintf(stderr, "rdma connect: selected=%s transport=%s\n",
                 id->verbs == nullptr || id->verbs->device == nullptr ? "unknown" : ibv_get_device_name(id->verbs->device),
                 context_ == nullptr || context_->device == nullptr ? "unknown" : ibv_get_device_name(context_->device));
    std::fprintf(stderr, "rdma connect: creating QP id=%p pd=%p cq=%p\n", static_cast<void *>(id),
                 static_cast<void *>(id->pd), static_cast<void *>(connection_cq));
    if (rdma_create_qp(id, id->pd, &qp_attr) != 0) {
        std::fprintf(stderr, "rdma connect: create qp failed errno=%d\n", errno);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_conn_param params{};
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = 7;
    params.rnr_retry_count     = 7;
    if (rdma_connect(id, &params) != 0) {
        std::fprintf(stderr, "rdma connect: rdma_connect failed addr=%s errno=%d\n", addr.c_str(), errno);
        CRB_LOG_WARN("rdma connect: rdma_connect failed addr={} errno={}", addr, errno);
        rdma_destroy_qp(id);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    if (!wait_cm_event(cm_channel_, &event, 5000) || event == nullptr || event->event != RDMA_CM_EVENT_ESTABLISHED) {
        std::fprintf(stderr, "rdma connect: ESTABLISHED missing event=%d status=%d errno=%d\n",
                     event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
        CRB_LOG_WARN("rdma connect: ESTABLISHED missing addr={} event={} status={} errno={}", addr,
                     event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_qp(id);
        ibv_destroy_cq(connection_cq);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);
    auto pools = std::make_unique<ConnectionPools>();
    pools->send = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    pools->recv = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    if (pools->send == nullptr || pools->recv == nullptr) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    auto connection = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), addr, pools->recv.get());
    connection->user_data = pools.get();
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    if (!prime_receive(connection)) {
        std::fprintf(stderr, "rdma connect: prime_receive failed after ESTABLISHED errno=%d\n", errno);
        rdma_destroy_qp(id);
        rdma_destroy_id(id);
        return nullptr;
    }
    {
        std::scoped_lock lock(cm_mu_);
        cm_ids_.push_back(id);
        cm_cqs_.emplace(id, connection_cq);
        cm_channels_.emplace(id, connection_channel);
        cm_pools_.emplace(id, std::move(pools));
        if (event_engine_ != nullptr)
            event_engine_->add_external_fd(connection_channel->fd, 3);
    }
    return connection;
}

bool RdmaTransport::listen(const std::string &addr, int port)
{
    if (pd_ == nullptr || addr.empty() || port <= 0 || port > 65535 || listen_id_ != nullptr)
        return false;
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
    if (listen_id_ == nullptr || cm_channel_ == nullptr || cq_ == nullptr)
        return nullptr;
    rdma_cm_event *event = nullptr;
    if (rdma_get_cm_event(cm_channel_, &event) != 0 || event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        if (event != nullptr) {
            rdma_cm_id *event_id = event->id;
            rdma_ack_cm_event(event);
            if (event_id != nullptr && event_id != listen_id_) {
                if (event_id->qp != nullptr)
                    rdma_destroy_qp(event_id);
                rdma_destroy_id(event_id);
            }
        }
        return nullptr;
    }
    rdma_cm_id *id = event->id;
    rdma_ack_cm_event(event);
    std::fprintf(stderr, "rdma accept: creating connection CQ verbs=%p\n", static_cast<void *>(id->verbs));
    ibv_comp_channel *connection_channel = ibv_create_comp_channel(id->verbs);
    if (connection_channel != nullptr)
        make_nonblocking(connection_channel->fd);
    ibv_cq *connection_cq = connection_channel == nullptr
                                ? nullptr
                                : ibv_create_cq(id->verbs, 2048, nullptr, connection_channel, 0);
    std::fprintf(stderr, "rdma accept: connection CQ=%p errno=%d\n", static_cast<void *>(connection_cq), errno);
    if (connection_cq == nullptr) {
        std::fprintf(stderr, "rdma accept: create CM CQ failed errno=%d\n", errno);
        if (connection_channel != nullptr)
            ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq          = connection_cq;
    qp_attr.recv_cq          = connection_cq;
    qp_attr.qp_type          = IBV_QPT_RC;
    qp_attr.cap.max_send_wr  = 1024;
    qp_attr.cap.max_recv_wr  = 1024;
    qp_attr.cap.max_send_sge = 2;
    qp_attr.cap.max_recv_sge = 2;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.sq_sig_all         = 1;
    qp_attr.qp_context         = id;
    std::fprintf(stderr, "rdma accept: creating QP id=%p pd=%p cq=%p\n", static_cast<void *>(id),
                 static_cast<void *>(id->pd), static_cast<void *>(connection_cq));
    if (rdma_create_qp(id, id->pd, &qp_attr) != 0) {
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_conn_param params{};
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = 7;
    params.rnr_retry_count     = 7;
    const int accept_rc = rdma_accept(id, &params);
    std::fprintf(stderr, "rdma accept: rdma_accept rc=%d errno=%d\n", accept_rc, errno);
    if (accept_rc != 0) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    event = nullptr;
    const int event_rc = wait_cm_event(cm_channel_, &event, 5000) ? 0 : -1;
    std::fprintf(stderr, "rdma accept: completion event rc=%d event=%d status=%d errno=%d\n", event_rc,
                 event == nullptr ? -1 : event->event, event == nullptr ? 0 : event->status, errno);
    if (event_rc != 0 || event == nullptr ||
        event->event != RDMA_CM_EVENT_ESTABLISHED) {
        if (event != nullptr)
            rdma_ack_cm_event(event);
        rdma_destroy_qp(id);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    rdma_ack_cm_event(event);
    std::fprintf(stderr, "rdma accept: established, priming receive\n");
    auto pools = std::make_unique<ConnectionPools>();
    pools->send = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    pools->recv = std::make_unique<RdmaBufferPool>(id->pd, 64U << 20, 1024);
    if (pools->send == nullptr || pools->recv == nullptr) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    auto connection = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), "rdma-peer", pools->recv.get());
    connection->user_data = pools.get();
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    if (!prime_receive(connection)) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(connection_cq);
        ibv_destroy_comp_channel(connection_channel);
        rdma_destroy_id(id);
        return nullptr;
    }
    std::fprintf(stderr, "rdma accept: receive primed\n");
    {
        std::scoped_lock lock(cm_mu_);
        cm_ids_.push_back(id);
        cm_cqs_.emplace(id, connection_cq);
        cm_channels_.emplace(id, connection_channel);
        cm_pools_.emplace(id, std::move(pools));
        if (event_engine_ != nullptr)
            event_engine_->add_external_fd(connection_channel->fd, 3);
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
    if (!running_.load(std::memory_order_acquire))
        return 0;
    uint32_t               drained = 0;
    std::array<ibv_wc, 32> completions{};
    auto poll_one = [&](ibv_cq *cq) {
        while (drained < max_completions && cq != nullptr) {
            const int batch = ibv_poll_cq(cq, static_cast<int>(std::min<uint32_t>(32, max_completions - drained)),
                                          completions.data());
            if (batch <= 0)
                break;
            for (int i = 0; i < batch; ++i) {
                const auto opcode = completions[static_cast<size_t>(i)].opcode;
                const auto wr_id = completions[static_cast<size_t>(i)].wr_id;
                if ((wr_id & uintptr_t{1}) != 0 || opcode == IBV_WC_RECV || opcode == IBV_WC_RECV_RDMA_WITH_IMM)
                    release_completed_receive(completions[static_cast<size_t>(i)]);
                else if (opcode == IBV_WC_SEND)
                    release_completed_frame(completions[static_cast<size_t>(i)]);
                else {
                    std::fprintf(stderr, "rdma completion: unknown opcode=%u status=%u wr_id=%#lx\n",
                                 static_cast<unsigned>(opcode),
                                 static_cast<unsigned>(completions[static_cast<size_t>(i)].status),
                                 static_cast<unsigned long>(completions[static_cast<size_t>(i)].wr_id));
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
            ibv_req_notify_cq(event_cq, 0);
            poll_one(event_cq);
        }
    }
    {
        std::scoped_lock lock(cm_mu_);
        for (const auto &[id, cq] : cm_cqs_) {
            (void)id;
            poll_one(cq);
            if (drained >= max_completions)
                break;
        }
    }
    return static_cast<int>(drained);
}

void RdmaTransport::register_events(SocketEngine *engine)
{
    if (engine == nullptr)
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
    if (tag == 3) {
        {
            std::scoped_lock lock(cm_mu_);
            for (const auto &[id, channel] : cm_channels_) {
                (void)id;
                ibv_cq *event_cq = nullptr;
                void *context = nullptr;
                if (ibv_get_cq_event(channel, &event_cq, &context) == 0) {
                    ibv_ack_cq_events(event_cq, 1);
                    (void)ibv_req_notify_cq(event_cq, 0);
                }
            }
        }
        return drain_cq(max_completions);
    }
    return 0;
}

bool RdmaTransport::prime_receive(const std::shared_ptr<Connection> &connection)
{
    if (connection == nullptr || connection->pool() == nullptr)
    {
        std::fprintf(stderr, "rdma receive: missing connection/pool\\n");
        return false;
    }
    constexpr uint32_t kInitialReceiveDepth = 8;
    BufferPool *pool = connection->pool();
    for (uint32_t i = 0; i < kInitialReceiveDepth; ++i) {
        Buffer *control = pool->alloc(RDMA_CONTROL_CAPACITY);
        Buffer *data    = pool->alloc(1U << 20);
        if (control == nullptr || data == nullptr) {
            std::fprintf(stderr, "rdma receive: buffer allocation failed depth=%u control=%p data=%p\\n", i,
                         static_cast<void *>(control), static_cast<void *>(data));
            if (control != nullptr)
                control->release();
            if (data != nullptr)
                data->release();
            return false;
        }
        if (!post_receive(connection.get(), control, data)) {
            std::fprintf(stderr, "rdma receive: initial ibv_post_recv failed depth=%u errno=%d\\n", i, errno);
            control->release();
            data->release();
            return false;
        }
        // post_receive retains the work-request references; release the caller
        // references so the completion owns the pair until it is consumed.
        control->release();
        data->release();
    }
    return true;
}

void RdmaTransport::release_completed_receive(ibv_wc &wc)
{
    std::fprintf(stderr, "rdma receive completion: wr_id=%#lx status=%u opcode=%u byte_len=%u\n",
                 static_cast<unsigned long>(wc.wr_id), static_cast<unsigned>(wc.status),
                 static_cast<unsigned>(wc.opcode), wc.byte_len);
    auto *work = reinterpret_cast<ReceiveWork *>(wc.wr_id & ~uintptr_t{1});
    if (work == nullptr)
        return;
    auto quarantine_and_release = [&] {
        stats_.cq_errors.fetch_add(1, std::memory_order_relaxed);
        work->conn->close();
        (void)work->control->quarantine();
        if (work->data != nullptr)
            (void)work->data->quarantine();
        work->control->release();
        if (work->data != nullptr)
            work->data->release();
        delete work;
    };
    if (wc.status != IBV_WC_SUCCESS) {
        quarantine_and_release();
        return;
    }
    if (!work->control->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight) ||
        (work->data != nullptr &&
         !work->data->transition(BufferOwnership::PostedReceive, BufferOwnership::ReceiveInFlight))) {
        quarantine_and_release();
        return;
    }
    const uint32_t control_bytes = work->control->capacity;
    if (control_bytes < HEADER_SIZE) {
        quarantine_and_release();
        return;
    }
    const Header   header         = parse_header(work->control->data);
    const uint32_t control_end    = static_cast<uint32_t>(HEADER_SIZE) + header.msg_size;
    const uint64_t expected_bytes = static_cast<uint64_t>(control_end) + header.data_size;
    if (header.magic != MAGIC || header.msg_offset < HEADER_SIZE ||
        static_cast<uint32_t>(header.msg_offset) + header.msg_size > control_bytes || expected_bytes > wc.byte_len ||
        (header.data_size > 0 && (work->data == nullptr || header.data_size > work->data->capacity))) {
        quarantine_and_release();
        return;
    }

    work->control->len = control_end;
    if (work->data != nullptr)
        work->data->len = header.data_size;
    auto *frame   = new Frame;
    frame->header = header;
    if (header.msg_size != 0) {
        frame->control.assign(work->control->data + header.msg_offset,
                              work->control->data + header.msg_offset + header.msg_size);
        extract_control_fields(work->control->data + header.msg_offset, header.msg_size, frame->request_id,
                               frame->rpc_create_nano, frame->response_create_nano);
    }
    if (work->data != nullptr && header.data_size != 0) {
        if (!work->data->transition(BufferOwnership::ReceiveInFlight, BufferOwnership::CallerOwned)) {
            delete frame;
            quarantine_and_release();
            return;
        }
        frame->data_buf = work->data;
        work->data      = nullptr;
    }
    Buffer *repost_control  = work->control->ref_clone();
    Buffer *repost_data     = (header.data_size == 0 && work->data != nullptr) ? work->data->ref_clone() : nullptr;
    auto   *repost          = new RepostContext{this, work->conn, repost_control, repost_data};
    frame->release_callback = &RdmaTransport::repost_frame_release;
    frame->release_context  = repost;
    work->control->ownership.store(BufferOwnership::Available, std::memory_order_release);
    if (work->data != nullptr && header.data_size == 0)
        work->data->ownership.store(BufferOwnership::Available, std::memory_order_release);
    work->control->release();
    if (work->data != nullptr && header.data_size != 0)
        work->data = nullptr;
    if (work->data != nullptr)
        work->data->release();
    work->conn->on_frame(frame);
    stats_.receives.fetch_add(1, std::memory_order_relaxed);
    delete work;
}

void RdmaTransport::repost_frame_release(void *context, Buffer *data)
{
    auto *repost = static_cast<RepostContext *>(context);
    if (repost == nullptr)
        return;
    Buffer *receive_data = data != nullptr ? data : repost->data;
    if (repost->transport == nullptr ||
        !repost->transport->post_receive(repost->connection, repost->control, receive_data)) {
        repost->control->release();
        if (receive_data != nullptr && receive_data != data)
            receive_data->release();
    }
    else {
        // The callback owns the caller references while post_receive owns
        // the WR references.
        repost->control->release();
        if (receive_data != nullptr && receive_data != data)
            receive_data->release();
    }
    delete repost;
}

void RdmaTransport::release_completed_frame(ibv_wc &wc)
{
    auto *frame = reinterpret_cast<OutFrame *>(wc.wr_id);
    if (frame == nullptr)
        return;
    std::fprintf(stderr, "rdma send completion: wr_id=%#lx status=%u frame=%p control=%p\n",
                 static_cast<unsigned long>(wc.wr_id), static_cast<unsigned>(wc.status),
                 static_cast<void *>(frame), static_cast<void *>(frame->control));
    if (wc.status != IBV_WC_SUCCESS) {
        stats_.cq_errors.fetch_add(1, std::memory_order_relaxed);
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
