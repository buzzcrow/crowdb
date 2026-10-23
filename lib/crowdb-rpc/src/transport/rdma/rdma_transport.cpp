// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#include "crowdb-common/log.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "rdma_aggregate.h"
#include "rdma_channel.h"
#include "rdma_control_stream.h"
#include "rdma_cq.h"
#include "rdma_device.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <arpa/inet.h>
#    include <fcntl.h>
#    include <netinet/in.h>
#    include <poll.h>
#    include <sys/eventfd.h>
#    include <unistd.h>

#    include <algorithm>
#    include <cassert>
#    include <chrono>
#    include <cstdlib>
#    include <cstring>
#    include <random>
#    include <thread>

namespace crowdb::rpc
{

// ── RdmaTransport ─────────────────────────────────────────────────

RdmaTransport::RdmaTransport(RdmaConfig config, BufferPool *application_pool)
    : config_(std::move(config)),
      application_pool_(application_pool == nullptr ? &default_application_pool_ : application_pool)
{
    const bool valid_config = (!config_.use_environment || config_.apply_environment()) && config_.valid();
    if (!valid_config) {
        config_.cq_depth = 1;
        config_.cq_count = 1;
    }
    memory_stats_ = std::make_shared<RdmaMemoryStats>(valid_config ? config_.registration_budget() : 0);
    for (uint32_t index = 0; index < config_.cq_count; ++index) {
        auto queue           = std::make_unique<CompletionQueue>();
        queue->index         = index;
        queue->work_requests = std::make_unique<WorkRequest[]>(config_.cq_depth);
        queue->deferred.reserve(config_.cq_depth);
        queue->connections.reserve(config_.cq_depth / std::max(1U, config_.sq_depth + config_.posted_receive_target));
        queue->continuation_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        completion_queues_.push_back(std::move(queue));
    }
    repost_state_            = std::make_shared<RepostState>();
    repost_state_->transport = this;
    if (!valid_config) {
        CRB_LOG_WARN("rdma configuration: inconsistent queue depths, slot capacity, or arena budget; using TCP");
        return;
    }
    control_send_budget_ = std::make_shared<ControlStreamBudget>(config_.control_send_budget_bytes);
    control_receive_budget_ =
        std::make_shared<ControlStreamBudget>(config_.control_receive_budget_bytes, [state = repost_state_] {
            Operation operation(state);
            if (operation) {
                for (uint32_t index = 0; index < state->transport->completion_queues_.size(); ++index) {
                    state->transport->notify_queue(index);
                }
            }
        });
    if (config_.use_environment && config_.device.empty()) {
        if (const char *requested_device = std::getenv("CROWDB_RDMA_DEVICE"); requested_device != nullptr)
            config_.device = requested_device;
    }
    requested_device_ = config_.device;
    port_             = config_.port;
    if (config_.use_environment && config_.source_address.empty()) {
        if (const char *source = std::getenv("CROWDB_RDMA_SOURCE_ADDR"); source != nullptr)
            config_.source_address = source;
    }
    if (config_.use_environment && config_.gid.empty()) {
        if (const char *gid = std::getenv("CROWDB_RDMA_GID"); gid != nullptr)
            config_.gid = gid;
    }
    std::random_device random;
    local_capability_.protocol_version = RDMA_PROTOCOL_VERSION;
    local_capability_.generation       = (static_cast<uint64_t>(random()) << 32) | random();
    if (local_capability_.generation == 0)
        local_capability_.generation = 1;
    local_capability_.data_classes          = {RDMA_CONTROL_CAPACITY, RDMA_METADATA_CAPACITY, RDMA_AGGREGATE_CAPACITY,
                                               RDMA_DATAPATH_CAPACITY};
    local_capability_.receive_slot_capacity = config_.wire_slot_capacity;
    local_capability_.wire_layout           = config_.wire_layout;
    local_capability_.max_control_message_bytes = config_.max_control_message_bytes;
    local_capability_.receive_frame_limit       = config_.wire_layout == RdmaWireLayout::ControlData
                                                    ? (config_.aggregation_enabled ? config_.aggregation_max_frames : 1)
                                                    : RDMA_MAX_AGGREGATE_FRAMES;
    local_capability_.cm_address                = config_.source_address;

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
            capability_available_ =
                ibv_query_port(probe, port_, &attributes) == 0 && attributes.state == IBV_PORT_ACTIVE;
            local_capability_.device = ibv_get_device_name(selected);
            local_capability_.gid    = config_.gid;
            if (local_capability_.gid.empty()) {
                ibv_gid gid{};
                char    text[INET6_ADDRSTRLEN]{};
                if (ibv_query_gid(probe, port_, 0, &gid) == 0 &&
                    ::inet_ntop(AF_INET6, &gid, text, sizeof(text)) != nullptr)
                    local_capability_.gid = text;
            }
            ibv_close_device(probe);
        }
    }
    ibv_free_device_list(devices);
}

RdmaEndpointCapability RdmaTransport::local_capability() const
{
    auto capability           = local_capability_;
    capability.rdma_available = ready() && (capability.port == 0 || listen_active_.load(std::memory_order_acquire));
    return capability;
}

RdmaTransport::~RdmaTransport()
{
    RdmaTransport::shutdown();
    send_pool_.reset();
    source_pool_.reset();
    destroy_completion_queues();
    for (const auto &queue : completion_queues_) {
        if (queue->continuation_fd >= 0)
            ::close(queue->continuation_fd);
    }
    device_.reset();
    pd_      = nullptr;
    context_ = nullptr;
}

bool RdmaTransport::adopt_cm_context(struct ibv_context *context)
{
    if (context == nullptr)
        return false;
    std::scoped_lock lock(cm_mu_);
    if (context_ == context)
        return ready();

    // A transport uses one device context for its CQ pool and registered
    // arenas. Do not replace live QPs/registrations when another HCA appears.
    if (context_ != nullptr)
        return false;
    if (local_capability_.device != ibv_get_device_name(context->device))
        return false;
    auto device = RdmaDevice::retain(context, memory_stats_);
    if (device == nullptr)
        return false;
    if (!create_completion_queues(context)) {
        return false;
    }
    context_     = context;
    device_      = std::move(device);
    pd_          = device_->pd;
    send_pool_   = make_pool(config_.send_arena_bytes, config_.sq_depth);
    source_pool_ = make_pool(config_.send_arena_bytes, config_.sq_depth);
    if (send_pool_ == nullptr || source_pool_ == nullptr) {
        send_pool_.reset();
        source_pool_.reset();
        capability_available_.store(false, std::memory_order_release);
        return false;
    }
    producer_pool_.store(source_pool_.get(), std::memory_order_release);
    register_events(event_engine_);
    return ready();
}

std::unique_ptr<RdmaBufferPool> RdmaTransport::make_pool(uint64_t bytes, uint32_t buffers)
{
    auto reservation = device_->reserve_bytes(bytes);
    if (reservation == nullptr)
        return nullptr;
    auto pool = std::make_unique<RdmaBufferPool>(pd_, bytes, buffers, std::move(reservation));
    if (!pool->ready()) {
        memory_stats_->failures.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    return pool;
}

bool RdmaTransport::post_receive(Connection *conn, Buffer *slot)
{
    if (config_.wire_layout == RdmaWireLayout::ControlData)
        return post_direct_receive(conn, slot);
    Operation operation(repost_state_);
    if (!operation)
        return false;
    if (conn == nullptr || !conn->is_open() || slot == nullptr || slot->data == nullptr ||
        slot->capacity < config_.wire_slot_capacity) {
        return false;
    }
    auto *pools = pools_for(conn);
    if (pools == nullptr)
        return false;
    Operation posting(pools->posting_state);
    if (!posting || !conn->is_open() || conn->transport_handle == 0)
        return false;
    ibv_sge sge{};
    sge.addr               = reinterpret_cast<uintptr_t>(slot->data);
    sge.length             = slot->capacity;
    sge.lkey               = slot->registration_key;
    const bool slot_posted = slot->transition(BufferOwnership::Available, BufferOwnership::PostedReceive) ||
                             slot->transition(BufferOwnership::CallerOwned, BufferOwnership::PostedReceive);
    if (!slot_posted) {
        return false;
    }
    const bool retained = slot->ref_clone() != nullptr;
    if (!retained) {
        (void)slot->transition(BufferOwnership::PostedReceive, BufferOwnership::Available);
        return false;
    }
    WorkRequest *work = acquire_work(WorkKind::Receive, conn, slot);
    if (work == nullptr) {
        (void)slot->transition(BufferOwnership::PostedReceive, BufferOwnership::Available);
        slot->release();
        return false;
    }
    if (config_.receive_batch_size > 1) {
        return stage_receive(work, std::span(&sge, 1));
    }
    ibv_recv_wr wr{};
    wr.wr_id             = work->token.load(std::memory_order_acquire);
    wr.sg_list           = &sge;
    wr.num_sge           = 1;
    ibv_recv_wr *bad     = nullptr;
    int          post_rc = -1;
    stats_.receive_post_calls.fetch_add(1, std::memory_order_relaxed);
    if (slot_posted)
        post_rc = ibv_post_recv(reinterpret_cast<ibv_qp *>(conn->transport_handle), &wr, &bad);
    if (!slot_posted || post_rc != 0) {
        release_work(work);
        CRB_LOG_WARN("rdma receive: post failed rc={} slot_state={}", post_rc,
                     static_cast<unsigned>(slot->ownership.load(std::memory_order_relaxed)));
        if (!slot->transition(BufferOwnership::PostedReceive, BufferOwnership::Available))
            (void)slot->quarantine();
        slot->release(); // drop the completion's retained reference
        return false;
    }

    stats_.posted_receives.fetch_add(1, std::memory_order_relaxed);
    return true;
}

Buffer *RdmaTransport::register_buffer(Buffer *buf)
{
    Operation operation(repost_state_);
    if (!operation)
        return nullptr;
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
    listen_active_.store(false, std::memory_order_release);
    repost_state_->users.fetch_or(RepostState::STOPPED, std::memory_order_acq_rel);
    while ((repost_state_->users.load(std::memory_order_acquire) & ~RepostState::STOPPED) != 0)
        std::this_thread::yield();
    stop();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.shutdown_drain_ms);
    while (true) {
        bool pending_sends = false;
        for (const auto &queue : completion_queues_) {
            for (uint32_t index = 0; index < config_.cq_depth; ++index) {
                const auto    &work  = queue->work_requests[index];
                const uint64_t token = work.token.load(std::memory_order_acquire);
                pending_sends |= token != 0 && token != WORK_RESERVED && work.kind == WorkKind::Send;
            }
        }
        if (!pending_sends || std::chrono::steady_clock::now() >= deadline)
            break;
        (void)drain_cq(config_.poll_batch);
        std::this_thread::yield();
    }
    for (const auto &[id, connection] : cm_connections_) {
        (void)id;
        if (auto *pools = pools_for(connection.get()); pools != nullptr)
            request_disconnect(*pools);
        connection->close();
    }
    {
        std::scoped_lock lock(cm_mu_);
        for (rdma_cm_id *id : cm_ids_) {
            if (auto it = cm_pools_.find(id); it != cm_pools_.end()) {
                it->second->send->quarantine_inflight();
                it->second->recv->quarantine_inflight();
                cm_pools_.erase(it);
            }
            if (id->qp != nullptr)
                rdma_destroy_qp(id);
            if (auto it = cm_connections_.find(id); it != cm_connections_.end())
                it->second->transport_handle = 0;
            rdma_destroy_id(id);
        }
        cm_ids_.clear();
        cm_connections_.clear();
    }
    while (drain_cq(config_.poll_batch) > 0) {
    }
    cancel_work();
    for (const auto &queue : completion_queues_)
        queue->reserved_capacity.store(0, std::memory_order_release);
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

void RdmaTransport::start()
{
    if (running_.exchange(true, std::memory_order_acq_rel))
        return;
    if ((repost_state_->users.load(std::memory_order_acquire) & RepostState::STOPPED) != 0) {
        repost_state_            = std::make_shared<RepostState>();
        repost_state_->transport = this;
    }
    stats_.poller_count.store(0, std::memory_order_relaxed);
    start_pollers();
}

void RdmaTransport::stop()
{
    running_.store(false, std::memory_order_release);
    for (const auto &queue : completion_queues_)
        if (queue->engine != nullptr)
            queue->engine->notify_worker();
    for (const auto &queue : completion_queues_) {
        if (queue->worker.joinable())
            queue->worker.join();
        queue->managed.store(false, std::memory_order_release);
    }
    if (event_engine_ != nullptr)
        event_engine_->notify_worker();
}

int RdmaTransport::completion_fd(uint32_t index) const
{
    if (index >= completion_queues_.size() || completion_queues_[index]->channel == nullptr)
        return -1;
    return completion_queues_[index]->channel->fd;
}

int RdmaTransport::cm_event_fd() const
{
    return cm_channel_ == nullptr ? -1 : cm_channel_->fd;
}

} // namespace crowdb::rpc

#endif // CROWDB_RPC_HAVE_RDMA
