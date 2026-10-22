// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/transport.h"
#include "crowdb-rpc/transport/rdma/rdma_capability.h"
#include "crowdb-rpc/transport/rdma/rdma_config.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <infiniband/verbs.h>
#    include <rdma/rdma_cma.h>

#    include <atomic>
#    include <functional>
#    include <memory>
#    include <mutex>
#    include <string>
#    include <thread>
#    include <unordered_map>
#    include <vector>

namespace crowdb::rpc
{

class SocketEngine;
struct RdmaDevice;
struct RdmaMemoryStats;

constexpr uint32_t RDMA_WIRE_SLOT_CAPACITY = RDMA_DATAPATH_CAPACITY;

// RdmaBufferPool: allocates buffers from ibv_mr-registered memory.
// The pool pre-registers a large memory region and carves out buffers
// from it, avoiding per-buffer ibv_reg_mr calls (which are expensive).
// Every buffer is a slice of the single MR, so registration is bounded by the
// pool region. Leases retain the arena and its registration owner; the final
// release deregisters the MR before dropping the PD/context owner.
class RdmaBufferPool : public BufferPool
{
  public:
    // mr_size: total registered memory size. max_buffers: pool capacity.
    RdmaBufferPool(struct ibv_pd *pd, uint64_t mr_size = 1 << 20, uint32_t max_buffers = 1024,
                   std::shared_ptr<void> registration_owner = {});
    ~RdmaBufferPool() override;

    Buffer *alloc(uint32_t capacity) override;
    Buffer *borrow_range(const uint8_t *data, uint32_t size);
    void    recycle(Buffer *buf) override;

    [[nodiscard]] uint64_t quarantined_bytes() const;
    [[nodiscard]] bool     quiescent() const;
    [[nodiscard]] bool     ready() const;
    void                   quarantine_inflight();

  private:
    std::shared_ptr<BufferArena> arena_;
};

// RdmaTransport: RDMA transport using libibverbs + librdmacm.
// Implements the Transport interface with ibv_post_send for submit and
// ibv_poll_cq for the completion loop. Connection setup uses librdmacm.
class RdmaTransport : public Transport
{
  public:
    using ConnectionHandler = std::function<void(std::shared_ptr<Connection>)>;

    struct Stats
    {
        std::atomic<uint64_t> sends{0};
        std::atomic<uint64_t> receives{0};
        std::atomic<uint64_t> cq_errors{0};
        std::atomic<uint64_t> fallback_count{0};
        std::atomic<uint64_t> posted_receives{0};
        std::atomic<uint64_t> quarantined_bytes{0};
        std::atomic<uint64_t> cq_polls{0};
        std::atomic<uint64_t> cq_empty_polls{0};
        std::atomic<uint64_t> cm_disconnects{0};
        std::atomic<uint64_t> flush_errors{0};
        std::atomic<uint64_t> shutdown_flushes{0};
        std::atomic<uint64_t> local_protection_errors{0};
        std::atomic<uint64_t> retry_exceeded_errors{0};
        std::atomic<uint64_t> remote_access_errors{0};
        std::atomic<uint64_t> poller_count{0};
        std::atomic<uint64_t> cq_events{0};
        std::atomic<uint64_t> cq_wakeups{0};
        std::atomic<uint64_t> pending_deferred_receives{0};
        std::atomic<uint64_t> registered_memory_budget_bytes{0};
        std::atomic<uint64_t> registered_bytes{0};
        std::atomic<uint64_t> registered_bytes_high_water{0};
        std::atomic<uint64_t> registration_rejects{0};
        std::atomic<uint64_t> registration_failures{0};
        std::atomic<uint64_t> cm_attempts{0};
        std::atomic<uint64_t> preflight_attempts{0};
        std::atomic<uint64_t> preflight_failures{0};
        std::atomic<uint64_t> protocol_errors{0};
        std::atomic<uint64_t> rpc_frames_sent{0};
        std::atomic<uint64_t> rpc_frames_received{0};
        std::atomic<uint64_t> wire_bytes_sent{0};
        std::atomic<uint64_t> wire_bytes_received{0};
        std::atomic<uint64_t> submit_rejects{0};
        std::atomic<uint64_t> send_queue_high_water{0};
        std::atomic<uint64_t> send_errors{0};
        std::atomic<uint64_t> receive_post_errors{0};
        std::atomic<uint64_t> direct_sends{0};
        std::atomic<uint64_t> direct_payload_bytes{0};
        std::atomic<uint64_t> copied_payload_bytes{0};
    };

    explicit RdmaTransport(RdmaConfig config = {}, BufferPool *application_pool = nullptr);
    ~RdmaTransport() override;

    bool    submit(Connection *conn, OutFrame *frame) override;
    bool    post_receive(Connection *conn, Buffer *slot);
    Buffer *register_buffer(Buffer *buf) override;
    Buffer *allocate_producer_buffer(uint32_t capacity);

    [[nodiscard]] bool has_producer_pool() const
    {
        return producer_pool_.load(std::memory_order_acquire) != nullptr;
    }

    void shutdown() override;

    // RDMA-specific: connect to a peer endpoint.
    // Returns a new Connection on success, nullptr on failure.
    std::shared_ptr<Connection>          connect(const std::string &addr, int port,
                                                 const RdmaEndpointCapability *expected_peer = nullptr);
    [[nodiscard]] RdmaEndpointCapability local_capability() const;

    [[nodiscard]] const RdmaConfig &config() const
    {
        return config_;
    }

    [[nodiscard]] std::string status_json() const;

    [[nodiscard]] const std::string &source_address() const
    {
        return config_.source_address;
    }

    // RDMA-specific: listen on an endpoint (server side).
    bool listen(const std::string &addr, int port);

    // Accept one pending CM request and create an RC QP-backed connection.
    // The caller owns the returned logical connection and should register it
    // with the common RPC worker before submitting frames.
    std::shared_ptr<Connection> accept();

    // Start one dedicated polling/event worker per initialized CQ.
    void              start();
    void              stop();
    [[nodiscard]] int completion_fd(uint32_t index = 0) const;
    [[nodiscard]] int cm_event_fd() const;
    int               drain_cq(uint32_t max_completions = 0);
    void              register_events(SocketEngine *engine);
    int               handle_external_event(uint64_t tag, uint32_t max_completions = 0);

    void set_connection_handler(ConnectionHandler handler)
    {
        connection_handler_ = std::move(handler);
    }

    [[nodiscard]] const Stats &stats() const
    {
        refresh_poll_stats();
        return stats_;
    }

    Stats &stats()
    {
        refresh_poll_stats();
        return stats_;
    }

    [[nodiscard]] bool ready() const
    {
        return capability_available_.load(std::memory_order_acquire);
    }

  private:
#    ifdef CROWDB_RPC_TEST_UTIL
    friend struct TestRdmaTransport;
#    endif
    RdmaConfig                                                           config_;
    RdmaEndpointCapability                                               local_capability_;
    std::atomic<bool>                                                    listen_active_{false};
    std::atomic<bool>                                                    capability_available_{false};
    struct ibv_context                                                  *context_ = nullptr;
    std::shared_ptr<RdmaDevice>                                          device_;
    std::shared_ptr<RdmaMemoryStats>                                     memory_stats_;
    struct ibv_pd                                                       *pd_         = nullptr;
    struct rdma_event_channel                                           *cm_channel_ = nullptr;
    struct rdma_cm_id                                                   *listen_id_  = nullptr;
    std::string                                                          requested_device_;
    uint8_t                                                              port_ = 1;
    std::vector<struct rdma_cm_id *>                                     cm_ids_;
    std::unordered_map<struct rdma_cm_id *, std::shared_ptr<Connection>> cm_connections_;
    SocketEngine                                                        *event_engine_ = nullptr;
    std::mutex                                                           cm_mu_;

    SystemBufferPool                default_application_pool_;
    BufferPool                     *application_pool_;
    std::unique_ptr<RdmaBufferPool> send_pool_;
    std::atomic<RdmaBufferPool *>   producer_pool_{nullptr};
    std::unique_ptr<RdmaBufferPool> recv_pool_;
    ConnectionHandler               connection_handler_;

    std::atomic<bool> running_{false};
    std::atomic_flag  cm_polling_ = ATOMIC_FLAG_INIT;

    bool                            prime_receive(const std::shared_ptr<Connection> &connection);
    std::unique_ptr<RdmaBufferPool> make_pool(uint64_t bytes, uint32_t buffers);
    mutable Stats                   stats_;
    void                            refresh_poll_stats() const;

    enum class WorkKind : uint8_t { Send, Receive };
    struct ConnectionPools;
    bool create_connection_pools(ConnectionPools &pools);

    struct WorkRequest
    {
        std::atomic<uint64_t>            token{0};
        WorkKind                         kind;
        std::shared_ptr<Connection>      conn;
        std::atomic<Connection *>        connection_identity{nullptr};
        std::shared_ptr<ConnectionPools> pools;
        Buffer                          *slot           = nullptr;
        Buffer                          *prefix         = nullptr;
        OutFrame                        *frame          = nullptr;
        uint64_t                         request_id     = 0;
        uint64_t                         stream_id      = 0;
        uint64_t                         sequence       = 0;
        uint64_t                         logical_offset = 0;
    };

    static constexpr uint64_t WORK_RESERVED = UINT64_MAX;

    struct CompletionQueue;

    struct CqReservation
    {
        CompletionQueue *queue;
        uint32_t         capacity;
        bool             committed = false;

        CqReservation(CompletionQueue *owner, uint32_t entries) : queue(owner), capacity(entries)
        {
        }

        ~CqReservation();
        CqReservation(const CqReservation &)            = delete;
        CqReservation &operator=(const CqReservation &) = delete;
    };

    std::vector<std::unique_ptr<CompletionQueue>> completion_queues_;
    std::atomic<uint32_t>                         next_cq_{0};
    CompletionQueue                              *reserve_cq(uint32_t capacity);
    bool                                          create_completion_queues(ibv_context *context);
    void                                          destroy_completion_queues();
    int      drain_queue(uint32_t index, uint32_t max_completions, bool wait_for_owner = false);
    uint32_t process_deferred(CompletionQueue &queue, uint32_t max_completions);
    void     cancel_deferred(CompletionQueue &queue, Connection *connection);
    void     start_pollers();
    void     run_poller(CompletionQueue *queue);
    void     notify_queue(uint32_t index);
    void     install_connection_notifiers(const std::shared_ptr<Connection> &connection, uint32_t index);
    uint32_t flush_sends(CompletionQueue &queue, uint32_t budget);
    uint32_t flush_connection(Connection *connection, ConnectionPools &pools, uint32_t budget);
    uint32_t flush_direct_connection(Connection *connection, ConnectionPools &pools, uint32_t budget);
    bool     post_direct(Connection *connection, ConnectionPools &pools, Buffer *slot, Buffer *prefix, OutFrame *frame);
    bool     post_direct_receive(Connection *connection, Buffer *slot);
    void     retire_sends(CompletionQueue &queue, Connection *connection);
    bool     post_aggregate(Connection *connection, ConnectionPools &pools, Buffer *slot, OutFrame *frames,
                            uint32_t frame_count);
    static constexpr uint64_t CM_EVENT_TAG  = 1;
    static constexpr uint64_t CQ_EVENT_BASE = 2;

    WorkRequest            *acquire_work(WorkKind kind, Connection *conn, Buffer *slot, OutFrame *frame = nullptr,
                                         Buffer *prefix = nullptr);
    WorkRequest            *claim_work(uint64_t token);
    void                    release_work(WorkRequest *work);
    void                    dispatch_completion(const struct ibv_wc &wc);
    void                    cancel_work(Connection *connection = nullptr);
    void                    close_after_drain(Connection *connection);
    void                    destroy_connection_qp(Connection *connection, rdma_cm_id *id);
    static ConnectionPools *pools_for(Connection *connection);
    static void             set_connection_pools(Connection *connection, std::shared_ptr<ConnectionPools> pools);
    static void             request_disconnect(ConnectionPools &pools);
    bool                    publish_cm_connection(rdma_cm_id *id, const std::shared_ptr<Connection> &connection,
                                                  std::shared_ptr<ConnectionPools> pools);

    struct RepostState
    {
        static constexpr uint64_t STOPPED = uint64_t{1} << 63;
        std::atomic<uint64_t>     users{0};
        RdmaTransport            *transport = nullptr;
    };

    class Operation
    {
      public:
        explicit Operation(std::shared_ptr<RepostState> state) : state_(std::move(state))
        {
            uint64_t users = state_->users.load(std::memory_order_acquire);
            while ((users & RepostState::STOPPED) == 0 &&
                   !state_->users.compare_exchange_weak(users, users + 1, std::memory_order_acq_rel)) {
            }
            entered_ = (users & RepostState::STOPPED) == 0;
        }

        ~Operation()
        {
            if (entered_)
                state_->users.fetch_sub(1, std::memory_order_release);
        }

        explicit operator bool() const
        {
            return entered_;
        }

        Operation(const Operation &)            = delete;
        Operation &operator=(const Operation &) = delete;

      private:
        std::shared_ptr<RepostState> state_;
        bool                         entered_ = false;
    };

    std::shared_ptr<RepostState> repost_state_;

    struct ConnectionPools : std::enable_shared_from_this<ConnectionPools>
    {
        explicit ConnectionPools(uint32_t queue_capacity = 4096) : send_queue(queue_capacity)
        {
        }

        ~ConnectionPools();

        ibv_qp_cap                      effective_capacity{};
        std::shared_ptr<RepostState>    posting_state = std::make_shared<RepostState>();
        std::unique_ptr<RdmaBufferPool> send;
        std::unique_ptr<RdmaBufferPool> recv;
        std::unique_ptr<RdmaBufferPool> recv_control;
        uint32_t                        cq_index                   = 0;
        uint32_t                        reserved_capacity          = 0;
        uint32_t                        peer_receive_slot_capacity = 0;
        uint32_t                        peer_receive_frame_limit   = 1;
        uint32_t                        qp_number                  = 0;
        rdma_cm_id                     *cm_id                      = nullptr; // immutable after publication
        std::atomic<bool>               disconnect_requested{false};
        SendQueue                       send_queue;
        std::atomic<uint32_t>           queued_frames{0};
        OutFrame                       *pending_frame   = nullptr;
        uint32_t                        sends_in_flight = 0; // CQ owner only
    };

    struct RepostContext
    {
        std::shared_ptr<RepostState>     state;
        std::shared_ptr<Connection>      connection;
        std::shared_ptr<ConnectionPools> pools;
    };

    std::unordered_map<struct rdma_cm_id *, std::shared_ptr<ConnectionPools>> cm_pools_;

    void        release_completed_frame(WorkRequest *work, const struct ibv_wc &wc);
    void        release_completed_receive(WorkRequest *work, const struct ibv_wc &wc);
    void        release_completed_direct_receive(WorkRequest *work, const struct ibv_wc &wc);
    void        record_completion_error(const struct ibv_wc &wc, bool expected_flush = false);
    static void repost_frame_release(void *context, Buffer *slot);
    bool        adopt_cm_context(struct ibv_context *context);
};

} // namespace crowdb::rpc

#endif // CROWDB_RPC_HAVE_RDMA
