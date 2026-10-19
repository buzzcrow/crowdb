// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/transport.h"
#include "crowdb-rpc/transport/rdma/rdma_capability.h"

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

constexpr uint32_t RDMA_WIRE_SLOT_CAPACITY = RDMA_DATAPATH_CAPACITY;

struct RdmaConfig
{
    std::string device;
    uint8_t     port                  = 1;
    uint32_t    cq_depth              = 2048;
    uint32_t    buffer_region_bytes   = 64U << 20;
    uint32_t    buffer_ring_depth     = 1024;
    uint32_t    qp_max_send_wr       = 1024;
    uint32_t    qp_max_recv_wr       = 1024;
    uint32_t    qp_max_send_sge      = 2;
    uint32_t    qp_max_recv_sge      = 2;
    uint32_t    poll_batch           = 256;
    uint32_t    poll_empty_budget    = 256;
    uint8_t     retry_count           = 7;
    uint8_t     rnr_retry_count       = 7;
};

// RdmaBufferPool: allocates buffers from ibv_mr-registered memory.
// The pool pre-registers a large memory region and carves out buffers
// from it, avoiding per-buffer ibv_reg_mr calls (which are expensive).
// Every buffer is a slice of the single MR, so registration is bounded by the
// pool region and deregistration happens exactly once at pool shutdown.
class RdmaBufferPool : public BufferPool
{
  public:
    // mr_size: total registered memory size. max_buffers: pool capacity.
    RdmaBufferPool(struct ibv_pd *pd, uint32_t mr_size = 1 << 20, uint32_t max_buffers = 1024);
    ~RdmaBufferPool() override;

    Buffer *alloc(uint32_t capacity) override;
    void    recycle(Buffer *buf) override;

  private:
    struct ibv_pd *pd_;
    struct ibv_mr *mr_     = nullptr;
    void          *region_ = nullptr;
    uint32_t       mr_size_;
    uint32_t       max_buffers_;
    uint32_t       outstanding_ = 0;
    uint32_t       next_offset_ = 0;

    std::mutex                                          mu_;
    std::unordered_map<uint32_t, std::vector<Buffer *>> free_list_;
    std::vector<Buffer *>                               quarantine_;
    std::vector<Buffer *>                               all_buffers_;

    Buffer *alloc_fresh(uint32_t capacity);
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
        std::atomic<uint64_t> local_protection_errors{0};
        std::atomic<uint64_t> retry_exceeded_errors{0};
        std::atomic<uint64_t> remote_access_errors{0};
    };

    explicit RdmaTransport(RdmaConfig config = {});
    ~RdmaTransport() override;

    bool    submit(Connection *conn, OutFrame *frame) override;
    bool    post_receive(Connection *conn, Buffer *slot);
    Buffer *register_buffer(Buffer *buf) override;
    void    shutdown() override;

    // RDMA-specific: connect to a peer endpoint.
    // Returns a new Connection on success, nullptr on failure.
    std::shared_ptr<Connection> connect(const std::string &addr, int port);

    // RDMA-specific: listen on an endpoint (server side).
    bool listen(const std::string &addr, int port);

    // Accept one pending CM request and create an RC QP-backed connection.
    // The caller owns the returned logical connection and should register it
    // with the common RPC worker before submitting frames.
    std::shared_ptr<Connection> accept();

    // Enable completion handling. CQ draining is driven by the epoll worker.
    void              start();
    void              stop();
    [[nodiscard]] int completion_fd() const;
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
        return stats_;
    }

    Stats &stats()
    {
        return stats_;
    }

    [[nodiscard]] bool ready() const
    {
        return capability_available_;
    }

  private:
    RdmaConfig                                                           config_;
    bool                                                                  capability_available_ = false;
    struct ibv_context                                                  *context_      = nullptr;
    bool                                                                  owns_context_ = true;
    struct ibv_pd                                                       *pd_           = nullptr;
    struct ibv_comp_channel                                             *comp_channel_ = nullptr;
    struct ibv_cq                                                       *cq_           = nullptr;
    struct rdma_event_channel                                           *cm_channel_   = nullptr;
    struct rdma_cm_id                                                   *listen_id_    = nullptr;
    std::string                                                           requested_device_;
    uint8_t                                                              port_         = 1;
    std::vector<struct rdma_cm_id *>                                     cm_ids_;
    std::unordered_map<struct rdma_cm_id *, std::shared_ptr<Connection>> cm_connections_;
    SocketEngine                                                        *event_engine_ = nullptr;
    std::mutex                                                           cm_mu_;

    std::unique_ptr<RdmaBufferPool> send_pool_;
    std::unique_ptr<RdmaBufferPool> recv_pool_;
    ConnectionHandler               connection_handler_;

    std::atomic<bool> running_{false};

    bool  prime_receive(const std::shared_ptr<Connection> &connection);
    Stats stats_;

    struct ReceiveWork
    {
        Connection *conn;
        Buffer     *slot;
    };

    struct RepostContext
    {
        RdmaTransport *transport;
        Connection    *connection;
        Buffer        *slot;
    };

    struct ConnectionPools
    {
        std::unique_ptr<RdmaBufferPool> send;
        std::unique_ptr<RdmaBufferPool> recv;
    };

    std::unordered_map<struct rdma_cm_id *, std::unique_ptr<ConnectionPools>> cm_pools_;
    std::vector<std::unique_ptr<ConnectionPools>> retired_pools_;

    void        release_completed_frame(struct ibv_wc &wc);
    void        release_completed_receive(struct ibv_wc &wc);
    void        record_completion_error(const struct ibv_wc &wc);
    static void repost_frame_release(void *context, Buffer *data);
    bool        adopt_cm_context(struct ibv_context *context);
};

} // namespace crowdb::rpc

#endif // CROWDB_RPC_HAVE_RDMA
