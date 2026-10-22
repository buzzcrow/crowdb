// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include "crowdb-rpc/transport/socket_transport.h"

namespace crowdb::rpc
{
struct RdmaTransport::CompletionQueue
{
    uint32_t                       index = 0;
    std::atomic<uint32_t>          effective_depth{0};
    ibv_cq                        *cq              = nullptr;
    ibv_comp_channel              *channel         = nullptr;
    int                            continuation_fd = -1;
    std::unique_ptr<WorkRequest[]> work_requests;

    struct DeferredReceive
    {
        WorkRequest *work;
        ibv_wc       completion;
    };

    std::vector<DeferredReceive> deferred;

    struct ConnectionBinding
    {
        std::shared_ptr<Connection>      connection;
        std::shared_ptr<ConnectionPools> pools;
    };

    std::vector<ConnectionBinding> connections; // protected by CQ polling ownership
    size_t                         next_connection = 0;
    // Producers publish PENDING and clear IDLE atomically. Only the first
    // producer observing IDLE writes the wake fd; active traffic needs none.
    static constexpr uint32_t     IDLE    = 1;
    static constexpr uint32_t     PENDING = 2;
    std::atomic<uint32_t>         wake_state{0};
    std::unique_ptr<SocketEngine> engine;
    std::thread                   worker;
    std::atomic<bool>             managed{false};
    std::atomic<bool>             event_wakeup{true};
    std::atomic_flag              polling = ATOMIC_FLAG_INIT;
    std::atomic<uint32_t>         next_work{0};
    std::atomic<uint32_t>         generation{1};
    std::atomic<uint32_t>         reserved_capacity{0};
    std::atomic<uint64_t>         poll_calls{0};
    std::atomic<uint64_t>         empty_polls{0};
    std::atomic<uint64_t>         direct_sends{0};
    std::atomic<uint64_t>         direct_payload_bytes{0};
    std::atomic<uint64_t>         copied_payload_bytes{0};
};

} // namespace crowdb::rpc
#endif
