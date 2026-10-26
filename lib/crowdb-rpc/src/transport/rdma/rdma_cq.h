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
    static constexpr uint32_t SENDS_PER_ROUND          = 64;
    static constexpr uint32_t PREPARE_FRAMES_PER_ROUND = 256;
    static constexpr uint32_t PREPARE_BYTES_PER_ROUND  = 4U << 20;
    static constexpr uint32_t RECEIVE_FRAMES_PER_ROUND = 256;
    static constexpr uint32_t RECEIVE_BYTES_PER_ROUND  = 4U << 20;
    uint32_t                  prepare_byte_limit       = PREPARE_BYTES_PER_ROUND;
    uint32_t                  prepare_frames_left      = PREPARE_FRAMES_PER_ROUND;
    uint32_t                  prepare_bytes_left       = PREPARE_BYTES_PER_ROUND;

    bool preparation_exhausted = false;

    uint32_t receive_byte_limit       = RECEIVE_BYTES_PER_ROUND;
    uint32_t receive_frames_left      = RECEIVE_FRAMES_PER_ROUND;
    uint32_t receive_bytes_left       = RECEIVE_BYTES_PER_ROUND;
    bool     receive_budget_exhausted = false;

    bool can_receive(uint32_t bytes, uint32_t frames = 1)
    {
        const bool fits = frames <= receive_frames_left && bytes <= receive_bytes_left;
        receive_budget_exhausted |= !fits;
        return fits;
    }

    void charge_receive(uint32_t bytes, uint32_t frames = 1)
    {
        receive_bytes_left -= bytes;
        receive_frames_left -= frames;
    }

    bool can_prepare(uint32_t bytes, uint32_t frames = 1)
    {
        const bool fits = frames <= prepare_frames_left && bytes <= prepare_bytes_left;
        preparation_exhausted |= !fits;
        return fits;
    }

    void charge_preparation(uint32_t bytes, uint32_t frames = 1)
    {
        prepare_bytes_left -= bytes;
        prepare_frames_left -= frames;
    }

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
    uint64_t                     deferred_epoch = 0;
    std::atomic<int>             control_wait_ms{1000};

    struct ConnectionBinding
    {
        std::shared_ptr<Connection>      connection;
        std::shared_ptr<ConnectionPools> pools;
    };

    std::vector<ConnectionBinding> connections; // protected by CQ polling ownership
    size_t                         next_connection        = 0;
    size_t                         next_repost_connection = 0;
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
    std::atomic<uint64_t>         copied_aggregate_payload_bytes{0};
    std::atomic<uint64_t>         copied_staging_payload_bytes{0};
    std::atomic<uint64_t>         copied_framing_control_bytes{0};
};

} // namespace crowdb::rpc
#endif
