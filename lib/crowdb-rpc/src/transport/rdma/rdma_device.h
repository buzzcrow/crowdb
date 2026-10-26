// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <infiniband/verbs.h>
#    include <rdma/rdma_cma.h>

#    include <atomic>
#    include <memory>

namespace crowdb::rpc
{
struct RdmaMemoryStats
{
    explicit RdmaMemoryStats(uint64_t bytes) : budget(bytes)
    {
    }

    const uint64_t        budget;
    std::atomic<uint64_t> current{0};
    std::atomic<uint64_t> high_water{0};
    std::atomic<uint64_t> rejects{0};
    std::atomic<uint64_t> failures{0};
};

struct RdmaDevice : std::enable_shared_from_this<RdmaDevice>
{
    ibv_context **devices = nullptr;
    ibv_pd       *pd      = nullptr;

    std::shared_ptr<RdmaMemoryStats>   memory;
    static std::shared_ptr<RdmaDevice> retain(ibv_context *context, std::shared_ptr<RdmaMemoryStats> memory);
    std::shared_ptr<void>              reserve_bytes(uint64_t bytes);
    ~RdmaDevice();

  private:
    struct Reservation
    {
        std::shared_ptr<RdmaDevice> device;
        uint64_t                    bytes = 0;
        ~Reservation();
    };
};
} // namespace crowdb::rpc
#endif
