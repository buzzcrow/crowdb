// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../../buffer_arena.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include "crowdb-common/log.h"

#    include <cstdlib>
#    include <memory>

namespace crowdb::rpc
{
namespace
{
struct RdmaRegion
{
    std::shared_ptr<void> device_owner;
    std::shared_ptr<void> allocation;
    ibv_mr               *mr = nullptr;

    ~RdmaRegion()
    {
        const int error = mr == nullptr ? 0 : ibv_dereg_mr(mr);
        if (error != 0)
            CRB_LOG_ERROR("rdma MR deregistration failed error={}", error);
    }
};

std::shared_ptr<BufferRegion> register_region(ibv_pd *pd, uint64_t size, std::shared_ptr<void> owner)
{
    if (pd == nullptr)
        return nullptr;
    auto region = BufferRegion::allocate(size, 0, 4096);
    if (region->data == nullptr)
        return nullptr;
    auto registration          = std::make_shared<RdmaRegion>();
    registration->device_owner = std::move(owner);
    registration->mr           = ibv_reg_mr(pd, region->data, size, IBV_ACCESS_LOCAL_WRITE);
    if (registration->mr == nullptr)
        return nullptr;
    registration->allocation    = std::move(region->owner);
    auto *mr                    = registration->mr;
    region->owner               = std::move(registration);
    region->registration_handle = reinterpret_cast<uintptr_t>(mr);
    region->registration_key    = mr->lkey;
    region->registration_id     = mr->lkey;
    return region;
}
} // namespace

RdmaBufferPool::RdmaBufferPool(ibv_pd *pd, uint64_t mr_size, uint32_t max_buffers,
                               std::shared_ptr<void> registration_owner)
    : arena_(
          std::make_shared<BufferArena>(register_region(pd, mr_size, std::move(registration_owner)), max_buffers, 4096))
{
}

RdmaBufferPool::~RdmaBufferPool() = default;

Buffer *RdmaBufferPool::alloc(uint32_t capacity)
{
    return arena_->alloc(capacity);
}

Buffer *RdmaBufferPool::borrow_range(const uint8_t *data, uint32_t size)
{
    return arena_->borrow_range(data, size);
}

void RdmaBufferPool::recycle(Buffer *buffer)
{
    arena_->recycle(buffer);
}

bool RdmaBufferPool::quiescent() const
{
    return arena_->outstanding.load(std::memory_order_acquire) == 0;
}

bool RdmaBufferPool::ready() const
{
    return arena_->region != nullptr && arena_->region->registration_handle != 0;
}

uint64_t RdmaBufferPool::quarantined_bytes() const
{
    return arena_->quarantined.load(std::memory_order_relaxed);
}

void RdmaBufferPool::quarantine_inflight()
{
    arena_->quarantine_inflight();
}

} // namespace crowdb::rpc
#endif
