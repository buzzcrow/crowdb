// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_device.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include "crowdb-common/log.h"

#    include <algorithm>

namespace crowdb::rpc
{
std::shared_ptr<RdmaDevice> RdmaDevice::retain(ibv_context *context, std::shared_ptr<RdmaMemoryStats> memory)
{
    auto owner     = std::make_shared<RdmaDevice>();
    owner->memory  = std::move(memory);
    int count      = 0;
    owner->devices = rdma_get_devices(&count);
    if (owner->devices == nullptr ||
        std::find(owner->devices, owner->devices + count, context) == owner->devices + count)
        return nullptr;
    owner->pd = ibv_alloc_pd(context);
    return owner->pd == nullptr ? nullptr : owner;
}

std::shared_ptr<void> RdmaDevice::reserve_bytes(uint64_t bytes)
{
    if (memory == nullptr)
        return nullptr;
    auto reservation    = std::make_shared<Reservation>();
    reservation->device = shared_from_this();
    uint64_t current    = memory->current.load(std::memory_order_relaxed);
    do {
        if (bytes == 0 || current > memory->budget || bytes > memory->budget - current) {
            memory->rejects.fetch_add(1, std::memory_order_relaxed);
            return nullptr;
        }
    } while (!memory->current.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel));
    reservation->bytes = bytes;
    uint64_t high      = memory->high_water.load(std::memory_order_relaxed);
    while (high < current + bytes &&
           !memory->high_water.compare_exchange_weak(high, current + bytes, std::memory_order_relaxed)) {
    }
    return reservation;
}

RdmaDevice::Reservation::~Reservation()
{
    if (bytes != 0)
        device->memory->current.fetch_sub(bytes, std::memory_order_release);
}

RdmaDevice::~RdmaDevice()
{
    if (pd != nullptr) {
        const int error = ibv_dealloc_pd(pd);
        if (error != 0)
            CRB_LOG_ERROR("rdma PD release failed error={}", error);
    }
    if (devices != nullptr)
        rdma_free_devices(devices);
}
} // namespace crowdb::rpc
#endif
