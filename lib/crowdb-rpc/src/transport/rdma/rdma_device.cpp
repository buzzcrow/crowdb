// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_device.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include "crowdb-common/log.h"

#    include <algorithm>

namespace crowdb::rpc
{
std::shared_ptr<RdmaDevice> RdmaDevice::retain(ibv_context *context)
{
    auto owner     = std::make_shared<RdmaDevice>();
    int  count     = 0;
    owner->devices = rdma_get_devices(&count);
    if (owner->devices == nullptr ||
        std::find(owner->devices, owner->devices + count, context) == owner->devices + count)
        return nullptr;
    owner->pd = ibv_alloc_pd(context);
    return owner->pd == nullptr ? nullptr : owner;
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
