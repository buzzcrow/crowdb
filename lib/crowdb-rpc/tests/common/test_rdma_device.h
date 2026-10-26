// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA
namespace crowdb::rpc
{
struct TestRdmaDevice
{
    ibv_context                      *context        = nullptr;
    ibv_pd                           *pd             = nullptr;
    std::shared_ptr<std::atomic<int>> release_result = std::make_shared<std::atomic<int>>(-1);

    static std::shared_ptr<TestRdmaDevice> open()
    {
        auto   device  = std::make_shared<TestRdmaDevice>();
        int    count   = 0;
        auto **devices = ibv_get_device_list(&count);
        if (devices == nullptr)
            return nullptr;
        for (int index = 0; index < count && device->context == nullptr; ++index)
            device->context = ibv_open_device(devices[index]);
        ibv_free_device_list(devices);
        if (device->context == nullptr)
            return nullptr;
        device->pd = ibv_alloc_pd(device->context);
        return device->pd == nullptr ? nullptr : device;
    }

    ~TestRdmaDevice()
    {
        if (pd != nullptr)
            release_result->store(ibv_dealloc_pd(pd), std::memory_order_release);
        if (context != nullptr)
            ibv_close_device(context);
    }
};
} // namespace crowdb::rpc
#endif
