// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#ifdef CROWDB_RPC_HAVE_RDMA
#    include <infiniband/verbs.h>
#    include <rdma/rdma_cma.h>

#    include <memory>

namespace crowdb::rpc
{
struct RdmaDevice
{
    ibv_context **devices = nullptr;
    ibv_pd       *pd      = nullptr;

    static std::shared_ptr<RdmaDevice> retain(ibv_context *context);
    ~RdmaDevice();
};
} // namespace crowdb::rpc
#endif
