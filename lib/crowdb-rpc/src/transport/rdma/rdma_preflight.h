// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_capability.h"

#include <memory>

namespace crowdb::rpc
{
struct RdmaPreflight
{
    RdmaPreflight()                                         = default;
    RdmaPreflight(const RdmaPreflight &)                    = delete;
    RdmaPreflight         &operator=(const RdmaPreflight &) = delete;
    int                    fd                               = -1;
    RdmaEndpointCapability peer;
    ~RdmaPreflight();
    int release_fd();

    static std::unique_ptr<RdmaPreflight> connect(const std::string &address, int port,
                                                  const RdmaEndpointCapability &local,
                                                  const std::string &source_address, uint32_t timeout_ms = 2000);
};
} // namespace crowdb::rpc
