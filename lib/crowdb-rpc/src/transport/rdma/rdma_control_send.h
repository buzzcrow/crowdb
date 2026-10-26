// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "rdma_control_stream.h"

#ifdef CROWDB_RPC_HAVE_RDMA
namespace crowdb::rpc
{
struct RdmaTransport::ControlSendState
{
    OutFrame       *frame = nullptr;
    ControlFragment fragment;
    ~ControlSendState();
};
} // namespace crowdb::rpc
#endif
