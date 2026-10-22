// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/socket_transport.h"

namespace crowdb::rpc
{
Buffer *SocketTransport::allocate_producer_buffer(uint32_t capacity, bool require_registered)
{
    if (capacity == 0 || capacity > RDMA_DATAPATH_CAPACITY)
        return nullptr;
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr) {
        auto *buffer = rdma_transport_->allocate_producer_buffer(capacity);
        if (buffer != nullptr || rdma_transport_->has_producer_pool() || require_registered)
            return buffer;
    }
#endif
    return require_registered ? nullptr : pool_->alloc(capacity);
}
} // namespace crowdb::rpc
