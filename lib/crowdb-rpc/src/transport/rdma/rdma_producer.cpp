// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA
namespace crowdb::rpc
{
Buffer *RdmaTransport::allocate_producer_buffer(uint32_t capacity)
{
    Operation operation(repost_state_);
    if (!operation)
        return nullptr;
    auto *pool = producer_pool_.load(std::memory_order_acquire);
    return pool == nullptr ? nullptr : pool->alloc(capacity);
}
} // namespace crowdb::rpc
#endif
