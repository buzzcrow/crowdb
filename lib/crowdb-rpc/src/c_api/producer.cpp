// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"

namespace
{
crowdb::rpc::Buffer *writable_buffer(crowdb_rpc_buffer_t handle)
{
    auto *buffer = handle == nullptr ? nullptr : handle->buf;
    return buffer != nullptr && buffer->len == 0 && buffer->ref != nullptr &&
                   buffer->ref->load(std::memory_order_acquire) == 1 &&
                   buffer->ownership.load(std::memory_order_acquire) == crowdb::rpc::BufferOwnership::Available
             ? buffer
             : nullptr;
}
} // namespace

extern "C" uint8_t *crowdb_rpc_buffer_mutable_data(crowdb_rpc_buffer_t handle)
{
    auto *buffer = writable_buffer(handle);
    return buffer == nullptr ? nullptr : buffer->data;
}

extern "C" int crowdb_rpc_buffer_commit(crowdb_rpc_buffer_t handle, uint32_t initialized_length)
{
    auto *buffer = writable_buffer(handle);
    if (buffer == nullptr || initialized_length == 0 || initialized_length > buffer->capacity)
        return 0;
    buffer->len = initialized_length;
    return 1;
}

extern "C" crowdb_rpc_buffer_t crowdb_rpc_server_alloc_producer_buffer(crowdb_rpc_server_t server, uint32_t capacity,
                                                                       int require_registered)
{
    if (server == nullptr || server->server == nullptr || (require_registered != 0 && require_registered != 1))
        return nullptr;
    try {
        auto release = [](crowdb::rpc::Buffer *buffer) { buffer->release(); };
        std::unique_ptr<crowdb::rpc::Buffer, decltype(release)> buffer(
            server->server->transport()->allocate_producer_buffer(capacity, require_registered != 0), release);
        if (buffer == nullptr)
            return nullptr;
        auto *handle = new crowdb_rpc_buffer_s{buffer.get()};
        (void)buffer.release();
        return handle;
    }
    catch (...) {
        return nullptr;
    }
}
