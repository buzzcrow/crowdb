// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"

namespace
{
crowdb::rpc::Buffer *allocation(crowdb_rpc_buffer_t handle)
{
    if (handle == nullptr || handle->buf == nullptr) {
        return nullptr;
    }
    auto *buffer = handle->buf->allocation_owner == nullptr ? handle->buf : handle->buf->allocation_owner;
    // External Bytes wrappers may retain a larger allocation than their view;
    // only their owner can supply that capacity. Do not guess from view length.
    if (buffer->allocation_capacity == 0 && buffer->free_cb != nullptr) {
        return nullptr;
    }
    return buffer;
}
} // namespace

extern "C" uint32_t crowdb_rpc_buffer_allocation_capacity(crowdb_rpc_buffer_t handle)
{
    const auto *buffer = allocation(handle);
    if (buffer == nullptr) {
        return 0;
    }
    return buffer->allocation_capacity == 0 ? buffer->capacity : buffer->allocation_capacity;
}

extern "C" uint64_t crowdb_rpc_buffer_allocation_id(crowdb_rpc_buffer_t handle)
{
    const auto *buffer = allocation(handle);
    if (buffer == nullptr) {
        return 0;
    }
    return static_cast<uint64_t>(
        reinterpret_cast<uintptr_t>(buffer->allocation_data == nullptr ? buffer->data : buffer->allocation_data));
}

extern "C" uint32_t crowdb_rpc_buffer_allocation_offset(crowdb_rpc_buffer_t handle)
{
    const auto *buffer = allocation(handle);
    if (buffer == nullptr) {
        return 0;
    }
    const auto begin = reinterpret_cast<uintptr_t>(buffer->payload_data());
    const auto view  = reinterpret_cast<uintptr_t>(handle->buf->payload_data());
    if (view < begin || view - begin > buffer->len) {
        return 0;
    }
    return static_cast<uint32_t>(view - begin);
}

extern "C" crowdb_rpc_buffer_t crowdb_rpc_buffer_allocation_ref(crowdb_rpc_buffer_t handle)
{
    try {
        auto *buffer = allocation(handle);
        if (buffer == nullptr || buffer->len == 0) {
            return nullptr;
        }
        auto result = std::make_unique<crowdb_rpc_buffer_s>();
        result->buf = buffer->slice(0, buffer->len);
        return result->buf == nullptr ? nullptr : result.release();
    }
    catch (...) {
        return nullptr;
    }
}
