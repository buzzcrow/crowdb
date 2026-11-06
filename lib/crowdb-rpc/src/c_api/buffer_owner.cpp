// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/control_allocator_c_api.h"

namespace crowdb::rpc
{
crowdb_rpc_buffer_t wrap_owned_buffer(Buffer *buffer) noexcept
{
    if (buffer == nullptr) {
        return nullptr;
    }
    auto *handle = buffer->control_allocator.create<crowdb_rpc_buffer_s>();
    if (handle == nullptr) {
        buffer->release();
        return nullptr;
    }
    handle->buf = buffer;
    return handle;
}
} // namespace crowdb::rpc

extern "C" crowdb_rpc_buffer_t
crowdb_rpc_buffer_create_external_accounted(const uint8_t *data, uint32_t len, void (*free_cb)(void *), void *free_ctx,
                                            const crowdb_rpc_control_allocator_v1 *callbacks)
{
    if (data == nullptr || len == 0 || free_cb == nullptr) {
        return nullptr;
    }
    if (callbacks != nullptr && (callbacks->version != 1 || callbacks->claim == nullptr ||
                                 callbacks->record == nullptr || callbacks->release == nullptr)) {
        return nullptr;
    }
    crowdb::rpc::ControlAllocator allocator;
    if (callbacks != nullptr) {
        allocator.callbacks = *callbacks;
    }
    auto *buffer = allocator.create<crowdb::rpc::Buffer>();
    if (buffer == nullptr) {
        return nullptr;
    }
    auto *references = allocator.create<std::atomic<int32_t>>(1);
    if (references == nullptr) {
        allocator.destroy(buffer);
        return nullptr;
    }
    auto *handle = allocator.create<crowdb_rpc_buffer_s>();
    if (handle == nullptr) {
        allocator.destroy(references);
        allocator.destroy(buffer);
        return nullptr;
    }
    buffer->control_allocator = allocator;
    buffer->data              = const_cast<uint8_t *>(data);
    buffer->len               = len;
    buffer->capacity          = len;
    buffer->ref               = references;
    buffer->free_cb           = free_cb;
    buffer->free_ctx          = free_ctx;
    handle->buf               = buffer;
    return handle;
}
