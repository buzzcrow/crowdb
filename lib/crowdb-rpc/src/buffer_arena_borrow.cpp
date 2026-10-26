// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "buffer_arena.h"

namespace crowdb::rpc
{
Buffer *BufferArena::borrow_owned_range(Buffer *source, const uint8_t *data, uint32_t size)
{
    if (source == nullptr || size == 0 || region == nullptr || region->registration_handle == 0) {
        return nullptr;
    }
    auto *root = source->allocation_owner == nullptr ? source : source->allocation_owner;
    if (root->pool_owner.get() != this || root->registration_handle != region->registration_handle ||
        root->ownership.load(std::memory_order_acquire) == BufferOwnership::Quarantined) {
        return nullptr;
    }
    const auto address = reinterpret_cast<uintptr_t>(data);
    const auto start   = reinterpret_cast<uintptr_t>(root->allocation_data);
    if (address < start || address - start > root->allocation_capacity ||
        size > root->allocation_capacity - (address - start)) {
        return nullptr;
    }
    // The immutable source pins this exact allocation, including detached
    // leases. Retain its canonical owner without scanning other descriptors.
    auto *view = root->slice(0, size);
    if (view != nullptr) {
        // Address is relative to allocation_data, independent of a canonical
        // receive owner's temporary payload offset.
        view->data = root->allocation_data + static_cast<uint32_t>(address - start);
    }
    return view;
}

Buffer *BufferArena::borrow_range(const uint8_t *data, uint32_t size)
{
    if (region == nullptr || region->registration_handle == 0 || entries_ == nullptr || size == 0) {
        return nullptr;
    }
    const auto address = reinterpret_cast<uintptr_t>(data);
    const auto begin   = reinterpret_cast<uintptr_t>(region->data);
    if (address < begin || size > bytes || address - begin > bytes - size) {
        return nullptr;
    }
    const auto count = next_entry_.load(std::memory_order_acquire);
    for (uint32_t index = 0; index < count; ++index) {
        auto *buffer = entries_[index].buffer.load(std::memory_order_acquire);
        if (buffer == nullptr) {
            continue;
        }
        const auto start = reinterpret_cast<uintptr_t>(buffer->allocation_data);
        if (address >= start && address - start <= buffer->allocation_capacity &&
            size <= buffer->allocation_capacity - (address - start)) {
            return borrow_owned_range(buffer, data, size);
        }
    }
    return nullptr;
}
} // namespace crowdb::rpc
