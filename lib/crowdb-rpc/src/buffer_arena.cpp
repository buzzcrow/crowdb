// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "buffer_arena.h"

#include <cassert>
#include <cstdlib>

namespace crowdb::rpc
{
std::shared_ptr<BufferRegion> BufferRegion::allocate(uint64_t bytes, uint32_t registration_id, uint32_t alignment)
{
    auto region             = std::make_shared<BufferRegion>();
    region->bytes           = bytes;
    region->registration_id = registration_id;
    void *memory            = nullptr;
    if (bytes != 0 && posix_memalign(&memory, alignment, bytes) == 0) {
        region->data  = static_cast<uint8_t *>(memory);
        region->owner = std::shared_ptr<void>(memory, std::free);
    }
    return region;
}

BufferArena::BufferArena(std::shared_ptr<BufferRegion> allocation, uint32_t count, uint32_t large_alignment)
    : region(std::move(allocation)),
      bytes(region == nullptr ? 0 : region->bytes),
      capacity(count),
      large_alignment_(large_alignment)
{
    if (region != nullptr && region->data != nullptr && count != 0 && count != UINT32_MAX)
        entries_ = std::make_unique<Entry[]>(count);
}

BufferArena::~BufferArena()
{
    assert(outstanding.load(std::memory_order_acquire) == 0);
    if (entries_ != nullptr) {
        for (uint32_t index = 0; index < capacity; ++index) {
            if (auto *buffer = entries_[index].buffer.load(std::memory_order_relaxed); buffer != nullptr) {
                delete buffer->ref;
                delete buffer;
            }
        }
    }
}

Buffer *BufferArena::fresh(uint32_t size)
{
    if (entries_ == nullptr)
        return nullptr;
    uint32_t index = pop_entry(unused_entries_);
    if (index == UINT32_MAX) {
        index = next_entry_.load(std::memory_order_relaxed);
        do {
            if (index >= capacity)
                return nullptr;
        } while (!next_entry_.compare_exchange_weak(index, index + 1, std::memory_order_relaxed));
    }
    uint64_t       offset = next_offset.load(std::memory_order_relaxed);
    uint64_t       aligned;
    const uint64_t mask = (size >= RDMA_METADATA_CAPACITY ? large_alignment_ : 64U) - 1U;
    do {
        if (offset > bytes || offset > UINT64_MAX - mask) {
            push_entry(unused_entries_, index);
            return nullptr;
        }
        aligned = (offset + mask) & ~mask;
        if (aligned > bytes || size > bytes - aligned) {
            push_entry(unused_entries_, index);
            return nullptr;
        }
    } while (!next_offset.compare_exchange_weak(offset, aligned + size, std::memory_order_relaxed));
    auto *buffer                = new Buffer;
    buffer->capacity            = size;
    buffer->type                = BufferType::Registered;
    buffer->pool                = this;
    buffer->pool_index          = index;
    buffer->ref                 = new std::atomic<int32_t>(1);
    buffer->data                = region->data + aligned;
    buffer->allocation_data     = buffer->data;
    buffer->allocation_capacity = size;
    buffer->registration_handle = region->registration_handle;
    buffer->registration_key    = region->registration_key;
    buffer->registration_id     = region->registration_id;
    entries_[index].buffer.store(buffer, std::memory_order_release);
    return buffer;
}

Buffer *BufferArena::pop(size_t bucket)
{
    const auto index = pop_entry(free_heads_[bucket]);
    return index == UINT32_MAX ? nullptr : entries_[index].buffer.load(std::memory_order_acquire);
}

uint32_t BufferArena::pop_entry(std::atomic<uint64_t> &head)
{
    uint64_t current = head.load(std::memory_order_acquire);
    while (static_cast<uint32_t>(current) != 0) {
        const auto index       = static_cast<uint32_t>(current) - 1;
        const auto next        = entries_[index].next.load(std::memory_order_relaxed);
        const auto replacement = ((current + (uint64_t{1} << 32)) & 0xffffffff00000000ULL) | next;
        if (head.compare_exchange_weak(current, replacement, std::memory_order_acq_rel))
            return index;
    }
    return UINT32_MAX;
}

void BufferArena::push(Buffer *buffer)
{
    push_entry(free_heads_[static_cast<size_t>(buffer->capacity_class)], buffer->pool_index);
}

void BufferArena::push_entry(std::atomic<uint64_t> &head, uint32_t index)
{
    auto    &entry   = entries_[index];
    uint64_t current = head.load(std::memory_order_relaxed);
    uint64_t replacement;
    do {
        entry.next.store(static_cast<uint32_t>(current), std::memory_order_relaxed);
        replacement = ((current + (uint64_t{1} << 32)) & 0xffffffff00000000ULL) | (index + 1);
    } while (!head.compare_exchange_weak(current, replacement, std::memory_order_release, std::memory_order_relaxed));
}

Buffer *BufferArena::alloc(uint32_t requested)
{
    BufferCapacityClass cls;
    uint32_t            size;
    if (!classify_rdma_capacity(requested, &cls, &size))
        return nullptr;
    auto *buffer = pop(static_cast<size_t>(cls));
    if (buffer == nullptr)
        buffer = fresh(size);
    if (buffer == nullptr) {
        for (size_t bucket = static_cast<size_t>(cls) + 1; bucket < free_heads_.size() && buffer == nullptr; ++bucket)
            buffer = pop(bucket);
    }
    if (buffer == nullptr)
        return nullptr;
    (void)classify_rdma_capacity(buffer->capacity, &cls, &size);
    buffer->reset_view();
    buffer->len = 0;
    buffer->ref->store(1, std::memory_order_relaxed);
    buffer->ownership.store(BufferOwnership::Available, std::memory_order_release);
    buffer->capacity_class = cls;
    buffer->pool_owner     = shared_from_this();
    outstanding.fetch_add(1, std::memory_order_relaxed);
    return buffer;
}

void BufferArena::recycle(Buffer *buffer)
{
    // Retain this receiver until after publication; another allocator may
    // immediately replace buffer->pool_owner after push.
    const auto owner = std::move(buffer->pool_owner);
    buffer->reset_view();
    const auto state = buffer->ownership.load(std::memory_order_acquire);
    if (state != BufferOwnership::Available && state != BufferOwnership::Quarantined)
        (void)buffer->quarantine();
    outstanding.fetch_sub(1, std::memory_order_release);
    if (buffer->ownership.load(std::memory_order_acquire) == BufferOwnership::Quarantined) {
        quarantined.fetch_add(buffer->capacity, std::memory_order_relaxed);
        return;
    }
    buffer->len = 0;
    push(buffer);
}

void BufferArena::quarantine_inflight()
{
    if (entries_ == nullptr)
        return;
    for (uint32_t index = 0; index < capacity; ++index) {
        auto *buffer = entries_[index].buffer.load(std::memory_order_acquire);
        if (buffer == nullptr)
            continue;
        const auto state = buffer->ownership.load(std::memory_order_acquire);
        if (state != BufferOwnership::Available && state != BufferOwnership::Quarantined)
            (void)buffer->quarantine();
    }
}

bool BufferArena::detach(Buffer *buffer)
{
    if (buffer == nullptr || buffer->pool != this || buffer->last_release_cb != nullptr || buffer->ref == nullptr ||
        buffer->ref->load(std::memory_order_acquire) != 1)
        return false;
    const auto state = buffer->ownership.load(std::memory_order_acquire);
    if (state != BufferOwnership::Available && state != BufferOwnership::CallerOwned)
        return false;
    auto context = std::make_unique<DetachedRelease>(DetachedRelease{this, buffer->pool_index, buffer});
    entries_[buffer->pool_index].buffer.store(nullptr, std::memory_order_release);
    buffer->pool     = nullptr;
    buffer->free_ctx = context.release();
    buffer->free_cb  = &BufferArena::release_detached;
    return true;
}

void BufferArena::release_detached(void *context)
{
    const auto release = std::unique_ptr<DetachedRelease>(static_cast<DetachedRelease *>(context));
    const auto state   = release->buffer->ownership.load(std::memory_order_acquire);
    if (state != BufferOwnership::Available && state != BufferOwnership::CallerOwned)
        release->arena->quarantined.fetch_add(release->buffer->allocation_capacity, std::memory_order_relaxed);
    release->arena->outstanding.fetch_sub(1, std::memory_order_release);
    release->arena->push_entry(release->arena->unused_entries_, release->index);
}

} // namespace crowdb::rpc
