// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_arena.h"

#ifdef CROWDB_RPC_HAVE_RDMA
#    include "crowdb-common/log.h"

#    include <cassert>
#    include <cstdlib>

namespace crowdb::rpc
{
RdmaBufferPool::RdmaBufferPool(ibv_pd *pd, uint64_t mr_size, uint32_t max_buffers,
                               std::shared_ptr<void> registration_owner)
    : arena_(std::make_shared<Arena>(pd, mr_size, max_buffers, std::move(registration_owner)))
{
}

RdmaBufferPool::~RdmaBufferPool() = default;

Buffer *RdmaBufferPool::alloc(uint32_t capacity)
{
    return arena_->alloc(capacity);
}

void RdmaBufferPool::recycle(Buffer *buffer)
{
    arena_->recycle(buffer);
}

bool RdmaBufferPool::quiescent() const
{
    return arena_->outstanding.load(std::memory_order_acquire) == 0;
}

uint64_t RdmaBufferPool::quarantined_bytes() const
{
    return arena_->quarantined.load(std::memory_order_relaxed);
}

void RdmaBufferPool::quarantine_inflight()
{
    arena_->quarantine_inflight();
}

RdmaBufferPool::Arena::Arena(ibv_pd *pd, uint64_t size, uint32_t count, std::shared_ptr<void> owner)
    : registration_owner(std::move(owner)),
      bytes(size),
      capacity(count)
{
    if (pd == nullptr || size == 0 || count == 0 || count == UINT32_MAX)
        return;
    entries = std::make_unique<Entry[]>(count);
    if (posix_memalign(&region, 64, size) != 0) {
        region = nullptr;
        return;
    }
    mr = ibv_reg_mr(pd, region, size, IBV_ACCESS_LOCAL_WRITE);
    if (mr == nullptr) {
        std::free(region);
        region = nullptr;
    }
}

RdmaBufferPool::Arena::~Arena()
{
    assert(outstanding.load(std::memory_order_acquire) == 0);
    if (entries != nullptr) {
        for (uint32_t index = 0; index < capacity; ++index) {
            if (auto *buffer = entries[index].buffer.load(std::memory_order_relaxed); buffer != nullptr) {
                delete buffer->ref;
                delete buffer;
            }
        }
    }
    if (mr != nullptr) {
        const int error = ibv_dereg_mr(mr);
        if (error != 0)
            CRB_LOG_ERROR("rdma MR deregistration failed error={}", error);
    }
    std::free(region);
}

Buffer *RdmaBufferPool::Arena::fresh(uint32_t size)
{
    if (mr == nullptr)
        return nullptr;
    uint32_t index = pop_entry(unused_entries);
    if (index == UINT32_MAX) {
        index = next_entry.load(std::memory_order_relaxed);
        do {
            if (index >= capacity)
                return nullptr;
        } while (!next_entry.compare_exchange_weak(index, index + 1, std::memory_order_relaxed));
    }
    uint64_t offset = next_offset.load(std::memory_order_relaxed);
    uint64_t aligned;
    do {
        if (offset > bytes || offset > UINT64_MAX - 63U) {
            push_entry(unused_entries, index);
            return nullptr;
        }
        aligned = (offset + 63U) & ~uint64_t{63};
        if (aligned > bytes || size > bytes - aligned) {
            push_entry(unused_entries, index);
            return nullptr;
        }
    } while (!next_offset.compare_exchange_weak(offset, aligned + size, std::memory_order_relaxed));
    auto *buffer                = new Buffer;
    buffer->capacity            = size;
    buffer->type                = BufferType::Registered;
    buffer->pool                = this;
    buffer->pool_index          = index;
    buffer->ref                 = new std::atomic<int32_t>(1);
    buffer->data                = static_cast<uint8_t *>(region) + aligned;
    buffer->allocation_data     = buffer->data;
    buffer->allocation_capacity = size;
    buffer->registration_handle = reinterpret_cast<uintptr_t>(mr);
    buffer->registration_key    = mr->lkey;
    buffer->registration_id     = mr->lkey;
    entries[index].buffer.store(buffer, std::memory_order_release);
    return buffer;
}

Buffer *RdmaBufferPool::Arena::pop(size_t bucket)
{
    const auto index = pop_entry(free_heads[bucket]);
    return index == UINT32_MAX ? nullptr : entries[index].buffer.load(std::memory_order_acquire);
}

uint32_t RdmaBufferPool::Arena::pop_entry(std::atomic<uint64_t> &head)
{
    uint64_t current = head.load(std::memory_order_acquire);
    while (static_cast<uint32_t>(current) != 0) {
        const auto index       = static_cast<uint32_t>(current) - 1;
        const auto next        = entries[index].next.load(std::memory_order_relaxed);
        const auto replacement = ((current + (uint64_t{1} << 32)) & 0xffffffff00000000ULL) | next;
        if (head.compare_exchange_weak(current, replacement, std::memory_order_acq_rel))
            return index;
    }
    return UINT32_MAX;
}

void RdmaBufferPool::Arena::push(Buffer *buffer)
{
    push_entry(free_heads[static_cast<size_t>(buffer->capacity_class)], buffer->pool_index);
}

void RdmaBufferPool::Arena::push_entry(std::atomic<uint64_t> &head, uint32_t index)
{
    auto    &entry   = entries[index];
    uint64_t current = head.load(std::memory_order_relaxed);
    uint64_t replacement;
    do {
        entry.next.store(static_cast<uint32_t>(current), std::memory_order_relaxed);
        replacement = ((current + (uint64_t{1} << 32)) & 0xffffffff00000000ULL) | (index + 1);
    } while (!head.compare_exchange_weak(current, replacement, std::memory_order_release, std::memory_order_relaxed));
}

Buffer *RdmaBufferPool::Arena::alloc(uint32_t requested)
{
    BufferCapacityClass cls;
    uint32_t            size;
    if (!classify_rdma_capacity(requested, &cls, &size))
        return nullptr;
    auto *buffer = pop(static_cast<size_t>(cls));
    if (buffer == nullptr)
        buffer = fresh(size);
    if (buffer == nullptr)
        return nullptr;
    buffer->reset_view();
    buffer->len = 0;
    buffer->ref->store(1, std::memory_order_relaxed);
    buffer->ownership.store(BufferOwnership::Available, std::memory_order_release);
    buffer->capacity_class = cls;
    buffer->pool_owner     = shared_from_this();
    outstanding.fetch_add(1, std::memory_order_relaxed);
    return buffer;
}

void RdmaBufferPool::Arena::recycle(Buffer *buffer)
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

void RdmaBufferPool::Arena::quarantine_inflight()
{
    if (entries == nullptr)
        return;
    for (uint32_t index = 0; index < capacity; ++index) {
        auto *buffer = entries[index].buffer.load(std::memory_order_acquire);
        if (buffer == nullptr)
            continue;
        const auto state = buffer->ownership.load(std::memory_order_acquire);
        if (state != BufferOwnership::Available && state != BufferOwnership::Quarantined)
            (void)buffer->quarantine();
    }
}
} // namespace crowdb::rpc
#endif
