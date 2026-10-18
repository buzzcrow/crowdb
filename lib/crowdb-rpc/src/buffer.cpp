// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/buffer.h"

#include <cassert>
#include <cstdlib>

namespace crowdb::rpc
{

bool classify_rdma_capacity(uint32_t requested, BufferCapacityClass *out, uint32_t *capacity)
{
    if (requested == 0 || out == nullptr || capacity == nullptr)
        return false;
    if (requested <= RDMA_CONTROL_CAPACITY) {
        *out      = BufferCapacityClass::Control;
        *capacity = RDMA_CONTROL_CAPACITY;
    }
    else if (requested <= RDMA_METADATA_CAPACITY) {
        *out      = BufferCapacityClass::Metadata;
        *capacity = RDMA_METADATA_CAPACITY;
    }
    else if (requested <= RDMA_AGGREGATE_CAPACITY) {
        *out      = BufferCapacityClass::Aggregate;
        *capacity = RDMA_AGGREGATE_CAPACITY;
    }
    else if (requested <= RDMA_DATAPATH_CAPACITY) {
        *out      = BufferCapacityClass::Datapath;
        *capacity = RDMA_DATAPATH_CAPACITY;
    }
    else
        return false;
    return true;
}

// ── Buffer ───────────────────────────────────────────────────────

void Buffer::write(const void *src, uint32_t n)
{
    assert(n <= capacity && "Buffer::write exceeds capacity");
    assert(len == 0 && "Buffer::write called twice (write-once precondition)");
    if (n > 0 && src != nullptr) {
        std::memcpy(data, src, n);
    }
    len = n;
}

Buffer *Buffer::ref_clone()
{
    assert(ref != nullptr && "Buffer::ref_clone on released buffer");
    ref->fetch_add(1, std::memory_order_relaxed);
    return this;
}

void Buffer::release()
{
    if (ref->fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // Last reference — recycle to pool, call external free, or free
        // standalone.
        if (free_cb != nullptr) {
            // External buffer (created via crowdb_rpc_buffer_create_external):
            // the data is owned by an external allocator (e.g. a Rust Vec).
            // free_cb drops the external owner; data itself is NOT freed
            // here (the external owner's destructor handles that).
            free_cb(free_ctx);
            delete ref;
            delete this;
        }
        else if (pool != nullptr) {
            pool->recycle(this);
        }
        else {
            // Standalone buffer (created via crowdb_rpc_buffer_create or
            // frame_to_c_handles): free data, refcount, and Buffer struct.
            // Callers must not access the Buffer after calling release().
            std::free(data);
            delete ref;
            delete this;
        }
    }
}

bool Buffer::transition(BufferOwnership from, BufferOwnership to)
{
    return ownership.compare_exchange_strong(from, to, std::memory_order_acq_rel, std::memory_order_acquire);
}

bool Buffer::quarantine()
{
    BufferOwnership state = ownership.load(std::memory_order_acquire);
    while (state != BufferOwnership::Quarantined) {
        if (ownership.compare_exchange_weak(state, BufferOwnership::Quarantined, std::memory_order_acq_rel,
                                            std::memory_order_acquire))
            return true;
    }
    return false;
}

void Buffer::set_metadata(uint64_t request, uint64_t stream, uint64_t seq, uint64_t offset, uint32_t registration)
{
    request_id      = request;
    stream_id       = stream;
    sequence        = seq;
    logical_offset  = offset;
    registration_id = registration;
}

// ── SystemBufferPool ──────────────────────────────────────────────
//
// Direct heap allocation — no free-list, no mutex. Each alloc does
// posix_memalign + new (Buffer + refcount); each recycle does free +
// delete. glibc per-thread arenas handle small-allocation recycling
// efficiently without a userspace pool.

SystemBufferPool::SystemBufferPool(uint32_t max_buffers) : max_buffers_(max_buffers)
{
}

Buffer *SystemBufferPool::alloc(uint32_t capacity)
{
    if (outstanding_.load(std::memory_order_relaxed) >= max_buffers_) {
        return nullptr; // bound exceeded
    }
    outstanding_.fetch_add(1, std::memory_order_relaxed);

    auto *buf     = new Buffer;
    buf->capacity = capacity;
    buf->type     = BufferType::System;
    buf->pool     = this;
    buf->ref      = new std::atomic<int32_t>(1);

    // posix_memalign for cache-line alignment (64 bytes).
    void *ptr = nullptr;
    if (posix_memalign(&ptr, 64, capacity) != 0) {
        delete buf->ref;
        delete buf;
        outstanding_.fetch_sub(1, std::memory_order_relaxed);
        return nullptr;
    }
    buf->data = static_cast<uint8_t *>(ptr);
    buf->len  = 0;
    return buf;
}

void SystemBufferPool::recycle(Buffer *buf)
{
    std::free(buf->data);
    delete buf->ref;
    delete buf;
    outstanding_.fetch_sub(1, std::memory_order_relaxed);
}

bool SystemBufferPool::detach(Buffer *buf)
{
    if (buf == nullptr || buf->pool != this || buf->ref->load(std::memory_order_acquire) != 1) {
        return false;
    }
    buf->pool = nullptr;
    outstanding_.fetch_sub(1, std::memory_order_relaxed);
    return true;
}

RegisteredBufferPool::RegisteredBufferPool(uint64_t max_bytes, uint32_t max_buffers, uint32_t registration_id)
    : max_bytes_(max_bytes),
      max_buffers_(max_buffers),
      registration_id_(registration_id)
{
    for (auto &head : free_heads_)
        head.store(nullptr, std::memory_order_relaxed);
}

RegisteredBufferPool::~RegisteredBufferPool()
{
    auto destroy = [](Buffer *head) {
        while (head != nullptr) {
            Buffer *next = head->next_pool;
            delete head->ref;
            std::free(head->data);
            delete head;
            head = next;
        }
    };
    for (auto &head : free_heads_)
        destroy(head.exchange(nullptr, std::memory_order_acq_rel));
    destroy(quarantine_head_.exchange(nullptr, std::memory_order_acq_rel));
}

Buffer *RegisteredBufferPool::pop(std::atomic<Buffer *> &head)
{
    Buffer *current = head.load(std::memory_order_acquire);
    while (current != nullptr && !head.compare_exchange_weak(current, current->next_pool, std::memory_order_acq_rel,
                                                             std::memory_order_acquire)) {
    }
    if (current != nullptr)
        current->next_pool = nullptr;
    return current;
}

void RegisteredBufferPool::push(std::atomic<Buffer *> &head, Buffer *buf)
{
    Buffer *current = head.load(std::memory_order_acquire);
    do {
        buf->next_pool = current;
    } while (!head.compare_exchange_weak(current, buf, std::memory_order_acq_rel, std::memory_order_acquire));
}

Buffer *RegisteredBufferPool::alloc(uint32_t requested)
{
    BufferCapacityClass cls;
    uint32_t            capacity;
    if (!classify_rdma_capacity(requested, &cls, &capacity))
        return nullptr;
    Buffer *buf = pop(free_heads_[index(cls)]);
    if (buf == nullptr) {
        uint64_t bytes = committed_bytes_.load(std::memory_order_relaxed);
        while (bytes + capacity <= max_bytes_ &&
               !committed_bytes_.compare_exchange_weak(bytes, bytes + capacity, std::memory_order_acq_rel,
                                                       std::memory_order_relaxed)) {
        }
        if (bytes + capacity > max_bytes_)
            return nullptr;
        uint32_t count = outstanding_.fetch_add(1, std::memory_order_acq_rel);
        if (count >= max_buffers_) {
            outstanding_.fetch_sub(1, std::memory_order_acq_rel);
            committed_bytes_.fetch_sub(capacity, std::memory_order_acq_rel);
            return nullptr;
        }
        void *data = nullptr;
        if (posix_memalign(&data, 64, capacity) != 0) {
            outstanding_.fetch_sub(1, std::memory_order_acq_rel);
            committed_bytes_.fetch_sub(capacity, std::memory_order_acq_rel);
            return nullptr;
        }
        buf           = new Buffer;
        buf->data     = static_cast<uint8_t *>(data);
        buf->capacity = capacity;
        buf->type     = BufferType::Registered;
        buf->pool     = this;
        buf->ref      = new std::atomic<int32_t>(1);
    }
    else {
        outstanding_.fetch_add(1, std::memory_order_acq_rel);
        buf->ref->store(1, std::memory_order_release);
    }
    buf->len             = 0;
    buf->capacity_class  = cls;
    buf->registration_id = registration_id_;
    buf->ownership.store(BufferOwnership::Available, std::memory_order_release);
    return buf;
}

void RegisteredBufferPool::recycle(Buffer *buf)
{
    outstanding_.fetch_sub(1, std::memory_order_acq_rel);
    if (buf->ownership.load(std::memory_order_acquire) == BufferOwnership::Quarantined) {
        quarantined_bytes_.fetch_add(buf->capacity, std::memory_order_acq_rel);
        push(quarantine_head_, buf);
        return;
    }
    buf->len = 0;
    buf->ownership.store(BufferOwnership::Available, std::memory_order_release);
    push(free_heads_[index(buf->capacity_class)], buf);
}

} // namespace crowdb::rpc
