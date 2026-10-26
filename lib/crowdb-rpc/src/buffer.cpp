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
    int32_t count = ref->load(std::memory_order_acquire);
    while (count > 0 &&
           !ref->compare_exchange_weak(count, count + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
    if (count <= 0)
        return nullptr;
    return this;
}

void Buffer::release()
{
    if (ref == nullptr)
        return;
    int32_t count = ref->load(std::memory_order_acquire);
    while (count > 0 &&
           !ref->compare_exchange_weak(count, count - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
    }
    if (count == 0 || count < 0)
        return;
    if (count == 1) {
        if (last_release_cb != nullptr) {
            auto  callback   = last_release_cb;
            void *context    = last_release_ctx;
            last_release_cb  = nullptr;
            last_release_ctx = nullptr;
            // No other owner remains. Give the recycler one reference so it
            // can repost or release using the ordinary ownership contract.
            ref->store(1, std::memory_order_release);
            callback(context, this);
            return;
        }
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

Buffer *Buffer::slice(uint32_t offset, uint32_t size)
{
    if (offset > capacity || size > capacity - offset || data == nullptr || ref_clone() == nullptr)
        return nullptr;
    auto *view     = new Buffer;
    view->data     = payload_data() + offset;
    view->len      = size;
    view->capacity = size;
    view->type     = type;
    view->ref      = new std::atomic<int32_t>(1);
    view->ownership.store(BufferOwnership::CallerOwned, std::memory_order_relaxed);
    view->capacity_class      = capacity_class;
    view->registration_id     = registration_id;
    view->registration_key    = registration_key;
    view->registration_handle = registration_handle;
    view->allocation_owner    = allocation_owner == nullptr ? this : allocation_owner;
    view->set_metadata(request_id, stream_id, sequence, logical_offset, registration_id);
    view->free_cb  = [](void *parent) { static_cast<Buffer *>(parent)->release(); };
    view->free_ctx = this;
    return view;
}

bool Buffer::transition(BufferOwnership from, BufferOwnership to)
{
    return ownership.compare_exchange_strong(from, to, std::memory_order_acq_rel, std::memory_order_acquire);
}

bool Buffer::quarantine()
{
    if (allocation_owner != nullptr)
        (void)allocation_owner->quarantine();
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

} // namespace crowdb::rpc
