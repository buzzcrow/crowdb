// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <cstdlib>

namespace crowdb::rpc
{

// ── RdmaBufferPool ────────────────────────────────────────────────

RdmaBufferPool::RdmaBufferPool(struct ibv_pd *pd, uint64_t mr_size, uint32_t max_buffers)
    : pd_(pd),
      mr_size_(mr_size),
      max_buffers_(max_buffers)
{
    for (auto &head : free_heads_)
        head.store(nullptr, std::memory_order_relaxed);
    if (pd_ == nullptr || mr_size_ == 0 || max_buffers_ == 0) {
        return;
    }
    if (posix_memalign(&region_, 64, mr_size_) != 0) {
        region_ = nullptr;
        return;
    }
    mr_ = ibv_reg_mr(pd_, region_, mr_size_,
                     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_ATOMIC);
    if (mr_ == nullptr) {
        std::free(region_);
        region_ = nullptr;
        return;
    }
}

RdmaBufferPool::~RdmaBufferPool()
{
    if (outstanding_.load(std::memory_order_acquire) != 0) {
        // Keep the MR registered while a caller, disk operation, or device
        // completion still owns a slice. Process teardown reclaims it.
        return;
    }
    Buffer *buf = all_head_.exchange(nullptr, std::memory_order_acq_rel);
    while (buf != nullptr) {
        Buffer *next = buf->next_all;
        delete buf->ref;
        delete buf;
        buf = next;
    }
    if (mr_ != nullptr) {
        ibv_dereg_mr(mr_);
        mr_ = nullptr;
    }
    if (region_ != nullptr) {
        std::free(region_);
        region_ = nullptr;
    }
}

Buffer *RdmaBufferPool::alloc_fresh(uint32_t capacity)
{
    if (mr_ == nullptr || region_ == nullptr) {
        return nullptr;
    }
    uint64_t offset = next_offset_.load(std::memory_order_relaxed);
    uint64_t aligned_offset;
    while (true) {
        if (offset > mr_size_ || offset > UINT64_MAX - 63U)
            return nullptr;
        aligned_offset = (offset + 63U) & ~uint64_t{63};
        if (aligned_offset > mr_size_ || capacity > mr_size_ - aligned_offset)
            return nullptr;
        if (next_offset_.compare_exchange_weak(offset, aligned_offset + capacity, std::memory_order_acq_rel,
                                               std::memory_order_relaxed))
            break;
    }

    auto *buf     = new Buffer;
    buf->capacity = capacity;
    buf->type     = BufferType::Registered;
    buf->pool     = this;
    buf->ref      = new std::atomic<int32_t>(1);

    buf->data                = static_cast<uint8_t *>(region_) + aligned_offset;
    buf->len                 = 0;
    buf->allocation_data     = buf->data;
    buf->allocation_capacity = capacity;
    buf->registration_handle = reinterpret_cast<uintptr_t>(mr_);
    buf->registration_key    = mr_->lkey;
    Buffer *all              = all_head_.load(std::memory_order_acquire);
    do {
        buf->next_all = all;
    } while (!all_head_.compare_exchange_weak(all, buf, std::memory_order_acq_rel, std::memory_order_acquire));
    return buf;
}

Buffer *RdmaBufferPool::pop(std::atomic<Buffer *> &head)
{
    Buffer *current = head.load(std::memory_order_acquire);
    while (current != nullptr && !head.compare_exchange_weak(current, current->next_pool, std::memory_order_acq_rel,
                                                             std::memory_order_acquire)) {
    }
    if (current != nullptr)
        current->next_pool = nullptr;
    return current;
}

void RdmaBufferPool::push(std::atomic<Buffer *> &head, Buffer *buf)
{
    Buffer *current = head.load(std::memory_order_acquire);
    do {
        buf->next_pool = current;
    } while (!head.compare_exchange_weak(current, buf, std::memory_order_acq_rel, std::memory_order_acquire));
}

bool RdmaBufferPool::reserve_outstanding()
{
    uint32_t count = outstanding_.load(std::memory_order_relaxed);
    while (count < max_buffers_ && !outstanding_.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel,
                                                                       std::memory_order_relaxed)) {
    }
    return count < max_buffers_;
}

void RdmaBufferPool::quarantine_inflight()
{
    for (Buffer *buf = all_head_.load(std::memory_order_acquire); buf != nullptr; buf = buf->next_all) {
        const BufferOwnership state = buf->ownership.load(std::memory_order_acquire);
        if (state != BufferOwnership::Available && state != BufferOwnership::Quarantined)
            (void)buf->quarantine();
    }
}

Buffer *RdmaBufferPool::alloc(uint32_t capacity)
{
    BufferCapacityClass cls;
    uint32_t            bucket;
    if (!classify_rdma_capacity(capacity, &cls, &bucket)) {
        return nullptr;
    }
    Buffer *buf = pop(free_heads_[static_cast<size_t>(cls)]);
    if (buf != nullptr) {
        buf->reset_view();
        buf->len = 0;
        buf->ref->store(1, std::memory_order_relaxed);
        buf->capacity_class = cls;
        buf->ownership.store(BufferOwnership::Available, std::memory_order_release);
        if (!reserve_outstanding()) {
            push(free_heads_[static_cast<size_t>(cls)], buf);
            return nullptr;
        }
        return buf;
    }
    if (!reserve_outstanding())
        return nullptr;
    buf = alloc_fresh(bucket);
    if (buf != nullptr) {
        buf->capacity_class  = cls;
        buf->registration_id = mr_ == nullptr ? 0 : mr_->lkey;
    }
    else
        outstanding_.fetch_sub(1, std::memory_order_acq_rel);
    return buf;
}

void RdmaBufferPool::recycle(Buffer *buf)
{
    outstanding_.fetch_sub(1, std::memory_order_acq_rel);
    buf->reset_view();
    const BufferOwnership state = buf->ownership.load(std::memory_order_acquire);
    if (state != BufferOwnership::Available && state != BufferOwnership::Quarantined)
        (void)buf->quarantine();
    if (buf->ownership.load(std::memory_order_acquire) == BufferOwnership::Quarantined) {
        quarantined_bytes_.fetch_add(buf->capacity, std::memory_order_acq_rel);
        push(quarantine_head_, buf);
        return;
    }
    buf->len = 0;
    buf->ownership.store(BufferOwnership::Available, std::memory_order_release);
    push(free_heads_[static_cast<size_t>(buf->capacity_class)], buf);
}

} // namespace crowdb::rpc

#endif
