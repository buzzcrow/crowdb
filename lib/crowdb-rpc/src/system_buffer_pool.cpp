// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/buffer.h"

#include <cstdlib>

namespace crowdb::rpc
{
// Outstanding buffers retain allocation admission and the recycler itself.
// The facade may disappear while an asynchronous caller still owns a lease.
struct SystemBufferPool::State final : BufferPool
{
    explicit State(uint32_t limit) : maximum(limit)
    {
    }

    static constexpr uint64_t FACADE_REFERENCE = uint64_t{1} << 63;
    const uint32_t            maximum;
    // Low bits are both bounded admission and allocation lifetime references.
    std::atomic<uint64_t> references{FACADE_REFERENCE};
    static_assert(std::atomic<uint64_t>::is_always_lock_free);

    void release_allocation()
    {
        if (references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

    void release_facade()
    {
        if (references.fetch_and(~FACADE_REFERENCE, std::memory_order_acq_rel) == FACADE_REFERENCE) {
            delete this;
        }
    }

    Buffer *alloc(uint32_t capacity) override
    {
        uint64_t count = references.load(std::memory_order_relaxed);
        do {
            if ((count & FACADE_REFERENCE) == 0 || (count & ~FACADE_REFERENCE) >= maximum) {
                return nullptr;
            }
        } while (!references.compare_exchange_weak(count, count + 1, std::memory_order_relaxed));
        try {
            auto  buffer = std::make_unique<Buffer>();
            auto  ref    = std::make_unique<std::atomic<int32_t>>(1);
            void *memory = nullptr;
            if (posix_memalign(&memory, 64, capacity) != 0) {
                release_allocation();
                return nullptr;
            }
            buffer->capacity = capacity;
            buffer->type     = BufferType::System;
            buffer->pool     = this;
            buffer->ref      = ref.release();
            buffer->data     = static_cast<uint8_t *>(memory);
            return buffer.release();
        }
        catch (...) {
            release_allocation();
            throw;
        }
    }

    void recycle(Buffer *buffer) override
    {
        std::free(buffer->data);
        delete buffer->ref;
        delete buffer;
        release_allocation();
    }

    bool detach(Buffer *buffer) override
    {
        const auto state =
            buffer == nullptr ? BufferOwnership::Quarantined : buffer->ownership.load(std::memory_order_acquire);
        if (buffer == nullptr || buffer->pool != this || buffer->ref->load(std::memory_order_acquire) != 1 ||
            (state != BufferOwnership::Available && state != BufferOwnership::CallerOwned)) {
            return false;
        }
        buffer->pool = nullptr;
        release_allocation();
        return true;
    }
};

SystemBufferPool::SystemBufferPool(uint32_t max_buffers) : state_(new State(max_buffers))
{
}

SystemBufferPool::~SystemBufferPool()
{
    state_->release_facade();
}

Buffer *SystemBufferPool::alloc(uint32_t capacity)
{
    return state_->alloc(capacity);
}

void SystemBufferPool::recycle(Buffer *buffer)
{
    state_->recycle(buffer);
}

bool SystemBufferPool::detach(Buffer *buffer)
{
    return state_->detach(buffer);
}

uint32_t SystemBufferPool::max_buffers() const
{
    return state_->maximum;
}

uint32_t SystemBufferPool::outstanding() const
{
    return static_cast<uint32_t>(state_->references.load(std::memory_order_relaxed) & ~State::FACADE_REFERENCE);
}
} // namespace crowdb::rpc
