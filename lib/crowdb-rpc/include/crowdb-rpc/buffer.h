// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>

namespace crowdb::rpc
{

// Buffer type determines the allocation strategy and memory registration.
// System: aligned system malloc (posix_memalign), recycled to a
// SystemBufferPool. Registered: ibv_reg_mr'd for RDMA, recycled to an
// RdmaBufferPool (Linux + CROWDB_RPC_HAVE_RDMA only).
enum class BufferType : uint8_t {
    System,
    Registered,
};

enum class BufferOwnership : uint8_t {
    Available,
    PostedReceive,
    ReceiveInFlight,
    SendInFlight,
    DiskOwned,
    CallerOwned,
    Quarantined,
};

enum class BufferCapacityClass : uint8_t { Control, Metadata, Aggregate, Datapath };

constexpr uint32_t RDMA_CONTROL_CAPACITY   = 1U << 10;
constexpr uint32_t RDMA_METADATA_CAPACITY  = 64U << 10;
constexpr uint32_t RDMA_AGGREGATE_CAPACITY = 256U << 10;
constexpr uint32_t RDMA_DATAPATH_CAPACITY  = 1U << 20;

[[nodiscard]] bool classify_rdma_capacity(uint32_t requested, BufferCapacityClass *out, uint32_t *capacity);

// A ref-counted, pool-owned byte buffer. The buffer is allocated from a
// BufferPool, written once, then shared across consumers via ref(). Each
// consumer calls release() when done; the buffer recycles to the pool when
// the last reference drops. The refcount is a separate pool-allocated
// atomic (not embedded in this struct) so multiple Buffer* handles to the
// same allocation share one refcount slot.
//
// Lifecycle: alloc → write once → ref-count down across consumers →
// recycle to pool when ref == 0.
struct Buffer
{
    uint8_t          *data     = nullptr;
    uint32_t          len      = 0; // bytes written (payload length)
    uint32_t          capacity = 0; // allocated capacity
    BufferType        type     = BufferType::System;
    class BufferPool *pool     = nullptr;
    // Registered allocations retain their arena independently of its facade.
    std::shared_ptr<void>        pool_owner;
    uint32_t                     pool_index = 0;
    std::atomic<int32_t>        *ref        = nullptr; // shared refcount (pool-allocated or standalone)
    std::atomic<BufferOwnership> ownership{BufferOwnership::Available};
    BufferCapacityClass          capacity_class      = BufferCapacityClass::Control;
    uint64_t                     request_id          = 0;
    uint64_t                     stream_id           = 0;
    uint64_t                     sequence            = 0;
    uint64_t                     logical_offset      = 0;
    uint32_t                     registration_id     = 0;
    uint32_t                     registration_key    = 0;
    uintptr_t                    registration_handle = 0;
    uint8_t                     *allocation_data     = nullptr;
    uint32_t                     allocation_capacity = 0;
    uint32_t                     view_offset         = 0;
    Buffer                      *next_pool           = nullptr;
    Buffer                      *next_all            = nullptr;

    void reset_view()
    {
        if (allocation_data != nullptr) {
            data        = allocation_data;
            capacity    = allocation_capacity;
            view_offset = 0;
        }
    }

    [[nodiscard]] uint8_t *payload_data()
    {
        return data == nullptr ? nullptr : data + view_offset;
    }

    [[nodiscard]] const uint8_t *payload_data() const
    {
        return data == nullptr ? nullptr : data + view_offset;
    }

    // External free callback: when set, release() calls free_cb(free_ctx)
    // instead of freeing `data` directly. Used for zero-copy buffers that
    // wrap externally-owned memory (e.g. a Rust Vec allocation).
    void (*free_cb)(void *) = nullptr;
    void *free_ctx          = nullptr;

    // Transfer the final reference to an asynchronous receive recycler.
    // The callback consumes that reference, including any repost attempt.
    void (*last_release_cb)(void *, Buffer *) = nullptr;
    void *last_release_ctx                    = nullptr;

    // Copy src into data, set len. Called once per buffer.
    void write(const void *src, uint32_t n);

    // Increment refcount, return a new Buffer* pointing at the same
    // allocation. Each consumer that needs to hold the buffer calls ref().
    Buffer *ref_clone();

    // Decrement refcount. On ref == 0, recycle to pool or call free_cb.
    void release();

    [[nodiscard]] bool transition(BufferOwnership from, BufferOwnership to);
    [[nodiscard]] bool quarantine();
    void set_metadata(uint64_t request, uint64_t stream, uint64_t seq, uint64_t offset, uint32_t registration);
};

// BufferPool allocates and recycles Buffer objects. The pool reuses
// recycled allocations (same capacity bucket) to amortize malloc cost.
// Capacity is bounded; alloc when exhausted returns nullptr.
class BufferPool
{
  public:
    virtual ~BufferPool() = default;

    // Returns a Buffer* with ref == 1, len == 0. nullptr if the pool is
    // exhausted (capacity bound reached).
    virtual Buffer *alloc(uint32_t capacity) = 0;

    // Called by Buffer::release when ref == 0. Returns the buffer to the
    // free list for reuse.
    virtual void recycle(Buffer *buf) = 0;

    // Transfer an immutable receive buffer out of the pool without copying.
    // The caller must hold the only live reference before detaching.
    virtual bool detach(Buffer *buf)
    {
        (void)buf;
        return false;
    }
};

// SystemBufferPool: direct heap allocation (posix_memalign + new/delete),
// no free-list recycling. Uses glibc per-thread
// arenas handle small-allocation recycling efficiently without a userspace
// pool. The max_buffers bound limits outstanding allocations; alloc returns
// nullptr when exceeded. Default pool for TCP transport.
class SystemBufferPool : public BufferPool
{
  public:
    // max_buffers: outstanding allocation bound (safety valve).
    SystemBufferPool(uint32_t max_buffers = 8192);
    ~SystemBufferPool() override = default;

    Buffer *alloc(uint32_t capacity) override;
    void    recycle(Buffer *buf) override;
    bool    detach(Buffer *buf) override;

  private:
    uint32_t              max_buffers_;
    std::atomic<uint32_t> outstanding_{0}; // in-use buffers (allocated, not yet freed)
};

// Bounded registered-memory abstraction. The pool remains usable without an
// RDMA device; a transport may attach registration metadata later.
class RegisteredBufferPool : public BufferPool
{
  public:
    RegisteredBufferPool(uint64_t max_bytes, uint32_t max_buffers = 1024, uint32_t registration_id = 0);
    ~RegisteredBufferPool() override;

    Buffer *alloc(uint32_t capacity) override;
    void    recycle(Buffer *buf) override;
    bool    detach(Buffer *buf) override;

    [[nodiscard]] uint64_t committed_bytes() const
    {
        return committed_bytes_.load();
    }

    [[nodiscard]] uint64_t quarantined_bytes() const
    {
        return quarantined_bytes_.load();
    }

    [[nodiscard]] uint32_t registration_id() const
    {
        return registration_id_;
    }

  private:
    static constexpr size_t                        CLASS_COUNT = 4;
    uint64_t                                       max_bytes_;
    uint32_t                                       max_buffers_;
    uint32_t                                       registration_id_;
    std::atomic<uint64_t>                          committed_bytes_{0};
    std::atomic<uint32_t>                          outstanding_{0};
    std::array<std::atomic<Buffer *>, CLASS_COUNT> free_heads_{};
    std::atomic<Buffer *>                          quarantine_head_{nullptr};
    std::atomic<uint64_t>                          quarantined_bytes_{0};

    static size_t index(BufferCapacityClass cls)
    {
        return static_cast<size_t>(cls);
    }

    Buffer *pop(std::atomic<Buffer *> &head);
    void    push(std::atomic<Buffer *> &head, Buffer *buf);
};

} // namespace crowdb::rpc
