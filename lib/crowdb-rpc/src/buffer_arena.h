// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"

namespace crowdb::rpc
{
struct BufferRegion
{
    uint8_t              *data                = nullptr;
    uint64_t              bytes               = 0;
    uintptr_t             registration_handle = 0;
    uint32_t              registration_key    = 0;
    uint32_t              registration_id     = 0;
    std::shared_ptr<void> owner;

    static std::shared_ptr<BufferRegion> allocate(uint64_t bytes, uint32_t registration_id = 0,
                                                  uint32_t alignment = 64);
};

class BufferArena : public BufferPool, public std::enable_shared_from_this<BufferArena>
{
  public:
    struct Entry
    {
        std::atomic<Buffer *> buffer{nullptr};
        std::atomic<uint32_t> next{0};
    };

    const std::shared_ptr<BufferRegion> region;
    const uint64_t                      bytes;
    const uint32_t                      capacity;
    std::atomic<uint32_t>               outstanding{0};
    std::atomic<uint64_t>               next_offset{0};
    std::atomic<uint64_t>               quarantined{0};

    BufferArena(std::shared_ptr<BufferRegion> region, uint32_t count, uint32_t large_alignment = 64);
    ~BufferArena() override;
    Buffer *alloc(uint32_t requested) override;
    Buffer *borrow_range(const uint8_t *data, uint32_t size);
    void    recycle(Buffer *buffer) override;
    bool    detach(Buffer *buffer) override;
    void    quarantine_inflight();

  private:
    const uint32_t                       large_alignment_;
    std::unique_ptr<Entry[]>             entries_;
    std::array<std::atomic<uint64_t>, 4> free_heads_{};
    std::atomic<uint64_t>                unused_entries_{0};
    std::atomic<uint32_t>                next_entry_{0};

    Buffer  *fresh(uint32_t size);
    Buffer  *pop(size_t bucket);
    void     push(Buffer *buffer);
    uint32_t pop_entry(std::atomic<uint64_t> &head);
    void     push_entry(std::atomic<uint64_t> &head, uint32_t index);

    struct DetachedRelease
    {
        BufferArena *arena;
        uint32_t     index;
        Buffer      *buffer;
    };

    static void release_detached(void *context);
};
} // namespace crowdb::rpc
