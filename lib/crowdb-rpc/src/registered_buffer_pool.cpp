// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "buffer_arena.h"

namespace crowdb::rpc
{
RegisteredBufferPool::RegisteredBufferPool(uint64_t max_bytes, uint32_t max_buffers, uint32_t registration_id)
    : arena_(std::make_shared<BufferArena>(BufferRegion::allocate(max_bytes, registration_id), max_buffers)),
      registration_id_(registration_id)
{
}

RegisteredBufferPool::~RegisteredBufferPool() = default;

Buffer *RegisteredBufferPool::alloc(uint32_t capacity)
{
    return arena_->alloc(capacity);
}

void RegisteredBufferPool::recycle(Buffer *buffer)
{
    arena_->recycle(buffer);
}

bool RegisteredBufferPool::detach(Buffer *buffer)
{
    return arena_->detach(buffer);
}

uint64_t RegisteredBufferPool::committed_bytes() const
{
    return arena_->next_offset.load(std::memory_order_acquire);
}

uint64_t RegisteredBufferPool::quarantined_bytes() const
{
    return arena_->quarantined.load(std::memory_order_acquire);
}
} // namespace crowdb::rpc
